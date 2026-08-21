package downloading

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/unkmonster/tmd/internal/database"
	"github.com/unkmonster/tmd/internal/twitter"
)

type TweetDumper struct {
	data  map[int][]*twitter.Tweet
	set   map[int]map[uint64]struct{}
	count int
}

func NewDumper() *TweetDumper {
	td := TweetDumper{}
	td.data = make(map[int][]*twitter.Tweet)
	td.set = make(map[int]map[uint64]struct{})
	return &td
}

func (td *TweetDumper) Push(eid int, tweet ...*twitter.Tweet) int {
	_, ok := td.data[eid]
	if !ok {
		td.data[eid] = make([]*twitter.Tweet, 0, len(tweet))
		td.set[eid] = make(map[uint64]struct{})
	}

	oldCount := td.count

	for _, tw := range tweet {
		if tw == nil {
			log.Warnf("Ignoring a nil tweet in error queue entity %d", eid)
			continue
		}
		_, exist := td.set[eid][tw.Id]
		if exist {
			continue
		}
		td.data[eid] = append(td.data[eid], tw)
		td.set[eid][tw.Id] = struct{}{}
		td.count++
	}
	return td.count - oldCount
}

func (td *TweetDumper) Load(path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	loaded := make(map[int][]*twitter.Tweet)
	err = json.Unmarshal(data, &loaded)
	if err != nil {
		return err
	}

	for k, v := range loaded {
		td.Push(k, v...)
	}
	return nil
}

func (td *TweetDumper) Dump(path string) error {
	data, err := json.MarshalIndent(td.data, "", "    ")
	if err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			os.Remove(tempPath)
		}
	}()

	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tempPath, 0666); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		// Some Windows callers create and keep an empty destination open. There
		// is no old queue to preserve in that specific case, so a direct write is
		// safe. Never truncate a non-empty queue after an atomic replace failure.
		if info, statErr := os.Stat(path); statErr == nil && info.Size() == 0 {
			log.Warnf("Atomic retry queue replacement could not replace the open empty file %s; writing it directly: %v", path, err)
			if writeErr := os.WriteFile(path, data, 0666); writeErr == nil {
				return nil
			}
		}
		removeTemp = false
		return fmt.Errorf("failed to replace retry queue %s; the complete new queue is preserved at %s: %w", path, tempPath, err)
	}
	return nil
}

func (td *TweetDumper) Clear() {
	td.data = make(map[int][]*twitter.Tweet)
	td.set = make(map[int]map[uint64]struct{})
	td.count = 0
}

func (td *TweetDumper) Remove(eid int, tweets ...*twitter.Tweet) int {
	existing, ok := td.data[eid]
	if !ok || len(tweets) == 0 {
		return 0
	}

	toRemove := make(map[uint64]struct{}, len(tweets))
	for _, tw := range tweets {
		if tw != nil {
			toRemove[tw.Id] = struct{}{}
		}
	}

	removed := 0
	remaining := existing[:0]
	for _, tw := range existing {
		if tw != nil {
			if _, exists := toRemove[tw.Id]; exists {
				delete(td.set[eid], tw.Id)
				td.count--
				removed++
				continue
			}
		}
		remaining = append(remaining, tw)
	}

	if len(remaining) == 0 {
		delete(td.data, eid)
		delete(td.set, eid)
	} else {
		td.data[eid] = remaining
	}
	return removed
}

func (td *TweetDumper) rebind(from, to int) int {
	if from == to {
		return 0
	}
	tweets, ok := td.data[from]
	if !ok {
		return 0
	}

	sourceCount := len(tweets)
	td.Push(to, tweets...)
	delete(td.data, from)
	delete(td.set, from)
	td.count -= sourceCount
	return sourceCount
}

func (td *TweetDumper) GetTotal(db *sqlx.DB) ([]*TweetInEntity, error) {
	results := make([]*TweetInEntity, 0, td.count)
	seen := make(map[int]map[uint64]struct{})
	type entityRemap struct {
		from int
		to   int
	}
	var remaps []entityRemap

	for k, v := range td.data {
		e, err := database.GetUserEntity(db, k)
		if err != nil {
			return nil, err
		}
		if e == nil {
			var creatorID uint64
			canRemap := true
			for _, tw := range v {
				if tw == nil || tw.Creator == nil || tw.Creator.Id == 0 {
					canRemap = false
					break
				}
				if creatorID == 0 {
					creatorID = tw.Creator.Id
				} else if creatorID != tw.Creator.Id {
					canRemap = false
					break
				}
			}
			if !canRemap || creatorID == 0 {
				log.Warnf("Skipping %d queued tweets for missing entity %d: author cannot be identified uniquely; records remain in errors.json", len(v), k)
				continue
			}

			var candidates []*database.UserEntity
			if err := db.Select(&candidates, "SELECT * FROM user_entities WHERE user_id = ?", creatorID); err != nil {
				return nil, err
			}
			if len(candidates) != 1 || !candidates[0].Id.Valid {
				log.Warnf("Skipping %d queued tweets for missing entity %d: author %d has %d current entities; records remain in errors.json", len(v), k, creatorID, len(candidates))
				continue
			}

			e = candidates[0]
			newID := int(e.Id.Int32)
			remaps = append(remaps, entityRemap{from: k, to: newID})
			log.Warnf("Remapping %d queued tweets from missing entity %d to current entity %d for author %d", len(v), k, newID, creatorID)
		}
		ue := UserEntity{db: db, record: e, created: true}
		entityID := ue.Id()
		if _, ok := seen[entityID]; !ok {
			seen[entityID] = make(map[uint64]struct{})
		}

		for _, tw := range v {
			if tw == nil {
				log.Warnf("Skipping a nil queued tweet for entity %d; record remains in errors.json", k)
				continue
			}
			if _, duplicate := seen[entityID][tw.Id]; duplicate {
				continue
			}
			seen[entityID][tw.Id] = struct{}{}
			results = append(results, &TweetInEntity{Tweet: tw, Entity: &ue})
		}
	}
	for _, remap := range remaps {
		td.rebind(remap.from, remap.to)
	}
	return results, nil
}

func (td *TweetDumper) Count() int {
	return td.count
}
