package downloading

import (
	"encoding/json"
	"io"
	"os"

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
	return os.WriteFile(path, data, 0666)
}

func (td *TweetDumper) Clear() {
	td.data = make(map[int][]*twitter.Tweet)
	td.set = make(map[int]map[uint64]struct{})
	td.count = 0
}

// Remove deletes only the specified tweets from one entity's retry queue.
func (td *TweetDumper) Remove(eid int, tweets ...*twitter.Tweet) int {
	existing, ok := td.data[eid]
	if !ok || len(tweets) == 0 {
		return 0
	}

	toRemove := make(map[uint64]struct{}, len(tweets))
	for _, tw := range tweets {
		toRemove[tw.Id] = struct{}{}
	}

	removed := 0
	remaining := existing[:0]
	for _, tw := range existing {
		if _, ok := toRemove[tw.Id]; ok {
			delete(td.set[eid], tw.Id)
			td.count--
			removed++
			continue
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

func (td *TweetDumper) GetTotal(db *sqlx.DB) ([]*TweetInEntity, error) {
	results := make([]*TweetInEntity, 0, td.count)

	for k, v := range td.data {
		e, err := database.GetUserEntity(db, k)
		if err != nil {
			return nil, err
		}
		if e == nil {
			log.WithFields(log.Fields{
				"entity_id": k,
				"tweets":    len(v),
			}).Warnln("skipping failed tweet retries because the user entity no longer exists")
			continue
		}
		ue := UserEntity{db: db, record: e, created: true}

		for _, tw := range v {
			results = append(results, &TweetInEntity{Tweet: tw, Entity: &ue})
		}
	}
	return results, nil
}

func (td *TweetDumper) Count() int {
	return td.count
}
