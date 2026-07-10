package downloading

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/go-resty/resty/v2"
	"github.com/gookit/color"
	"github.com/jmoiron/sqlx"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"github.com/unkmonster/tmd/internal/database"
	"github.com/unkmonster/tmd/internal/twitter"
	"github.com/unkmonster/tmd/internal/utils"
)

type PackgedTweet interface {
	GetTweet() *twitter.Tweet
	GetPath() string
}

type TweetInDir struct {
	tweet *twitter.Tweet
	path  string
}

func (pt TweetInDir) GetTweet() *twitter.Tweet {
	return pt.tweet
}

func (pt TweetInDir) GetPath() string {
	return pt.path
}

// 任何一个 url 下载失败直接返回
// TODO: 要么全做，要么不做
func downloadTweetMedia(ctx context.Context, client *resty.Client, db *sqlx.DB, dir string, tweet *twitter.Tweet) error {
	for i, u := range tweet.Urls {
		ext, err := utils.GetExtFromUrl(u)
		if err != nil {
			return err
		}

		// Generate deterministic filenames
		truncatedName, originalName := GenerateFileName(tweet.CreatedAt, tweet.Id, i+1, tweet.Text, ext)
		path := filepath.Join(dir, truncatedName)

		// 1. Check DB first if db is available
		if db != nil {
			mf, err := database.GetMediaFile(db, tweet.Id, u)
			if err == nil && mf != nil && mf.DownloadStatus == 1 {
				// Verify if file actually exists on disk
				if exists, _ := utils.PathExists(path); exists {
					continue // Already downloaded, skip completely
				}
			}
		}

		// 2. Double check disk with new format (in case DB record is missing)
		if exists, _ := utils.PathExists(path); exists {
			// Write DB record for consistency
			if db != nil {
				dbTweet := &database.TweetRecord{
					Id:        tweet.Id,
					UserId:    tweet.Creator.Id,
					Text:      tweet.Text,
					CreatedAt: tweet.CreatedAt,
				}
				database.SaveTweet(db, dbTweet)

				dbMedia := &database.MediaFileRecord{
					TweetId:          tweet.Id,
					Url:              u,
					Filename:         truncatedName,
					OriginalFilename: originalName,
					DownloadStatus:   1,
					DownloadedAt:     sql.NullTime{Time: time.Now(), Valid: true},
				}
				if fi, err := os.Stat(path); err == nil {
					dbMedia.FileSize = sql.NullInt64{Int64: fi.Size(), Valid: true}
				}
				database.SaveMediaFile(db, dbMedia)
			}
			continue
		}

		// 3. 无缝兼容老版本文件名平滑迁移
		var oldName string
		cleanText := utils.WinFileName(tweet.Text)
		if i == 0 {
			oldName = cleanText + ext
		} else {
			oldName = fmt.Sprintf("%s(%d)%s", cleanText, i, ext)
		}
		oldPath := filepath.Join(dir, oldName)

		if exists, _ := utils.PathExists(oldPath); exists {
			// Rename locally
			err := os.Rename(oldPath, path)
			if err == nil {
				log.Infof("Migrated file %s to %s on-the-fly", oldName, truncatedName)
				if db != nil {
					dbTweet := &database.TweetRecord{
						Id:        tweet.Id,
						UserId:    tweet.Creator.Id,
						Text:      tweet.Text,
						CreatedAt: tweet.CreatedAt,
					}
					database.SaveTweet(db, dbTweet)

					dbMedia := &database.MediaFileRecord{
						TweetId:          tweet.Id,
						Url:              u,
						Filename:         truncatedName,
						OriginalFilename: originalName,
						DownloadStatus:   1,
						DownloadedAt:     sql.NullTime{Time: time.Now(), Valid: true},
					}
					if fi, err := os.Stat(path); err == nil {
						dbMedia.FileSize = sql.NullInt64{Int64: fi.Size(), Valid: true}
					}
					database.SaveMediaFile(db, dbMedia)
				}
				continue
			}
		}

		// 4. Perform HTTP request to download
		resp, err := client.R().SetContext(ctx).SetQueryParam("name", "4096x4096").Get(u)
		if err != nil {
			if db != nil {
				dbMedia := &database.MediaFileRecord{
					TweetId:          tweet.Id,
					Url:              u,
					Filename:         truncatedName,
					OriginalFilename: originalName,
					DownloadStatus:   2, // failed
				}
				database.SaveMediaFile(db, dbMedia)
			}
			return err
		}

		file, err := os.Create(path)
		if err != nil {
			return err
		}

		_, err = file.Write(resp.Body())
		file.Close() // Close immediately to set modification time
		if err != nil {
			return err
		}

		os.Chtimes(path, time.Time{}, tweet.CreatedAt)

		// Record success to DB
		if db != nil {
			dbTweet := &database.TweetRecord{
				Id:        tweet.Id,
				UserId:    tweet.Creator.Id,
				Text:      tweet.Text,
				CreatedAt: tweet.CreatedAt,
			}
			database.SaveTweet(db, dbTweet)

			dbMedia := &database.MediaFileRecord{
				TweetId:          tweet.Id,
				Url:              u,
				Filename:         truncatedName,
				OriginalFilename: originalName,
				DownloadStatus:   1,
				DownloadedAt:     sql.NullTime{Time: time.Now(), Valid: true},
				FileSize:         sql.NullInt64{Int64: int64(len(resp.Body())), Valid: true},
			}
			database.SaveMediaFile(db, dbMedia)
		}
	}

	fmt.Printf("%s %s\n", color.FgLightMagenta.Render("["+tweet.Creator.Title()+"]"), utils.WinFileName(tweet.Text))
	return nil
}

var MaxDownloadRoutine int

// map[user_id]*UserEntity 记录本次程序运行已同步过的用户
var syncedUsers sync.Map

func init() {
	MaxDownloadRoutine = min(100, runtime.GOMAXPROCS(0)*10)
}

type workerConfig struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	cancel context.CancelCauseFunc
	db     *sqlx.DB
}

// 负责下载推文，保证 tweet chan 内的推文要么下载成功，要么推送至 error chan
func tweetDownloader(client *resty.Client, config *workerConfig, errch chan<- PackgedTweet, twech <-chan PackgedTweet) {
	var pt PackgedTweet
	var ok bool

	defer config.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			config.cancel(fmt.Errorf("%v", p)) // panic 取消上下文，防止生产者死锁
			log.WithField("worker", "downloading").Errorln("panic:", p)

			if pt != nil {
				errch <- pt // push 正下载的推文
			}
			// 确保只有1个协程的情况下，未能下载完毕的推文仍然会全部推送到 errch
			for pt := range twech {
				errch <- pt
			}
		}
	}()

	for {
		select {
		case pt, ok = <-twech:
			if !ok {
				return
			}
		case <-config.ctx.Done():
			for pt := range twech {
				errch <- pt
			}
			return
		}

		path := pt.GetPath()
		if path == "" {
			errch <- pt
			continue
		}
		err := downloadTweetMedia(config.ctx, client, config.db, path, pt.GetTweet())
		// 403: Dmcaed
		if err != nil && !utils.IsStatusCode(err, 404) && !utils.IsStatusCode(err, 403) {
			errch <- pt
		}

		// cancel context and exit if no disk space
		if errors.Is(err, syscall.ENOSPC) {
			config.cancel(err)
		}
	}
}

// 批量下载推文并返回下载失败 of 推文，可以保证推文被成功下载或被返回
func BatchDownloadTweet(ctx context.Context, client *resty.Client, db *sqlx.DB, pts ...PackgedTweet) []PackgedTweet {
	if len(pts) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancelCause(ctx)

	var errChan = make(chan PackgedTweet)
	var tweetChan = make(chan PackgedTweet, len(pts))
	var wg sync.WaitGroup // number of working goroutines
	var numRoutine = min(len(pts), MaxDownloadRoutine)

	for _, pt := range pts {
		tweetChan <- pt
	}
	close(tweetChan)

	config := workerConfig{
		ctx:    ctx,
		cancel: cancel,
		wg:     &wg,
		db:     db,
	}
	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go tweetDownloader(client, &config, errChan, tweetChan)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	errors := []PackgedTweet{}
	for pt := range errChan {
		errors = append(errors, pt)
	}
	return errors
}

// 更新数据库中对用户的记录
func syncUser(db *sqlx.DB, user *twitter.User) error {
	renamed := false
	isNew := false
	usrdb, err := database.GetUserById(db, user.Id)
	if err != nil {
		return err
	}

	if usrdb == nil {
		isNew = true
		usrdb = &database.User{}
		usrdb.Id = user.Id
	} else {
		renamed = usrdb.Name != user.Name || usrdb.ScreenName != user.ScreenName
	}

	usrdb.FriendsCount = user.FriendsCount
	usrdb.IsProtected = user.IsProtected
	usrdb.Name = user.Name
	usrdb.ScreenName = user.ScreenName

	if isNew {
		err = database.CreateUser(db, usrdb)
	} else {
		err = database.UpdateUser(db, usrdb)
	}
	if err != nil {
		return err
	}
	if renamed || isNew {
		err = database.RecordUserPreviousName(db, user.Id, user.Name, user.ScreenName)
	}
	return err
}

func getTweetAndUpdateLatestReleaseTime(ctx context.Context, client *resty.Client, user *twitter.User, entity *UserEntity) ([]*twitter.Tweet, error) {
	tweets, err := user.GetMeidas(ctx, client, &utils.TimeRange{Min: entity.LatestReleaseTime()})
	if err != nil || len(tweets) == 0 {
		return nil, err
	}
	if err := entity.SetLatestReleaseTime(tweets[0].CreatedAt); err != nil {
		return nil, err
	}
	return tweets, nil
}

func DownloadUser(ctx context.Context, db *sqlx.DB, client *resty.Client, user *twitter.User, dir string) ([]PackgedTweet, error) {
	if user.Blocking || user.Muting {
		return nil, nil
	}

	_, loaded := syncedUsers.Load(user.Id)
	if loaded {
		log.WithField("user", user.Title()).Debugln("skiped downloaded user")
		return nil, nil
	}
	entity, err := syncUserAndEntity(db, user, dir)
	if err != nil {
		return nil, err
	}

	syncedUsers.Store(user.Id, entity)
	tweets, err := getTweetAndUpdateLatestReleaseTime(ctx, client, user, entity)
	if err != nil || len(tweets) == 0 {
		return nil, err
	}

	// 打包推文
	pts := make([]PackgedTweet, 0, len(tweets))
	for _, tw := range tweets {
		pts = append(pts, TweetInEntity{Tweet: tw, Entity: entity})
	}

	return BatchDownloadTweet(ctx, client, db, pts...), nil
}

func syncUserAndEntity(db *sqlx.DB, user *twitter.User, dir string) (*UserEntity, error) {
	if err := syncUser(db, user); err != nil {
		return nil, err
	}
	expectedTitle := utils.WinFileName(user.Title())

	entity, err := NewUserEntity(db, user.Id, dir)
	if err != nil {
		return nil, err
	}
	if err = syncPath(entity, expectedTitle); err != nil {
		return nil, err
	}
	return entity, nil
}

type TweetInEntity struct {
	Tweet  *twitter.Tweet
	Entity *UserEntity
}

func (pt TweetInEntity) GetTweet() *twitter.Tweet {
	return pt.Tweet
}

func (pt TweetInEntity) GetPath() string {
	path, err := pt.Entity.Path()
	if err != nil {
		return ""
	}
	return path
}

const userTweetRateLimit = 500
const userTweetMaxConcurrent = 100 // avoid DownstreamOverCapacityError

// var syncedListUsers = make(map[uint64]map[int64]struct{})
var syncedListUsers sync.Map //leid -> uid -> struct{}

// 需要请求多少次时间线才能获取完毕用户的推文？
func calcUserDepth(exist int, total int) int {
	if exist >= total {
		return 1
	}

	miss := total - exist
	depth := miss / twitter.AvgTweetsPerPage
	if miss%twitter.AvgTweetsPerPage != 0 {
		depth++
	}
	if exist == 0 {
		depth++ //对于新用户，需要多获取一个空页
	}
	return depth
}

type userInLstEntity struct {
	user *twitter.User
	leid *int
}

func shouldIngoreUser(user *twitter.User) bool {
	return user.Blocking || user.Muting
}

func BatchUserDownload(ctx context.Context, client *resty.Client, db *sqlx.DB, users []userInLstEntity, dir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	if len(users) == 0 {
		return nil, nil
	}

	uidToUser := make(map[uint64]*twitter.User)
	for _, u := range users {
		uidToUser[u.user.Id] = u.user
	}

	// channels
	tweetChan := make(chan PackgedTweet, MaxDownloadRoutine)
	errChan := make(chan PackgedTweet)
	// WG
	prodwg := sync.WaitGroup{}
	conswg := sync.WaitGroup{}
	// ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// logger
	updaterLogger := log.WithField("worker", "updating")
	getterLogger := log.WithField("worker", "getting")

	panicHandler := func() {
		if r := recover(); r != nil {
			cancel(fmt.Errorf("%v", r))
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, false)
			fmt.Printf("Recovered from panic: %v\n%s\n", r, buf[:n])
		}
	}

	missingTweets := 0
	depthByEntity := make(map[*UserEntity]int)
	// 大顶堆，以用户深度
	userEntityHeap := utils.NewHeap(func(lhs, rhs *UserEntity) bool {
		luser, ruser := uidToUser[lhs.Uid()], uidToUser[rhs.Uid()]
		lOnlyMater := luser.IsProtected && luser.Followstate == twitter.FS_FOLLOWING
		rOnlyMaster := ruser.IsProtected && ruser.Followstate == twitter.FS_FOLLOWING

		if lOnlyMater == rOnlyMaster {
			return depthByEntity[lhs] > depthByEntity[rhs]
		}
		return lOnlyMater // 优先让 master 获取只有他能看到的
	})

	start := time.Now()
	deepest := 0

	// pre-process
	func() {
		defer panicHandler()
		log.Infoln("start pre processing users")

		for _, userInLST := range users {
			var pathEntity *UserEntity
			var err error
			user := userInLST.user
			leid := userInLST.leid

			if shouldIngoreUser(user) {
				continue
			}

			pe, loaded := syncedUsers.Load(user.Id)
			if !loaded {
				pathEntity, err = syncUserAndEntity(db, user, dir)
				if err != nil {
					updaterLogger.WithField("user", user.Title()).Warnln("failed to update user or entity", err)
					continue
				}
				syncedUsers.Store(user.Id, pathEntity)

				// 同步所有现存的指向此用户的符号链接
				upath, _ := pathEntity.Path()
				linkds, err := database.GetUserLinks(db, user.Id)
				if err != nil {
					updaterLogger.WithField("user", user.Title()).Warnln("failed to get links to user:", err)
				}
				for _, linkd := range linkds {
					if err = updateUserLink(linkd, db, upath); err != nil {
						updaterLogger.WithField("user", user.Title()).Warnln("failed to update link:", err)
					}
					sl, _ := syncedListUsers.LoadOrStore(int(linkd.ParentLstEntityId), &sync.Map{})
					syncedList := sl.(*sync.Map)
					syncedList.Store(user.Id, struct{}{})
				}

				// 计算深度
				if user.MediaCount != 0 && user.IsVisiable() {
					missingTweets += max(0, user.MediaCount-int(pathEntity.record.MediaCount.Int32))
					depthByEntity[pathEntity] = calcUserDepth(int(pathEntity.record.MediaCount.Int32), user.MediaCount)
					userEntityHeap.Push(pathEntity)
					deepest = max(deepest, depthByEntity[pathEntity])
				}

				// 自动关注
				if user.IsProtected && user.Followstate == twitter.FS_UNFOLLOW && autoFollow {
					if err := twitter.FollowUser(ctx, client, user); err != nil {
						log.WithField("user", user.Title()).Warnln("failed to follow user:", err)
					} else {
						log.WithField("user", user.Title()).Debugln("follow request has been sent")
					}
				}
			} else {
				pathEntity = pe.(*UserEntity)
			}

			// 即便同步一个用户时也同步了所有指向此用户的链接，
			// 但此用户仍可能会是一个新的 “列表-用户”，所以判断此用户链接是否同步过，
			// 如果否，那么创建一个属于此列表的用户链接
			if leid == nil {
				continue
			}
			sl, _ := syncedListUsers.LoadOrStore(*leid, &sync.Map{})
			syncedList := sl.(*sync.Map)
			_, loaded = syncedList.LoadOrStore(user.Id, struct{}{})
			if loaded {
				continue
			}

			// 为当前列表的新用户创建符号链接
			upath, _ := pathEntity.Path()
			var linkname = pathEntity.Name()

			curlink := &database.UserLink{}
			curlink.Name = linkname
			curlink.ParentLstEntityId = int32(*leid)
			curlink.Uid = user.Id

			linkpath, err := curlink.Path(db)
			if err == nil {
				if err = os.Symlink(upath, linkpath); err == nil || os.IsExist(err) {
					err = database.CreateUserLink(db, curlink)
				}
			}
			if err != nil {
				updaterLogger.WithField("user", user.Title()).Warnln("failed to create link for user:", err)
			}
		}
	}()

	if userEntityHeap.Empty() {
		return nil, nil
	}
	log.Debugln("preprocessing finish, elapsed:", time.Since(start))
	log.Debugln("real members:", userEntityHeap.Size())
	log.Debugln("missing tweets:", missingTweets)
	log.Debugln("deepest:", deepest)

	clients := make([]*resty.Client, 0)
	clients = append(clients, client)
	clients = append(clients, additional...)

	producer := func(entity *UserEntity) {
		defer prodwg.Done()
		defer panicHandler()

		user := uidToUser[entity.Uid()]
		cli := twitter.SelectUserMediaClient(ctx, clients)
		if ctx.Err() != nil {
			userEntityHeap.Push(entity)
			return
		}
		if cli == nil {
			userEntityHeap.Push(entity)
			cancel(fmt.Errorf("no client available"))
			return
		}

		tweets, err := user.GetMeidas(ctx, cli, &utils.TimeRange{Min: entity.LatestReleaseTime()})
		if err == twitter.ErrWouldBlock {
			userEntityHeap.Push(entity)
			return
		}
		if v, ok := err.(*twitter.TwitterApiError); ok {
			// 客户端不再可用
			if v.Code == twitter.ErrExceedPostLimit {
				twitter.SetClientError(cli, fmt.Errorf("reached the limit for seeing posts today"))
				userEntityHeap.Push(entity)
				return
			} else if v.Code == twitter.ErrAccountLocked {
				twitter.SetClientError(cli, fmt.Errorf("account is locked"))
				userEntityHeap.Push(entity)
				return
			}
		}
		if ctx.Err() != nil {
			userEntityHeap.Push(entity)
			return
		}
		if err != nil {
			getterLogger.WithField("user", entity.Name()).Warnln("failed to get user medias:", err)
			return
		}

		if len(tweets) == 0 {
			if err := database.UpdateUserEntityMediCount(db, entity.Id(), user.MediaCount); err != nil {
				getterLogger.WithField("user", entity.Name()).Panicln("failed to update user medias count:", err)
			}
			return
		}

		// 确保该用户所有推文已推送并更新用户推文状态
		for _, tw := range tweets {
			pt := TweetInEntity{Tweet: tw, Entity: entity}
			select {
			case tweetChan <- &pt:
			case <-ctx.Done():
				return // 防止无消费者导致死锁
			}
		}

		if err := database.UpdateUserEntityTweetStat(db, entity.Id(), tweets[0].CreatedAt, user.MediaCount); err != nil {
			// 影响程序的正确性，必须 Panic
			getterLogger.WithField("user", entity.Name()).Panicln("failed to update user tweets stat:", err)
		}
	}

	// launch worker
	config := workerConfig{
		ctx:    ctx,
		wg:     &conswg,
		cancel: cancel,
		db:     db,
	}
	for i := 0; i < MaxDownloadRoutine; i++ {
		conswg.Add(1)
		go tweetDownloader(client, &config, errChan, tweetChan)
	}

	producerPool, err := ants.NewPool(min(userTweetMaxConcurrent, userEntityHeap.Size()))
	if err != nil {
		return nil, err
	}
	defer ants.Release()

	//closer
	go func() {
		// 按批次调用生产者
		for !userEntityHeap.Empty() && ctx.Err() == nil {
			selected := []int{}
			for count := 0; count < userTweetRateLimit && ctx.Err() == nil; {
				if userEntityHeap.Empty() {
					break
				}

				entity := userEntityHeap.Peek()
				depth := depthByEntity[entity]
				if depth > userTweetRateLimit {
					log.WithFields(log.Fields{
						"user":  entity.Name(),
						"depth": depth,
					}).Warnln("user depth greater than the max limit of window")
					userEntityHeap.Pop()
					continue
				}

				if depth+count > userTweetRateLimit {
					break
				}

				prodwg.Add(1)
				producerPool.Submit(func() {
					producer(entity)
				})
				selected = append(selected, depth)

				count += depth
				//delete(depthByEntity, entity)
				userEntityHeap.Pop()
			}
			log.Debugln(selected)
			prodwg.Wait()
		}
		close(tweetChan)
		log.Debugf("getting tweets completed, elapsed time: %v", time.Since(start))

		conswg.Wait()
		close(errChan)
	}()

	fails := []*TweetInEntity{}
	for pt := range errChan {
		fails = append(fails, pt.(*TweetInEntity))
	}
	log.Debugf("%d users unable to start", userEntityHeap.Size())
	return fails, context.Cause(ctx)
}

func downloadList(ctx context.Context, client *resty.Client, db *sqlx.DB, list twitter.ListBase, dir string, realDir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	expectedTitle := utils.WinFileName(list.Title())
	entity, err := NewListEntity(db, list.GetId(), dir)
	if err != nil {
		return nil, err
	}
	if err := syncPath(entity, expectedTitle); err != nil {
		return nil, err
	}

	members, err := list.GetMembers(ctx, client)
	if err != nil || len(members) == 0 {
		return nil, err
	}

	eid := entity.Id()
	log.Debugln("members:", len(members))
	packgedUsers := make([]userInLstEntity, len(members))
	for i, user := range members {
		packgedUsers[i] = userInLstEntity{user: user, leid: &eid}
	}
	return BatchUserDownload(ctx, client, db, packgedUsers, realDir, autoFollow, additional)
}

func syncList(db *sqlx.DB, list *twitter.List) error {
	listdb, err := database.GetLst(db, list.Id)
	if err != nil {
		return err
	}
	if listdb == nil {
		return database.CreateLst(db, &database.Lst{Id: list.Id, Name: list.Name, OwnerId: list.Creator.Id})
	}
	return database.UpdateLst(db, &database.Lst{Id: list.Id, Name: list.Name, OwnerId: list.Creator.Id})
}

func DownloadList(ctx context.Context, client *resty.Client, db *sqlx.DB, list twitter.ListBase, dir string, realDir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	tlist, ok := list.(*twitter.List)
	if ok {
		if err := syncList(db, tlist); err != nil {
			return nil, err
		}
	}
	return downloadList(ctx, client, db, list, dir, realDir, autoFollow, additional)
}

func syncLstAndGetMembers(ctx context.Context, client *resty.Client, db *sqlx.DB, lst twitter.ListBase, dir string) ([]userInLstEntity, error) {
	if v, ok := lst.(*twitter.List); ok {
		if err := syncList(db, v); err != nil {
			return nil, err
		}
	}

	// update lst path and record
	expectedTitle := utils.WinFileName(lst.Title())
	entity, err := NewListEntity(db, lst.GetId(), dir)
	if err != nil {
		return nil, err
	}
	if err := syncPath(entity, expectedTitle); err != nil {
		return nil, err
	}

	// get all members
	members, err := lst.GetMembers(ctx, client)
	if err != nil || len(members) == 0 {
		return nil, err
	}

	// bind lst entity to users for creating symlink
	packgedUsers := make([]userInLstEntity, 0, len(members))
	eid := entity.Id()
	for _, user := range members {
		packgedUsers = append(packgedUsers, userInLstEntity{user: user, leid: &eid})
	}
	return packgedUsers, nil
}

func BatchDownloadAny(ctx context.Context, client *resty.Client, db *sqlx.DB, lists []twitter.ListBase, users []*twitter.User, dir string, realDir string, autoFollow bool, additional []*resty.Client) ([]*TweetInEntity, error) {
	log.Debugln("start collecting users")
	packgedUsers := make([]userInLstEntity, 0)
	wg := sync.WaitGroup{}
	mtx := sync.Mutex{}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	for _, lst := range lists {
		wg.Add(1)
		go func(lst twitter.ListBase) {
			defer wg.Done()
			res, err := syncLstAndGetMembers(ctx, client, db, lst, dir)
			if err != nil {
				cancel(err)
			}
			log.Debugf("members of %s: %d", lst.Title(), len(res))
			mtx.Lock()
			defer mtx.Unlock()
			packgedUsers = append(packgedUsers, res...)
		}(lst)
	}
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	for _, usr := range users {
		packgedUsers = append(packgedUsers, userInLstEntity{user: usr, leid: nil})
	}

	log.Debugln("collected users:", len(packgedUsers))
	return BatchUserDownload(ctx, client, db, packgedUsers, realDir, autoFollow, additional)
}

// GenerateFileName generates two filenames: a truncated one (<= 200 chars) for disk,
// and an original, untruncated one for the database metadata.
func GenerateFileName(createdAt time.Time, tweetId uint64, index int, text string, ext string) (truncatedName string, originalName string) {
	dateStr := createdAt.Format("20060102")
	prefix := fmt.Sprintf("%s_%d_%d_", dateStr, tweetId, index)

	// Clean the text first
	cleanText := utils.WinFileName(text)

	// 1. Original (untruncated) name
	if cleanText == "" {
		originalName = fmt.Sprintf("%s_%d_%d%s", dateStr, tweetId, index, ext)
	} else {
		originalName = prefix + cleanText + ext
	}

	// 2. Truncated name (<= 200 chars total)
	maxTextLen := 200 - len(prefix) - len(ext)
	if maxTextLen <= 0 {
		needed := 200 - len(ext)
		if needed > 0 {
			truncatedName = prefix[:needed] + ext
		} else {
			truncatedName = ext
		}
		return truncatedName, originalName
	}

	if len(cleanText) > maxTextLen {
		cleanText = cleanText[:maxTextLen]
		for len(cleanText) > 0 && !utf8.ValidString(cleanText) {
			cleanText = cleanText[:len(cleanText)-1]
		}
	}
	cleanText = strings.TrimSpace(cleanText)

	if cleanText == "" {
		truncatedName = fmt.Sprintf("%s_%d_%d%s", dateStr, tweetId, index, ext)
	} else {
		truncatedName = prefix + cleanText + ext
	}

	return truncatedName, originalName
}

func drawProgressBar(processed int, total int) {
	if total <= 0 {
		fmt.Printf("\rProgress: [------------------------------] 0%% (0 / 0)")
		return
	}
	percent := (processed * 100) / total
	if percent > 100 {
		percent = 100
	}
	width := 30
	filled := (percent * width) / 100
	bar := make([]rune, width)
	for i := 0; i < width; i++ {
		if i < filled {
			bar[i] = '█'
		} else {
			bar[i] = '░'
		}
	}
	fmt.Printf("\rProgress: [%s] %d%% (%d / %d)", string(bar), percent, processed, total)
}

type dbWriteTask struct {
	tweet    *database.TweetRecord
	media    *database.MediaFileRecord
	log      *database.RollbackLog
	entityId int
	rawTweet *twitter.Tweet
}

func UpgradeDatabaseAndFiles(ctx context.Context, client *resty.Client, db *sqlx.DB, rootPath string, additional []*resty.Client) error {
	log.Infoln("Starting full database and filename upgrade...")

	errorJsonPath := filepath.Join(rootPath, ".data", "errors.json")
	dumper := NewDumper()
	if err := dumper.Load(errorJsonPath); err != nil {
		log.Warnf("Failed to load existing errors.json: %v", err)
	}
	defer func() {
		if err := dumper.Dump(errorJsonPath); err != nil {
			log.Errorf("\nFailed to dump missing files to %s: %v", errorJsonPath, err)
		} else if dumper.Count() > 0 {
			log.Infof("Successfully recorded %d pending/missing files to %s", dumper.Count(), errorJsonPath)
		}
	}()

	clients := make([]*resty.Client, 0)
	clients = append(clients, client)
	clients = append(clients, additional...)

	// 1. Get total media count for progress bar
	var totalMedia int
	err := db.Get(&totalMedia, "SELECT COALESCE(SUM(media_count), 0) FROM user_entities")
	if err != nil {
		totalMedia = 0
	}
	log.Infof("Estimated files to process: %d", totalMedia)

	// 2. Fetch all user entities
	type TempUserEntity struct {
		Id        int    `db:"id"`
		Uid       uint64 `db:"user_id"`
		Name      string `db:"name"`
		ParentDir string `db:"parent_dir"`
	}
	var entities []TempUserEntity
	err = db.Select(&entities, "SELECT id, user_id, name, parent_dir FROM user_entities")
	if err != nil {
		return fmt.Errorf("failed to query user entities: %v", err)
	}

	var initialProcessed int
	err = db.Get(&initialProcessed, "SELECT COUNT(*) FROM media_files WHERE download_status = 1")
	if err != nil {
		initialProcessed = 0
	}
	var processedCounter = int32(initialProcessed)
	var successUsers int32
	var failedUsers int32
	var missingFiles int32
	drawProgressBar(int(processedCounter), totalMedia)

	// 3. Start DB batch writer goroutine
	dbWriteChan := make(chan dbWriteTask, 5000)
	var writerWg sync.WaitGroup
	writerWg.Add(1)

	go func() {
		defer writerWg.Done()

		const batchSize = 1000
		var batch []dbWriteTask

		commitBatch := func() {
			if len(batch) == 0 {
				return
			}
			tx, err := db.Beginx()
			if err != nil {
				log.Errorf("\nFailed to begin transaction: %v", err)
				return
			}
			for _, task := range batch {
				if task.log != nil {
					_, err = tx.Exec("INSERT INTO rollback_logs(old_path, new_path, migrated_at) VALUES(?, ?, ?)", task.log.OldPath, task.log.NewPath, task.log.MigratedAt)
					if err != nil {
						log.Errorf("\nFailed to save rollback log: %v", err)
					}
				}
				if task.tweet != nil {
					err = database.SaveTweetTx(tx, task.tweet)
					if err != nil {
						log.Errorf("\nFailed to save tweet: %v", err)
					}
				}
				if task.media != nil {
					err = database.SaveMediaFileTx(tx, task.media)
					if err != nil {
						log.Errorf("\nFailed to save media file: %v", err)
					}
				}
				if task.rawTweet != nil && task.entityId != 0 {
					dumper.Push(task.entityId, task.rawTweet)
				}
			}
			err = tx.Commit()
			if err != nil {
				log.Errorf("\nFailed to commit transaction: %v", err)
			}
			batch = nil
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case task, ok := <-dbWriteChan:
				if !ok {
					commitBatch()
					return
				}
				batch = append(batch, task)
				if len(batch) >= batchSize {
					commitBatch()
				}
			case <-ticker.C:
				commitBatch()
			}
		}
	}()

	// 4. Start concurrent timeline checkers and file renamers
	var entityChan = make(chan TempUserEntity, len(entities))
	for _, entity := range entities {
		entityChan <- entity
	}
	close(entityChan)

	var workerWg sync.WaitGroup
	const numWorkers = 4

	for w := 0; w < numWorkers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for entity := range entityChan {
				if ctx.Err() != nil {
					return
				}

				// Reconstruct the user dir
				var userDir string
				if filepath.IsAbs(entity.ParentDir) {
					userDir = filepath.Join(entity.ParentDir, entity.Name)
				} else {
					userDir = filepath.Join(rootPath, entity.ParentDir, entity.Name)
				}

				// Get the user info from db to reconstruct twitter.User
				var user database.User
				err = db.Get(&user, "SELECT * FROM users WHERE id = ?", entity.Uid)
				if err != nil {
					continue
				}

				twUser := &twitter.User{
					Id:           user.Id,
					Name:         user.Name,
					ScreenName:   user.ScreenName,
					IsProtected:  user.IsProtected,
					FriendsCount: user.FriendsCount,
				}

				// Fetch their full media timeline with 3 retries on network failure
				var totalMediaForUser int
				err = db.Get(&totalMediaForUser, "SELECT COUNT(*) FROM media_files mf JOIN tweets t ON mf.tweet_id = t.id WHERE t.user_id = ?", entity.Uid)
				if err == nil && totalMediaForUser > 0 {
					var pendingCount int
					err = db.Get(&pendingCount, "SELECT COUNT(*) FROM media_files mf JOIN tweets t ON mf.tweet_id = t.id WHERE t.user_id = ? AND mf.download_status != 1", entity.Uid)
					if err == nil && pendingCount == 0 {
						var files []struct {
							Filename string `db:"filename"`
						}
						err = db.Select(&files, "SELECT filename FROM media_files mf JOIN tweets t ON mf.tweet_id = t.id WHERE t.user_id = ?", entity.Uid)
						if err == nil {
							allExist := true
							for _, f := range files {
								p := filepath.Join(userDir, f.Filename)
								if ok, _ := utils.PathExists(p); !ok {
									allExist = false
									break
								}
							}
							if allExist {
								atomic.AddInt32(&successUsers, 1)
								continue
							}
						}
					}
				}

				var tweets []*twitter.Tweet
				for retry := 0; retry < 3; retry++ {
					cli := twitter.SelectUserMediaClient(ctx, clients)
					if cli == nil {
						err = fmt.Errorf("no client available")
						break
					}
					tweets, err = twUser.GetMeidas(ctx, cli, nil)
					if err == nil {
						break
					}
					if ctx.Err() != nil {
						return
					}
					if v, ok := err.(*twitter.TwitterApiError); ok {
						if v.Code == twitter.ErrExceedPostLimit {
							twitter.SetClientError(cli, fmt.Errorf("reached the limit for seeing posts today"))
						} else if v.Code == twitter.ErrAccountLocked {
							twitter.SetClientError(cli, fmt.Errorf("account is locked"))
						}
					}
					if err == twitter.ErrWouldBlock {
						continue
					}
					if v, ok := err.(*utils.HttpStatusError); ok && v.Code == 429 {
						continue
					}
					if strings.Contains(err.Error(), "user unavailable") || strings.Contains(err.Error(), "user unavaiable") {
						break
					}
					log.Warnf("\n[Retry] Failed to fetch timeline for user %s (%s), retrying (%d/3)... Error: %v", twUser.Name, twUser.ScreenName, retry+1, err)
					time.Sleep(1500 * time.Millisecond)
				}

				if err != nil {
					atomic.AddInt32(&failedUsers, 1)
					log.Errorf("\n[Error] Failed to fetch timeline for user %s (%s) after 3 retries: %v", twUser.Name, twUser.ScreenName, err)
					continue
				}
				atomic.AddInt32(&successUsers, 1)

				for _, tweet := range tweets {
					if ctx.Err() != nil {
						return
					}
					tweet.Creator = twUser

					for i, u := range tweet.Urls {
						ext, err := utils.GetExtFromUrl(u)
						if err != nil {
							continue
						}

						// Generate deterministic filenames
						truncatedName, originalName := GenerateFileName(tweet.CreatedAt, tweet.Id, i+1, tweet.Text, ext)
						newPath := filepath.Join(userDir, truncatedName)

						// Check if the record already exists in the database
						mf, err := database.GetMediaFile(db, tweet.Id, u)
						if err == nil && mf != nil && mf.DownloadStatus == 1 {
							if exists, _ := utils.PathExists(newPath); exists {
								continue
							}
						}

						newExists, _ := utils.PathExists(newPath)

						// Case 2: New file exists on disk, but DB is missing the record (crash recovery)
						if newExists {
							dbTweet := &database.TweetRecord{
								Id:        tweet.Id,
								UserId:    tweet.Creator.Id,
								Text:      tweet.Text,
								CreatedAt: tweet.CreatedAt,
							}
							dbMedia := &database.MediaFileRecord{
								TweetId:          tweet.Id,
								Url:              u,
								Filename:         truncatedName,
								OriginalFilename: originalName,
								DownloadStatus:   1,
								DownloadedAt:     sql.NullTime{Time: time.Now(), Valid: true},
							}
							if fi, err := os.Stat(newPath); err == nil {
								dbMedia.FileSize = sql.NullInt64{Int64: fi.Size(), Valid: true}
							}

							select {
							case dbWriteChan <- dbWriteTask{tweet: dbTweet, media: dbMedia}:
							case <-ctx.Done():
								return
							}

							atomic.AddInt32(&processedCounter, 1)
							drawProgressBar(int(atomic.LoadInt32(&processedCounter)), totalMedia)
							continue
						}

						// Case 3: Match old file on disk using Modification Time metadata
						cleanText := utils.WinFileName(tweet.Text)
						var matchedOldPath string
						var foundOld bool

						// 1. First, check if defaultOldName exists and metadata (mtime) matches
						var defaultOldName string
						if i == 0 {
							defaultOldName = cleanText + ext
						} else {
							defaultOldName = fmt.Sprintf("%s(%d)%s", cleanText, i, ext)
						}
						defaultOldPath := filepath.Join(userDir, defaultOldName)

						if fi, err := os.Stat(defaultOldPath); err == nil {
							timeDiff := fi.ModTime().Sub(tweet.CreatedAt)
							if timeDiff < 0 {
								timeDiff = -timeDiff
							}
							if timeDiff <= 2*time.Second {
								matchedOldPath = defaultOldPath
								foundOld = true
							}
						}

						// 2. If default doesn't match (e.g. bracket shift or collision), scan directory by mtime metadata
						if !foundOld {
							files, err := os.ReadDir(userDir)
							if err == nil {
								var matchedFiles []string
								for _, f := range files {
									if f.IsDir() {
										continue
									}
									if isNewFormatName(f.Name()) {
										continue
									}
									if strings.EqualFold(filepath.Ext(f.Name()), ext) {
										fPath := filepath.Join(userDir, f.Name())
										if fi, err := os.Stat(fPath); err == nil {
											timeDiff := fi.ModTime().Sub(tweet.CreatedAt)
											if timeDiff < 0 {
												timeDiff = -timeDiff
											}
											if timeDiff <= 2*time.Second {
												matchedFiles = append(matchedFiles, f.Name())
											}
										}
									}
								}

								if len(matchedFiles) > 0 {
									// Try exact matching by bracket index first
									targetBracketStr := fmt.Sprintf("(%d)", i)
									for _, mfName := range matchedFiles {
										hasBracket := strings.Contains(mfName, "(")
										if (i == 0 && !hasBracket) || (i > 0 && strings.Contains(mfName, targetBracketStr)) {
											matchedOldPath = filepath.Join(userDir, mfName)
											foundOld = true
											break
										}
									}

									// If no exact bracket match, sort and match by relative download sequence index
									if !foundOld {
										targetIdx := 0
										for idx := 0; idx < i; idx++ {
											otherExt, err := utils.GetExtFromUrl(tweet.Urls[idx])
											if err == nil && strings.EqualFold(otherExt, ext) {
												targetIdx++
											}
										}

										sort.Slice(matchedFiles, func(a, b int) bool {
											extractNum := func(name string) int {
												start := strings.LastIndex(name, "(")
												end := strings.LastIndex(name, ")")
												if start != -1 && end != -1 && start < end {
													var val int
													if _, err := fmt.Sscanf(name[start+1:end], "%d", &val); err == nil {
														return val
													}
												}
												return 0
											}
											return extractNum(matchedFiles[a]) < extractNum(matchedFiles[b])
										})

										if targetIdx < len(matchedFiles) {
											matchedOldPath = filepath.Join(userDir, matchedFiles[targetIdx])
											foundOld = true
										}
									}
								}
							}
						}

						if foundOld {
							// Old file successfully matched on disk, rename it
							dbTweet := &database.TweetRecord{
								Id:        tweet.Id,
								UserId:    tweet.Creator.Id,
								Text:      tweet.Text,
								CreatedAt: tweet.CreatedAt,
							}
							dbMedia := &database.MediaFileRecord{
								TweetId:          tweet.Id,
								Url:              u,
								Filename:         truncatedName,
								OriginalFilename: originalName,
								DownloadStatus:   1, // Migrated
								DownloadedAt:     sql.NullTime{Time: time.Now(), Valid: true},
							}
							if fi, err := os.Stat(matchedOldPath); err == nil {
								dbMedia.FileSize = sql.NullInt64{Int64: fi.Size(), Valid: true}
							}
							dbLog := &database.RollbackLog{
								OldPath:    matchedOldPath,
								NewPath:    newPath,
								MigratedAt: time.Now(),
							}

							err = os.Rename(matchedOldPath, newPath)
							if err != nil {
								log.Errorf("\nFailed to rename %s to %s: %v", matchedOldPath, newPath, err)
								continue
							}

							select {
							case dbWriteChan <- dbWriteTask{tweet: dbTweet, media: dbMedia, log: dbLog}:
							case <-ctx.Done():
								os.Rename(newPath, matchedOldPath) // revert rename on context cancel
								return
							}
						} else {
							// Old file is missing from local disk
							// Write a download_status=2 (failed/pending) record to error database for future repair
							dbTweet := &database.TweetRecord{
								Id:        tweet.Id,
								UserId:    tweet.Creator.Id,
								Text:      tweet.Text,
								CreatedAt: tweet.CreatedAt,
							}
							dbMedia := &database.MediaFileRecord{
								TweetId:          tweet.Id,
								Url:              u,
								Filename:         truncatedName,
								OriginalFilename: originalName,
								DownloadStatus:   2, // Pending redownload
							}

							select {
							case dbWriteChan <- dbWriteTask{
								tweet:    dbTweet,
								media:    dbMedia,
								entityId: entity.Id,
								rawTweet: tweet,
							}:
							case <-ctx.Done():
								return
							}

							atomic.AddInt32(&missingFiles, 1)
						}

						atomic.AddInt32(&processedCounter, 1)
						drawProgressBar(int(atomic.LoadInt32(&processedCounter)), totalMedia)
					}
				}
			}
		}()
	}

	// 5. Wait for all checking workers to finish
	workerWg.Wait()
	close(dbWriteChan)

	// 6. Wait for DB batch writer to commit remaining batches and exit
	writerWg.Wait()



	fmt.Println()

	totalUsers := len(entities)
	failedCount := atomic.LoadInt32(&failedUsers)

	if failedCount == int32(totalUsers) && totalUsers > 0 {
		return fmt.Errorf("upgrade aborted: all %d users failed to fetch timelines. Please check your network connection, proxy settings, or Twitter API rate limits", totalUsers)
	}

	if failedCount > 0 {
		log.Warnf("Upgrade completed with warnings: %d/%d users failed to process due to timeline fetching errors. Run '--upgrade' again to retry.", failedCount, totalUsers)
		return nil
	}

	missingCount := atomic.LoadInt32(&missingFiles)
	if missingCount > 0 {
		log.Infof("Upgrade process completed. Found %d missing media files on disk. They have been logged as pending in database and will be auto-completed during your next download session.", missingCount)
	}

	log.Infoln("Upgrade completed successfully.")
	return nil
}

func RollbackUpgrade(db *sqlx.DB) error {
	log.Infoln("Starting database and filename rollback...")

	var logs []database.RollbackLog
	err := db.Select(&logs, "SELECT * FROM rollback_logs ORDER BY id DESC")
	if err != nil {
		return fmt.Errorf("failed to read rollback logs: %v", err)
	}

	total := len(logs)
	log.Infof("Found %d file modifications to rollback.", total)

	processed := 0
	drawProgressBar(processed, total)

	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_media_files_filename ON media_files(filename)")
	if err != nil {
		log.Warnf("failed to create index for media_files filename: %v", err)
	}

	const batchSize = 1000
	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}
		batch := logs[i:end]

		tx, err := db.Beginx()
		if err != nil {
			return err
		}

		var renamedLogs []database.RollbackLog
		var iterErr error

		for _, l := range batch {
			newExists, _ := utils.PathExists(l.NewPath)
			if newExists {
				err = os.Rename(l.NewPath, l.OldPath)
				if err != nil {
					iterErr = fmt.Errorf("failed to rename %s back to %s: %v", l.NewPath, l.OldPath, err)
					break
				}
				renamedLogs = append(renamedLogs, l)
			}

			_, err = tx.Exec("DELETE FROM rollback_logs WHERE id = ?", l.Id)
			if err != nil {
				iterErr = err
				break
			}

			filename := filepath.Base(l.NewPath)
			_, err = tx.Exec("DELETE FROM media_files WHERE filename = ?", filename)
			if err != nil {
				iterErr = err
				break
			}
		}

		if iterErr == nil {
			iterErr = tx.Commit()
		}

		if iterErr != nil {
			tx.Rollback()
			// Revert file renames to maintain consistency with the rolled-back database
			for _, rl := range renamedLogs {
				os.Rename(rl.OldPath, rl.NewPath)
			}
			return iterErr
		}

		processed += len(batch)
		drawProgressBar(processed, total)
	}

	fmt.Println()
	log.Infoln("Rollback completed successfully.")
	return nil
}

func isNewFormatName(name string) bool {
	if len(name) < 15 {
		return false
	}
	for i := 0; i < 8; i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	if name[8] != '_' {
		return false
	}
	secondUnderscore := strings.Index(name[9:], "_")
	dotIndex := strings.Index(name[9:], ".")
	idEnd := -1
	if secondUnderscore != -1 {
		idEnd = 9 + secondUnderscore
	} else if dotIndex != -1 {
		idEnd = 9 + dotIndex
	}
	if idEnd == -1 {
		return false
	}
	idStr := name[9:idEnd]
	if len(idStr) < 1 || len(idStr) > 20 {
		return false
	}
	for i := 0; i < len(idStr); i++ {
		if idStr[i] < '0' || idStr[i] > '9' {
			return false
		}
	}
	return true
}
