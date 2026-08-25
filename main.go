package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gookit/color"
	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/unkmonster/tmd/internal/database"
	"github.com/unkmonster/tmd/internal/downloading"
	"github.com/unkmonster/tmd/internal/twitter"
	"github.com/unkmonster/tmd/internal/utils"
	"gopkg.in/yaml.v3"
)

type Cookie struct {
	AuthCoken string `yaml:"auth_token"`
	Ct0       string `yaml:"ct0"`
}

type Config struct {
	RootPath           string `yaml:"root_path"`
	Cookie             Cookie `yaml:"cookie"`
	MaxDownloadRoutine int    `yaml:"max_download_routine"`
}

type userArgs struct {
	id         []uint64
	screenName []string
}

func (u *userArgs) GetUser(ctx context.Context, client *resty.Client) ([]*twitter.User, error) {
	users := []*twitter.User{}
	for _, id := range u.id {
		usr, err := twitter.GetUserById(ctx, client, id)
		if err != nil {
			return nil, err
		}
		users = append(users, usr)
	}

	for _, screenName := range u.screenName {
		usr, err := twitter.GetUserByScreenName(ctx, client, screenName)
		if err != nil {
			return nil, err
		}
		users = append(users, usr)
	}
	return users, nil
}

func (u *userArgs) Set(str string) error {
	if u.id == nil {
		u.id = make([]uint64, 0)
		u.screenName = make([]string, 0)
	}

	id, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		str, _ := strings.CutPrefix(str, "@")
		u.screenName = append(u.screenName, str)
	} else {
		u.id = append(u.id, id)
	}
	return nil
}

func (u *userArgs) String() string {
	return "string"
}

type intArgs struct {
	id []uint64
}

func (l *intArgs) Set(str string) error {
	if l.id == nil {
		l.id = make([]uint64, 0)
	}

	id, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return err
	}
	l.id = append(l.id, id)
	return nil
}

func (a *intArgs) String() string {
	return "string array"
}

type ListArgs struct {
	intArgs
}

func (l ListArgs) GetList(ctx context.Context, client *resty.Client) ([]*twitter.List, error) {
	lists := []*twitter.List{}
	for _, id := range l.id {
		list, err := twitter.GetLst(ctx, client, id)
		if err != nil {
			return nil, err
		}
		lists = append(lists, list)
	}
	return lists, nil
}

type Task struct {
	users []*twitter.User
	lists []twitter.ListBase
}

func printTask(task *Task) {
	if len(task.users) != 0 {
		fmt.Printf("users: %d\n", len(task.users))
	}
	for _, u := range task.users {
		fmt.Printf("    - %s\n", u.Title())
	}
	if len(task.lists) != 0 {
		fmt.Printf("lists: %d\n", len(task.lists))
	}
	for _, l := range task.lists {
		fmt.Printf("    - %s\n", l.Title())
	}
}

func MakeTask(ctx context.Context, client *resty.Client, usrArgs userArgs, listArgs ListArgs, follArgs userArgs) (*Task, error) {
	task := Task{}
	task.users = make([]*twitter.User, 0)
	task.lists = make([]twitter.ListBase, 0)

	users, err := usrArgs.GetUser(ctx, client)
	if err != nil {
		return nil, err
	}
	task.users = append(task.users, users...)

	lists, err := listArgs.GetList(ctx, client)
	if err != nil {
		return nil, err
	}
	for _, list := range lists {
		task.lists = append(task.lists, list)
	}

	// fo
	users, err = follArgs.GetUser(ctx, client)
	if err != nil {
		return nil, err
	}
	for _, user := range users {
		task.lists = append(task.lists, user.Following())
	}
	return &task, nil
}

type storePath struct {
	root   string
	users  string
	data   string
	db     string
	errorj string
}

func newStorePath(root string) (*storePath, error) {
	ph := storePath{}
	ph.root = root
	ph.users = filepath.Join(root, "users")
	ph.data = filepath.Join(root, ".data")

	ph.db = filepath.Join(ph.data, "foo.db")
	ph.errorj = filepath.Join(ph.data, "errors.json")

	// ensure folder exist
	err := os.Mkdir(ph.root, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	err = os.Mkdir(ph.users, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	err = os.Mkdir(ph.data, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &ph, nil
}

func initLogger(dbg bool, logFile io.Writer, logKeepFile io.Writer) {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})

	if dbg {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.AddHook(lfshook.NewHook(logFile, nil))
	log.AddHook(lfshook.NewHook(logKeepFile, nil))
}

func main() {
	//flags
	var usrArgs userArgs
	var listArgs ListArgs
	var follArgs userArgs
	var confArg bool
	var dbg bool
	var autoFollow bool
	var noRetry bool
	var upgradeArg bool
	var rollbackArg bool

	flag.BoolVar(&confArg, "conf", false, "reconfigure")
	flag.Var(&usrArgs, "user", "download tweets from the user specified by user_id/screen_name since the last download")
	flag.Var(&listArgs, "list", "batch download each member from list specified by list_id")
	flag.Var(&follArgs, "foll", "batch download each member followed by the user specified by user_id/screen_name")
	flag.BoolVar(&dbg, "dbg", false, "display debug message")
	flag.BoolVar(&autoFollow, "auto-follow", false, "send follow request automatically to protected users")
	flag.BoolVar(&noRetry, "no-retry", false, "quickly exit without retrying failed tweets")
	flag.BoolVar(&upgradeArg, "upgrade", false, "upgrade database and rename existing files")
	flag.BoolVar(&rollbackArg, "rollback", false, "restore file renames recorded by upgrade and clear upgrade completion state")
	flag.Parse()
	if upgradeArg && rollbackArg {
		fmt.Fprintln(os.Stderr, "-upgrade and -rollback cannot be used together")
		os.Exit(2)
	}

	var err error

	// context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var homepath string
	if runtime.GOOS == "windows" {
		homepath = os.Getenv("appdata")
	} else {
		homepath = os.Getenv("HOME")
	}
	if homepath == "" {
		panic("failed to get home path from env")
	}

	appRootPath := filepath.Join(homepath, ".tmd2")
	confPath := filepath.Join(appRootPath, "conf.yaml")
	cliLogPath := filepath.Join(appRootPath, "client.log")
	logPath := filepath.Join(appRootPath, "tmd2.log")
	logKeepPath := filepath.Join(appRootPath, "tmd2_KEEP.log")
	additionalCookiesPath := filepath.Join(appRootPath, "additional_cookies.yaml")
	if err = os.MkdirAll(appRootPath, 0755); err != nil {
		log.Fatalln("failed to make app dir", err)
	}

	// init logger
	logFile, err := os.OpenFile(logPath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalln("failed to create log file:", err)
	}
	defer logFile.Close()

	logKeepFile, err := os.OpenFile(logKeepPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalln("failed to create keep log file:", err)
	}
	defer logKeepFile.Close()

	initLogger(dbg, logFile, logKeepFile)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(sigChan)
	go func() {
		sig, ok := <-sigChan
		if ok {
			log.Warnln("[listener] caught signal:", sig)
			cancel()
		}
	}()

	// report at exit
	defer func() {
		if dbg {
			twitter.ReportRequestCount()
		}
	}()

	// read/write config
	conf, err := readConf(confPath)
	if os.IsNotExist(err) || confArg {
		conf, err = promptConfig(confPath)
		if err != nil {
			log.Fatalln("config failure with", err)
		}
	}
	if err != nil {
		log.Fatalln("failed to load config:", err)
	}
	if confArg {
		log.Println("config done")
		return
	}
	log.Infoln("config is loaded")
	if conf.MaxDownloadRoutine > 0 {
		downloading.MaxDownloadRoutine = conf.MaxDownloadRoutine
	}

	// ensure store path exist
	pathHelper, err := newStorePath(conf.RootPath)
	if err != nil {
		log.Fatalln("failed to make store dir:", err)
	}

	if rollbackArg {
		if err := backupDatabase(ctx, pathHelper.db); err != nil {
			log.Fatalln("failed to backup database before rollback:", err)
		}

		db, err := connectDatabase(pathHelper.db, conf.RootPath, false)
		if err != nil {
			log.Fatalln("failed to connect to database:", err)
		}
		defer db.Close()
		log.Infoln("database is connected")

		if err := downloading.RollbackUpgrade(ctx, db); err != nil {
			log.Fatalln("rollback failed:", err)
		}
		return
	}

	if upgradeArg {
		if err := backupDatabase(ctx, pathHelper.db); err != nil {
			log.Fatalln("failed to backup database before upgrade:", err)
		}
	}

	// sign in
	client, screenName, err := twitter.Login(ctx, conf.Cookie.AuthCoken, conf.Cookie.Ct0)
	if err != nil {
		log.Fatalln("failed to login:", err)
	}
	twitter.EnableRateLimit(client)
	if dbg {
		twitter.EnableRequestCounting(client)
	}
	log.Infoln("signed in as:", color.FgLightBlue.Render(screenName))

	// load additional cookies
	cookies, err := readAdditionalCookies(additionalCookiesPath)
	if err != nil {
		log.Warnln("failed to load additional cookies:", err)
	}
	log.Debugln("loaded additional cookies:", len(cookies))
	addtional := batchLogin(ctx, dbg, cookies, screenName)

	// set clients logger
	cliLogFile, err := os.OpenFile(cliLogPath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalln("failed to create log file:", err)
	}
	defer cliLogFile.Close()
	setClientLogger(client, cliLogFile)
	for _, cli := range addtional {
		setClientLogger(cli, cliLogFile)
	}

	if upgradeArg {
		db, err := connectDatabase(pathHelper.db, conf.RootPath, true)
		if err != nil {
			log.Fatalln("failed to connect to database:", err)
		}
		defer db.Close()
		log.Infoln("database is connected")

		if err := downloading.UpgradeDatabaseAndFiles(ctx, client, db, conf.RootPath, addtional); err != nil {
			log.Fatalln("upgrade failed:", err)
		}
		return
	}

	// load previous tweets
	dumper := downloading.NewDumper()
	err = dumper.Load(pathHelper.errorj)
	if err != nil {
		log.Fatalln("failed to load previous tweets", err)
	}
	log.Infoln("loaded previous failed tweets:", dumper.Count())

	// collect tasks
	task, err := MakeTask(ctx, client, usrArgs, listArgs, follArgs)
	if err != nil {
		log.Fatalln("failed to parse cmd args:", err)
	}

	// connect db
	db, err := connectDatabase(pathHelper.db, conf.RootPath, true)
	if err != nil {
		log.Fatalln("failed to connect to database:", err)
	}
	defer db.Close()
	log.Infoln("database is connected")

	// dump failed tweets at exit
	var todump = make([]*downloading.TweetInEntity, 0)
	defer func() {
		if err := dumper.Dump(pathHelper.errorj); err != nil {
			log.Errorf("failed to save retry queue: %v", err)
		} else {
			log.Infof("%d tweets have been dumped and will be downloaded the next time the program runs", dumper.Count())
			if err := downloading.CommitPersistedRetryProgress(db, todump); err != nil {
				log.Errorf("retry queue was saved, but failed to advance persisted timeline watermarks: %v", err)
			}
		}
	}()

	// retry failed tweets at exit
	defer func() {
		for _, te := range todump {
			dumper.Push(te.Entity.Id(), te.Tweet)
		}
		// 如果手动取消，不尝试重试，快速终止进程
		if ctx.Err() != context.Canceled && !noRetry {
			if err := retryFailedTweets(ctx, dumper, db, client); err != nil {
				log.Errorf("failed to retry queued tweets: %v", err)
			}
		}
	}()

	// do job
	if len(task.users) == 0 && len(task.lists) == 0 {
		return
	}
	log.Infoln("start working for...")
	printTask(task)

	todump, err = downloading.BatchDownloadAny(ctx, client, db, task.lists, task.users, pathHelper.root, pathHelper.users, autoFollow, addtional)
	if err != nil {
		log.Errorln("failed to download:", err)
	}
}

func setClientLogger(client *resty.Client, out io.Writer) {
	logger := log.New()
	logger.SetLevel(log.InfoLevel)
	logger.SetOutput(out)
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		DisableQuote:  true,
	})
	client.SetLogger(logger)
}

func connectDatabase(path string, rootPath string, migrate bool) (*sqlx.DB, error) {
	ex, err := utils.PathExists(path)
	if err != nil {
		return nil, err
	}
	if !ex && !migrate {
		return nil, fmt.Errorf("database does not exist: %s", path)
	}

	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&busy_timeout=2147483647", path)
	db, err := sqlx.Connect("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	database.RootPath = rootPath
	if migrate {
		if err := database.MigrateDatabase(db, rootPath); err != nil {
			db.Close()
			return nil, err
		}
	}
	//db.SetMaxOpenConns(1)
	if !ex {
		log.Debugln("created new db file", path)
	}
	return db, nil
}

func readConf(path string) (*Config, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var result Config
	err = yaml.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func writeConf(path string, conf *Config) error {
	file, err := os.OpenFile(path, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, bytes.NewReader(data))
	return err
}

func promptConfig(saveto string) (*Config, error) {
	conf := Config{}
	scan := bufio.NewScanner(os.Stdin)

	print("enter storage dir: ")
	scan.Scan()
	storePath := scan.Text()
	// 确保路径可用
	err := os.MkdirAll(storePath, 0755)
	if err != nil {
		return nil, err
	}
	storePath, err = filepath.Abs(storePath)
	if err != nil {
		return nil, err
	}

	conf.RootPath = storePath

	print("enter auth_token: ")
	scan.Scan()
	conf.Cookie.AuthCoken = scan.Text()

	print("enter ct0: ")
	scan.Scan()
	conf.Cookie.Ct0 = scan.Text()

	print("enter max download routine: ")
	scan.Scan()
	conf.MaxDownloadRoutine, err = strconv.Atoi(scan.Text())
	if err != nil {
		return nil, err
	}

	return &conf, writeConf(saveto, &conf)
}

func retryFailedTweets(ctx context.Context, dumper *downloading.TweetDumper, db *sqlx.DB, client *resty.Client) error {
	if dumper.Count() == 0 {
		return nil
	}

	log.Infoln("starting to retry failed tweets")
	legacy, err := dumper.GetTotal(db)
	if err != nil {
		return err
	}

	toretry := make([]downloading.PackgedTweet, 0, len(legacy))
	for _, leg := range legacy {
		toretry = append(toretry, leg)
	}

	newFails := downloading.BatchDownloadTweet(ctx, client, db, toretry...)
	for _, te := range legacy {
		dumper.Remove(te.Entity.Id(), te.Tweet)
	}
	for _, pt := range newFails {
		te := pt.(*downloading.TweetInEntity)
		dumper.Push(te.Entity.Id(), te.Tweet)
	}

	return nil
}

func readAdditionalCookies(path string) ([]*Cookie, error) {
	res := []*Cookie{}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return res, yaml.Unmarshal(data, &res)
}

func batchLogin(ctx context.Context, dbg bool, cookies []*Cookie, master string) []*resty.Client {
	if len(cookies) == 0 {
		return nil
	}

	added := sync.Map{}
	msgs := make([]string, len(cookies))
	clients := []*resty.Client{}
	wg := sync.WaitGroup{}
	mtx := sync.Mutex{}
	added.Store(master, struct{}{})

	for i, cookie := range cookies {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			cli, sn, err := twitter.Login(ctx, cookie.AuthCoken, cookie.Ct0)
			if err != nil {
				msgs[index] = fmt.Sprintf("    - ? %v\n", err)
				return
			}
			if _, loaded := added.LoadOrStore(sn, struct{}{}); loaded {
				msgs[index] = "    - ? repeated\n"
				return
			}
			twitter.EnableRateLimit(cli)
			if dbg {
				twitter.EnableRequestCounting(cli)
			}
			mtx.Lock()
			defer mtx.Unlock()
			clients = append(clients, cli)
			msgs[index] = fmt.Sprintf("    - %s\n", sn)
		}(i)
	}

	wg.Wait()
	log.Infoln("loaded additional accounts:", len(clients))
	for _, msg := range msgs {
		fmt.Print(msg)
	}
	return clients
}

func backupDatabase(ctx context.Context, dbPath string) error {
	ex, err := utils.PathExists(dbPath)
	if err != nil {
		return err
	}
	if !ex {
		log.Infoln("Database does not exist yet; no pre-upgrade backup is needed.")
		return nil
	}

	timestamp := time.Now().Format("20060102_150405.000000000")
	backupPath := fmt.Sprintf("%s.backup_%s", dbPath, timestamp)

	placeholder, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if err := placeholder.Close(); err != nil {
		os.Remove(backupPath)
		return err
	}

	backupComplete := false
	defer func() {
		if !backupComplete {
			if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
				log.Warnf("failed to remove incomplete database backup %s: %v", backupPath, err)
			}
		}
	}()

	log.Infof("Creating SQLite online backup: %s", backupPath)

	sourceDsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=60000", filepath.ToSlash(dbPath))
	sourceDb, err := sql.Open("sqlite3", sourceDsn)
	if err != nil {
		return err
	}
	defer sourceDb.Close()
	sourceDb.SetMaxOpenConns(1)

	destinationDb, err := sql.Open("sqlite3", backupPath)
	if err != nil {
		return err
	}
	defer destinationDb.Close()
	destinationDb.SetMaxOpenConns(1)

	sourceConn, err := sourceDb.Conn(ctx)
	if err != nil {
		return err
	}
	destinationConn, err := destinationDb.Conn(ctx)
	if err != nil {
		sourceConn.Close()
		return err
	}

	err = sourceConn.Raw(func(sourceDriverConn any) error {
		sourceSqliteConn, ok := sourceDriverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected source SQLite driver connection type %T", sourceDriverConn)
		}
		return destinationConn.Raw(func(destinationDriverConn any) error {
			destinationSqliteConn, ok := destinationDriverConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("unexpected destination SQLite driver connection type %T", destinationDriverConn)
			}

			backup, err := destinationSqliteConn.Backup("main", sourceSqliteConn, "main")
			if err != nil {
				return err
			}

			for {
				done, stepErr := backup.Step(2048)
				if stepErr != nil {
					backup.Finish()
					return stepErr
				}
				if done {
					return backup.Finish()
				}
				select {
				case <-ctx.Done():
					backup.Finish()
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	})
	destinationConn.Close()
	sourceConn.Close()
	if err != nil {
		return err
	}

	var integrity string
	if err := destinationDb.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		return fmt.Errorf("failed to verify database backup: %w", err)
	}
	if integrity != "ok" {
		return fmt.Errorf("database backup integrity check failed: %s", integrity)
	}

	backupComplete = true
	log.Infoln("Database backup completed and verified successfully.")
	return nil
}
