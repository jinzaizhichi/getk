package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"getk/appconfig"
	"getk/dbconn"

	"github.com/longportapp/openapi-go/config"
	"github.com/longportapp/openapi-go/quote"
)

type Candlestick struct {
	Symbol    string
	Timestamp time.Time
	Open      float64
	Close     float64
	High      float64
	Low       float64
	Volume    int64
	Turnover  float64
}
type job struct {
	symbol string
	date   time.Time
}

// 使用 appconfig.AccountConfig 的类型别名，避免重复定义
type AccountConfig = appconfig.AccountConfig

type RetryConfig struct {
	MaxAttempts int
	BaseDelayMS int
	MaxDelayMS  int
}

type FetchResult struct {
	Account string
	Worker  int
	Symbol  string
	Ok      bool
	Err     error
	Records int
	Elapsed time.Duration
}

// 统一生成安全的表名，避免与保留关键字冲突
func safeTableName(symbol string) string {
	base := strings.Split(symbol, ".")[0]
	region := strings.Split(symbol, ".")[1]
	name := strings.ToLower(base)
	regionName := strings.ToLower(region)
	return name + "_" + regionName
}

// 自动创建数据表（若不存在）
func EnsureTable(db *sql.DB, symbol string) error {
	tableName := safeTableName(symbol)
	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
timestamp   TIMESTAMPTZ PRIMARY KEY,
open        DOUBLE PRECISION,
close       DOUBLE PRECISION,
high        DOUBLE PRECISION,
low         DOUBLE PRECISION,
volume      BIGINT,
turnover    DOUBLE PRECISION
)`, tableName)
	_, err := db.Exec(createSQL)
	return err
}

// getPeriodFromConfig 根据配置字符串返回对应的Period类型
func getPeriodFromConfig(periodStr string) quote.Period {
	switch periodStr {
	case "OneMinute":
		return quote.PeriodOneMinute
	case "FiveMinute":
		return quote.PeriodFiveMinute
	case "FifteenMinute":
		return quote.PeriodFifteenMinute
	case "ThirtyMinute":
		return quote.PeriodThirtyMinute
	default:
		return quote.PeriodOneMinute
	}
}

// getAdjustTypeFromConfig 根据配置字符串返回对应的AdjustType类型
func getAdjustTypeFromConfig(adjustStr string) quote.AdjustType {
	switch adjustStr {
	case "No":
		return quote.AdjustTypeNo
	case "ForwardAdjust":
		return quote.AdjustTypeForward
	default:
		return quote.AdjustTypeNo
	}
}

// configPath 返回与可执行文件同级的 config 目录下的文件路径
func configPath(filename string) string {
	// 1) 优先使用环境变量 CONFIG_DIR（可设置为绝对或相对目录）
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}

	// 2) 优先尝试当前工作目录下的 config 目录（兼容 go run）
	if wd, err := os.Getwd(); err == nil {
		p := filepath.Join(wd, "config", filename)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	// 3) 回退到可执行文件所在目录的 config 目录（兼容已编译二进制的部署场景）
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		p := filepath.Join(dir, "config", filename)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	// 4) 最后回退到相对路径，交由后续读取时报错并提示
	return filepath.Join(".", "config", filename)
}

func InsertCandlesticks(db *sql.DB, symbol string, candlesticks []Candlestick) error {
	tableName := safeTableName(symbol)
	insertStmt, err := db.Prepare(fmt.Sprintf(
		`INSERT INTO %s (timestamp, open, close, high, low, volume, turnover)
 VALUES ($1, $2, $3, $4, $5, $6, $7)
 ON CONFLICT (timestamp) DO NOTHING`,
		tableName,
	))
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for _, c := range candlesticks {
		_, err := insertStmt.Exec(c.Timestamp, c.Open, c.Close, c.High, c.Low, c.Volume, c.Turnover)
		if err != nil {
			log.Printf("Insert failed: %v\n", err)
		}
	}

	return nil
}

func main() {
	// 加载应用配置
	appCfg, err := appconfig.Load(configPath("config.yaml"))
	if err != nil {
		log.Fatalf("读取应用配置失败: %v", err)
	}

	// 解析日期
	dates, err := appCfg.ParseDates()
	if err != nil {
		log.Fatalf("解析日期失败: %v", err)
	}

	// 通过独立包加载数据库配置并建立连接
	dbCfg, err := dbconn.Load(configPath("db.yaml"))
	if err != nil {
		log.Fatalf("读取数据库配置失败: %v", err)
	}
	db, err := dbconn.Open(dbCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 获取期间类型
	period := getPeriodFromConfig(appCfg.Settings.Period)
	adjustType := getAdjustTypeFromConfig(appCfg.Settings.AdjustType)

	fmt.Printf("开始数据获取任务...\n")
	fmt.Printf("配置的股票数量: %d\n", len(appCfg.Symbols))
	fmt.Printf("配置的日期数量: %d\n", len(dates))
	fmt.Printf("总任务数: %d\n\n", len(appCfg.Symbols)*len(dates))

	// 预先确保所有表存在（可并发安全，若不存在则创建）
	for _, symbol := range appCfg.Symbols {
		if err := EnsureTable(db, symbol); err != nil {
			fmt.Printf("   创建表失败: %v\n", err)
		}
	}

	// 单账号模式：加载 longport.yaml
	acc, err := appconfig.LoadLongportAccount(configPath("longport.yaml"))
	if err != nil {
		log.Fatalf("读取 longport.yaml 失败: %v", err)
	}

	// 创建单账号的 QuoteContext（通过临时配置文件）
	tmpPath := filepath.Join(os.TempDir(), "longport_single.yaml")
	content := fmt.Sprintf(
		"longport:\n  app_key: %q\n  app_secret: %q\n  access_token: %q\n  region: %q\n",
		acc.AppKey, acc.AppSecret, acc.AccessToken, acc.Region,
	)
	if err := os.WriteFile(tmpPath, []byte(content), 0600); err != nil {
		log.Fatalf("写入临时配置失败: %v", err)
	}
	confAcc, err := config.New(config.WithFilePath(tmpPath))
	if err != nil {
		log.Fatalf("加载行情配置失败: %v", err)
	}
	accCtx, err := quote.NewFromCfg(confAcc)
	if err != nil {
		log.Fatalf("创建行情上下文失败: %v", err)
	}
	defer accCtx.Close()

	// 重试策略（用于 SDK 拉取失败时的指数退避重试）
	retryCfg := RetryConfig{MaxAttempts: 3, BaseDelayMS: 500, MaxDelayMS: 2000}

	// 构建任务队列（所有 symbols x dates）
	totalTasks := len(appCfg.Symbols) * len(dates)
	var currentTask int64
	jobs := make(chan job, totalTasks)
	for _, symbol := range appCfg.Symbols {
		for _, date := range dates {
			jobs <- job{symbol: symbol, date: date}
		}
	}
	close(jobs)

	// 账号级速率限制器
	rps := acc.RPS
	if rps <= 0 {
		rps = 10
	}
	interval := time.Second / time.Duration(rps)
	limiter := time.NewTicker(interval)
	defer limiter.Stop()

	// 启动 worker 池
	threads := acc.Threads
	if threads <= 0 {
		threads = 5
	}
	var wg sync.WaitGroup
	var successCount int64
	var failCount int64
	wg.Add(threads)
	for w := 0; w < threads; w++ {
		workerID := w + 1
		go func(workerID int, jobs <-chan job) {
			defer wg.Done()
			for j := range jobs {
				t0 := time.Now()
				<-limiter.C
				cur := atomic.AddInt64(&currentTask, 1)
				fmt.Printf("  [进度=%d/%d] 线程=%d 正在查询 %s 的 %s 数据...\n", cur, totalTasks, workerID, j.symbol, j.date.Format("2006-01-02"))

				start := time.Date(j.date.Year(), j.date.Month(), j.date.Day(), 0, 0, 0, 0, time.UTC)
				end := time.Date(j.date.Year(), j.date.Month(), j.date.Day(), 23, 59, 59, 0, time.UTC)

				var fetched bool
				for attempt := 1; attempt <= retryCfg.MaxAttempts; attempt++ {
					candlesticks, err := accCtx.HistoryCandlesticksByDate(
						context.Background(),
						j.symbol,
						period,
						adjustType,
						&start,
						&end,
					)
					if err != nil {
						if attempt < retryCfg.MaxAttempts {
							base := retryCfg.BaseDelayMS * (1 << (attempt - 1))
							if base > retryCfg.MaxDelayMS {
								base = retryCfg.MaxDelayMS
							}
							backoff := time.Duration(base) * time.Millisecond
							fmt.Printf("  线程=%d 重试 %d/%d (错误: %v, 等待=%dms)\n", workerID, attempt, retryCfg.MaxAttempts, err, backoff.Milliseconds())
							time.Sleep(backoff)
							continue
						}
						elapsed := time.Since(t0)
						atomic.AddInt64(&failCount, 1)
						fmt.Printf("  线程=%d 失败 (耗时=%dms): %v\n", workerID, elapsed.Milliseconds(), err)
						break
					}

					var records []Candlestick
					for _, c := range candlesticks {
						if c.Open == nil || c.Close == nil || c.High == nil || c.Low == nil {
							continue
						}
						turnover := 0.0
						if c.Turnover != nil {
							turnover = c.Turnover.InexactFloat64()
						}
						records = append(records, Candlestick{
							Symbol:    j.symbol,
							Timestamp: time.Unix(c.Timestamp, 0),
							Open:      c.Open.InexactFloat64(),
							Close:     c.Close.InexactFloat64(),
							High:      c.High.InexactFloat64(),
							Low:       c.Low.InexactFloat64(),
							Volume:    c.Volume,
							Turnover:  turnover,
						})
					}

					if err := InsertCandlesticks(db, j.symbol, records); err != nil {
						elapsed := time.Since(t0)
						fmt.Printf("  线程=%d 数据库插入失败 (耗时=%dms): %v\n", workerID, elapsed.Milliseconds(), err)
						atomic.AddInt64(&failCount, 1)
						break
					}

					elapsed := time.Since(t0)
					fmt.Printf("  线程=%d 完成 (记录=%d, 耗时=%dms)\n", workerID, len(records), elapsed.Milliseconds())
					atomic.AddInt64(&successCount, 1)
					fetched = true
					break
				}

				if !fetched {
					// 已在失败路径计数与打印
				}
			}
		}(workerID, jobs)
	}

	wg.Wait()
	fmt.Printf(" 所有数据已保存到PostgreSQL！\n")
	fmt.Printf("成功=%d 失败=%d 总任务=%d\n", successCount, failCount, totalTasks)

}
