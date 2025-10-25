package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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

// 移除 GetOrCreateTable，改为直接根据 symbol 推导表名
func InsertCandlesticks(db *sql.DB, symbol string, candlesticks []Candlestick) error {
	tableName := strings.Split(symbol, ".")[0]
	tableName = strings.ToLower(tableName)

	// Prepare existence check and insert statements for efficiency
	existsStmt, err := db.Prepare(fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE timestamp = $1)`, tableName))
	if err != nil {
		return err
	}
	defer existsStmt.Close()

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
		var exists bool
		if err := existsStmt.QueryRow(c.Timestamp).Scan(&exists); err != nil {
			log.Printf("Existence check failed: %v\n", err)
			continue
		}
		if exists {
			// Skip duplicates
			continue
		}

		_, err := insertStmt.Exec(c.Timestamp, c.Open, c.Close, c.High, c.Low, c.Volume, c.Turnover)
		if err != nil {
			log.Printf("Insert failed: %v\n", err)
		}
	}

	return nil
}

// 自动创建数据表（若不存在）
func EnsureTable(db *sql.DB, symbol string) error {
	tableName := strings.Split(symbol, ".")[0]
	tableName = strings.ToLower(tableName)
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

	// 通过配置文件加载 Longbridge 配置 (YAML/TOML)
	conf, err := config.New(config.WithFilePath(configPath("longport.yaml")))
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}

	qctx, err := quote.NewFromCfg(conf)
	if err != nil {
		log.Fatal(err)
	}
	defer qctx.Close()

	// 获取期间类型
	period := getPeriodFromConfig(appCfg.Settings.Period)
	adjustType := getAdjustTypeFromConfig(appCfg.Settings.AdjustType)

	fmt.Printf("开始数据获取任务...\n")
	fmt.Printf("配置的股票数量: %d\n", len(appCfg.Symbols))
	fmt.Printf("配置的日期数量: %d\n", len(dates))
	fmt.Printf("总任务数: %d\n\n", len(appCfg.Symbols)*len(dates))

	totalTasks := len(appCfg.Symbols) * len(dates)
	currentTask := 0

	// 循环处理每个股票的每个日期
	for _, symbol := range appCfg.Symbols {
		fmt.Printf("📈 开始处理股票: %s\n", symbol)

		// 确保目标表存在（自动创建）
		if err := EnsureTable(db, symbol); err != nil {
			fmt.Printf("  ❌ 创建表失败: %v\n", err)
			continue
		}

		for _, date := range dates {
			currentTask++

			// 显示进度
			fmt.Printf("  [%d/%d] 正在查询 %s 的 %s 数据...",
				currentTask, totalTasks, symbol, date.Format("2006-01-02"))

			// 设置查询时间范围（一整天）
			start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
			end := time.Date(date.Year(), date.Month(), date.Day(), 23, 59, 59, 0, time.UTC)

			// 获取K线数据
			candlesticks, err := qctx.HistoryCandlesticksByDate(
				context.Background(),
				symbol,
				period,
				adjustType,
				&start,
				&end,
			)
			if err != nil {
				fmt.Printf(" ❌ 失败: %v\n", err)
				continue
			}

			// 转换数据格式
			var records []Candlestick
			for _, c := range candlesticks {
				records = append(records, Candlestick{
					Symbol:    symbol,
					Timestamp: time.Unix(c.Timestamp, 0),
					Open:      c.Open.InexactFloat64(),
					Close:     c.Close.InexactFloat64(),
					High:      c.High.InexactFloat64(),
					Low:       c.Low.InexactFloat64(),
					Volume:    c.Volume,
					Turnover:  c.Turnover.InexactFloat64(),
				})
			}

			// 插入数据
			err = InsertCandlesticks(db, symbol, records)
			if err != nil {
				fmt.Printf(" ❌ 数据库插入失败: %v\n", err)
				continue
			}

			fmt.Printf(" ✅ 完成 (获取 %d 条记录)\n", len(records))
		}
		fmt.Printf("✅ 股票 %s 处理完成\n\n", symbol)
	}

	fmt.Printf("🎉 所有数据已保存到PostgreSQL！\n")
	fmt.Printf("总共处理了 %d 个任务\n", totalTasks)
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
