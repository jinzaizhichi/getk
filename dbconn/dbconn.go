package dbconn

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

// Config 表示数据库连接配置
// 该配置与 ./db.yaml 中的结构一致
//
// db:
//   host: "127.0.0.1"
//   port: 5432
//   user: "postgres"
//   password: "..."
//   name: "..."
//   sslmode: "disable"
//
// 注意：此包仅负责连接与DSN构建，不做表创建逻辑

type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	SSLMode  string `yaml:"sslmode"`
}

type appConfig struct {
	DB Config `yaml:"db"`
}

// Load 从指定 YAML 文件加载数据库配置
func Load(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	var ac appConfig
	if err := yaml.Unmarshal(b, &ac); err != nil {
		return cfg, err
	}
	return ac.DB, nil
}

// DSN 构建 Postgres 连接字符串
func DSN(cfg Config) string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%d sslmode=%s",
		cfg.User,
		cfg.Password,
		cfg.Name,
		cfg.Host,
		cfg.Port,
		cfg.SSLMode,
	)
}

// Open 根据配置建立数据库连接（不验证连接可用性）
func Open(cfg Config) (*sql.DB, error) {
	dsn := DSN(cfg)
	return sql.Open("postgres", dsn)
}

// OpenFromFile 便捷函数：从文件加载配置并建立连接
func OpenFromFile(path string) (*sql.DB, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	return Open(cfg)
}
