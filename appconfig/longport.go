package appconfig

import (
	"os"

	"gopkg.in/yaml.v3"
)

// AccountConfig 表示单个账号的配置（单账号模式）
type AccountConfig struct {
	Name        string
	AppKey      string
	AppSecret   string
	AccessToken string
	Region      string
	Threads     int
	RPS         int
	TimeoutMS   int
}

// longportYAML 用于解析 longport.yaml 文件（仅支持单账号）
type longportYAML struct {
	Longport struct {
		AppKey      string `yaml:"app_key"`
		AppSecret   string `yaml:"app_secret"`
		AccessToken string `yaml:"access_token"`
		Region      string `yaml:"region"`
		Threads     int    `yaml:"threads"`
		RPS         int    `yaml:"rps"`
		TimeoutMS   int    `yaml:"timeout_ms"`
	} `yaml:"longport"`
}

// LoadLongportAccount 读取 longport.yaml 并返回单账号配置
func LoadLongportAccount(path string) (AccountConfig, error) {
	var out AccountConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return out, err
	}
	var cfg longportYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return out, err
	}

	threads := cfg.Longport.Threads
	if threads <= 0 {
		threads = 5
	}
	rps := cfg.Longport.RPS
	if rps <= 0 {
		rps = 10
	}
	timeout := cfg.Longport.TimeoutMS
	if timeout <= 0 {
		timeout = 10000
	}

	out = AccountConfig{
		Name:        "default",
		AppKey:      cfg.Longport.AppKey,
		AppSecret:   cfg.Longport.AppSecret,
		AccessToken: cfg.Longport.AccessToken,
		Region:      cfg.Longport.Region,
		Threads:     threads,
		RPS:         rps,
		TimeoutMS:   timeout,
	}
	return out, nil
}
