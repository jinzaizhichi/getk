package appconfig

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 应用配置结构
type Config struct {
	Symbols  []string `yaml:"symbols"`
	Dates    []string `yaml:"dates"`
	Settings struct {
		Period     string `yaml:"period"`
		AdjustType string `yaml:"adjust_type"`
	} `yaml:"settings"`
}

// Load 从YAML文件加载配置
func Load(filepath string) (*Config, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}

// ParseDates 将字符串日期转换为time.Time切片
func (c *Config) ParseDates() ([]time.Time, error) {
	var dates []time.Time
	for _, dateStr := range c.Dates {
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return nil, fmt.Errorf("解析日期 %s 失败: %v", dateStr, err)
		}
		dates = append(dates, date)
	}
	return dates, nil
}