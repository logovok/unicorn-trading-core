package config

import (
	"os"
	"sync"
)

type Config struct {
	ClickHouse ClickHouseConfig
}

type ClickHouseConfig struct {
	Url          string
	UserName     string
	UserPassword string
	DB           string
}

var (
	cfg  *Config
	once sync.Once
)

func Get() *Config {
	once.Do(func() {
		cfg = load()
	})
	return cfg
}

func load() *Config {
	cfg := &Config{
		ClickHouse: ClickHouseConfig{
			Url:          getEnv("CLICKHOUSE_URL", "localhost:9000"),
			UserName:     getEnv("CLICKHOUSE_USERNAME", "default"),
			UserPassword: getEnv("CLICKHOUSE_PASSWORD", ""),
			DB:           getEnv("DB_NAME", "default"),
		},
	}

	return cfg
}

func getEnv(key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultVal
}
