package config

import (
	"log"
	"os"
	"strconv"
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
			Url:          getEnv("CLICKHOUSE_URL", "localhost"),
			UserName:     getEnv("CLICKHOUSE_USERNAME", "postgres"),
			UserPassword: getEnv("CLICKHOUSE_PASSWORD", ""),
			DB:           getEnv("DB_NAME", "postgres"),
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

func getEnvAsInt(key string, defaultVal int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return defaultVal
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("Invalid integer value for %s: %s", key, v)
	}

	return i
}
