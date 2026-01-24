package storage

import (
	"context"
	"time"
	"trading/core/config"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouse struct {
	CHConn clickhouse.Conn
}

func (CH *ClickHouse) InitClickHouse() error {
	cfg := config.Get()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouse.Url},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouse.DB,
			Username: cfg.ClickHouse.UserName,
			Password: cfg.ClickHouse.UserPassword,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	CH.CHConn = conn
	return nil
}

func (CH *ClickHouse) InsertAccountMetricsJSON(jsonPayload []byte) error {
	batch, err := CH.CHConn.PrepareBatch(
		context.Background(),
		"INSERT INTO account_metrics (payload)",
	)
	if err != nil {
		return err
	}

	if err := batch.Append(string(jsonPayload)); err != nil {
		return err
	}

	return batch.Send()
}
