package storage

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/logovok/unicorn-trading-core/config"
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

	// Open is lazy; Ping makes sure the server is actually reachable.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
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
