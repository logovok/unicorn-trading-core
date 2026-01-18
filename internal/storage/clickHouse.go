package storage

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var CH clickhouse.Conn

func InitClickHouse() error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"gustaf.mnknta.pp.ua:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	CH = conn
	return nil
}

func InsertAccountMetricsJSON(jsonPayload []byte) error {
	batch, err := CH.PrepareBatch(
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
