package main

import "time"

type Exchange struct {
	name   string
	baseWS string
	coins  map[string]*Coin
}

type Coin struct {
	symbol     string
	multiplier int64 `default:"1"`
	window     time.Duration
	data       *CoinData
}

type CoinData struct {
	price Distributor[PriceTime]
	avg   Distributor[AvgMeanDiff]
}

type AggTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Qty       string `json:"q"`
	TradeTime int64  `json:"T"`
}

type Trade struct {
	Time   time.Time
	Price  float64
	Volume float64
}

type AvgMeanDiff struct {
	Avg  float64   `json:"avg"`
	Diff float64   `json:"diff"`
	Prc  float64   `json:"prc"`
	Time time.Time `json:"time"`
}

type PriceTime struct {
	Price float64   `json:"price"`
	Time  time.Time `json:"time"`
}

type MexcMsg struct {
	Channel string `json:"channel"`
	Data    struct {
		Close float64 `json:"c"`
	} `json:"data"`
}

type ExchangeBase interface {
	getCoin(coin string) (*Coin, bool)
	getName() string
}

type ExchangeAvgMeanDiff interface {
	ExchangeBase
	getAvgMeanDiff(c *Coin, aggr chan<- AvgMeanDiff)
}

type ExchangePrice interface {
	ExchangeBase
	getPrice(c *Coin, price chan<- PriceTime)
}
