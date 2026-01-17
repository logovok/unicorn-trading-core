package types

import (
	"time"
	"trading/core/internal/distributors"
)

var AppConfig = Config{
	OrderTimeout: time.Millisecond * 15000,
	OrderVolume:  10,
}

type Config struct {
	OrderTimeout time.Duration
	OrderVolume  float64
}

type Exchange struct {
	Name   string
	BaseWS string
	Coins  map[string]*Coin
}

type Thresholds struct {
	DiffThreshold               float64
	CrossExchangePriceThreshold float64
}

type Coin struct {
	Symbol string
	// leading exchange price / Multiplier = current exchange price
	Multiplier float64 `default:"1.0"`
	Window     time.Duration
	Data       *CoinData
	Thresholds Thresholds
}

type CoinData struct {
	Price distributors.Distributor[PriceTime]
	Avg   distributors.Distributor[AvgMeanDiff]
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

type Order struct {
	Coin        *Coin
	IsUpDirect  bool
	DealTimeout time.Duration
	Volume      float64
	PriceStart  float64
	PriceClose  float64
	PriceAbort  float64
}

type ExchangeBase interface {
	GetCoin(coin string) (*Coin, bool)
	GetName() string
}

type ExchangeAvgMeanDiff interface {
	ExchangeBase
	GetAvgMeanDiff(c *Coin, aggr chan<- AvgMeanDiff)
}

type ExchangePrice interface {
	ExchangeBase
	GetPrice(c *Coin, price chan<- PriceTime)
}
