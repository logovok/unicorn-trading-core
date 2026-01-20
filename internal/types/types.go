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
	DiffThreshold               float64 `json:"diff_threshold"`
	CrossExchangePriceThreshold float64 `json:"cept"`
}

type Coin struct {
	Symbol string `json:"coin"`
	// leading exchange price / Multiplier = current exchange price
	Multiplier float64       `default:"1"`
	Commission float64       `default:"0"`
	Window     time.Duration `json:"window"`
	Data       *CoinData     `json:"coin_data"`
	Thresholds Thresholds    `json:"thresholds"`
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
	Coin          *Coin
	IsUpDirect    bool               `json:"is_up_direct"`
	DealTimeout   time.Duration      `json:"deal_timeout"`
	DealFoundTime time.Time          `json:"deal_found_time"`
	Strategy      StrategyMonitoring `json:"strategy"`
	Volume        float64            `json:"volume"`
	PriceStart    float64            `json:"price_start"`
	PriceClose    float64            `json:"price_close"`
	PriceAbort    float64            `json:"price_abort"`
}

type StrategyMonitoring struct {
	Name                   string                 `json:"name"`
	SlowExchangeIndicators map[string]interface{} `json:"slow_exchange_indicators"`
	FastExchangeIndicators map[string]interface{} `json:"fast_exchange_indicators"`
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
