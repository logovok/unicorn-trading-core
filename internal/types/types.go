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
	DiffThreshold               float64 `json:"diff_threashold"`
	CrossExchangePriceThreshold float64 `json:"cept"`
}

type Coin struct {
	Symbol string `json:"coin"`
	// leading exchange price / Multiplier = current exchange price
	Multiplier float64       `default:"1"`
	Window     time.Duration `json:"window"`
	Data       *CoinData     `json:"coin_data"`
	Thresholds Thresholds    `json:"threasholds"`
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

type AVG struct {
	TimeBoundSlidingWindow
	SumV  float64 `json:"sum_vol"`
	SumPV float64 `json:"sum_prc_vol"`
}

// TODO: Double-check that we will be getting a reference to the object that won't update, so it won't mes up our calculations
// Move calling recalc logic to Distributor or make another distributor type
// So, we call Subscribe(window time.Duration)
func (avg *AVG) Recalc(trades *[]Trade, CurrentLast time.Time, now time.Time) {
	cutoff := now.Add(-avg.Window)
	ln := len(*trades)

	for i := 0; i < ln && !((*trades)[i].Time.After(cutoff)); i++ {
		avg.SumPV -= (*trades)[i].Price * (*trades)[i].Volume
		avg.SumPV -= (*trades)[i].Volume
	}

	for i := len(*trades) - 1; i >= 0; i-- {
		if !(*trades)[i].Time.After(avg.TimeBoundSlidingWindow.CurrentFirst) {
			break
		}

		// Add newly added price points
		avg.SumPV += (*trades)[i].Price * (*trades)[i].Volume
		avg.SumV += (*trades)[i].Volume
	}

}

// TODO: add distributor logic, so that before sending data it waits for window to populate
// (if Last - First first are ~ Window)
type TimeBoundSlidingWindow struct {
	Window       time.Duration `json:"window"`
	CurrentFirst time.Time     `json:"current_first"`
	CurrentLast  time.Time     `json:"current_last"`
}

type PriceTime struct {
	Price float64   `json:"price"`
	Time  time.Time `json:"time"`
}

type Order struct {
	SlowExchangeCoin *Coin
	FastExchangeCoin *Coin
	IsUpDirect       bool               `json:"is_up_direct"`
	DealTimeout      time.Duration      `json:"deal_timeout"`
	DealFoundTime    time.Time          `json:"deal_found_time"`
	Strategy         StrategyMonitoring `json:"strategy"`
	Volume           float64            `json:"volume"`
	PriceStart       float64            `json:"price_start"`
	PriceClose       float64            `json:"price_close"`
	PriceAbort       float64            `json:"price_abort"`
}

type StrategyMonitoring struct {
	Strategy               Strategy               `json:"name"`
	SlowExchangeIndicators map[string]interface{} `json:"slow_exchange_indicators"`
	FastExchangeIndicators map[string]interface{} `json:"fast_exchange_indicators"`
}

type Strategy interface {
	GetName() string
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
