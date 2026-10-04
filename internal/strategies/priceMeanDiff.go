package strategies

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/logovok/unicorn-trading-core/internal/types"
)

func (pmd *PriceMeanDiff) Strategize(leadExchange types.ExchangeAvgMeanDiff, slowExchange types.ExchangePrice, coinName string, orderChan chan<- types.Order) {
	leadExchCoin, ok := leadExchange.GetCoin(coinName)
	if !ok {
		fmt.Println("Lead exchange doesn't have the required coin")
		return
	}

	slowExchCoin, ok := slowExchange.GetCoin(coinName)
	if !ok {
		fmt.Println("Slow exchange doesn't have the required coin")
		return
	}

	// Indicators must be enabled (see enableCoinAVG/enableCoinPrice) before the strategy starts.
	if leadExchCoin.Data.Avg == nil || slowExchCoin.Data.Price == nil {
		log.Panicf("Coin data streams not initialized for %v", coinName)
	}

	leadAvgMeanDiffChannel := leadExchCoin.Data.Avg.Subscribe()
	slowPriceChannel := slowExchCoin.Data.Price.Subscribe()

	AMD := types.AvgMeanDiff{}
	PT := types.PriceTime{}
	for {
		select {
		case res := <-leadAvgMeanDiffChannel:
			AMD = res
		case res := <-slowPriceChannel:
			PT = res
		}

		go func(amd types.AvgMeanDiff, pt types.PriceTime) {
			order := pmd.Process(amd, pt, *leadExchCoin, *slowExchCoin)
			if order != nil {
				orderChan <- *order
			}
		}(AMD, PT)
	}
}

func (pmd *PriceMeanDiff) Process(AMD types.AvgMeanDiff, PT types.PriceTime, leadExchCoin types.Coin, slowExchCoin types.Coin) *types.Order {
	if AMD.Time.Sub(PT.Time).Abs() > pmd.TS.CrossExchangeLag {
		return nil
	}

	if (AMD != types.AvgMeanDiff{} && PT != types.PriceTime{}) {
		isUpDirect := AMD.Diff < 0
		isDiffOK := math.Abs(AMD.Diff) >= leadExchCoin.Thresholds.DiffThreshold
		if !isDiffOK {
			return nil
		}

		diffExchanges := math.Abs(leadExchCoin.GetPrice(AMD.Prc) - slowExchCoin.GetPrice(PT.Price))
		isCrossExchangePriceOK := diffExchanges >= slowExchCoin.GetPrice(slowExchCoin.Thresholds.CrossExchangePriceThreshold)
		if !isCrossExchangePriceOK {
			return nil
		}

		dealTimeout := pmd.TS.DealTimeout
		volume := pmd.calcOrderVolume()
		priceStart := pmd.calcPriceStart(PT.Price)
		priceClose := pmd.calcPriceClose(PT.Price, (AMD.Diff / slowExchCoin.Multiplier * (-1)))
		priceAbort := pmd.calcPriceAbort(PT.Price)

		order := types.Order{
			SlowExchangeCoin: &slowExchCoin,
			FastExchangeCoin: &leadExchCoin,
			Strategy: types.StrategyMonitoring{
				Strategy: pmd,
				SlowExchangeIndicators: map[string]interface{}{
					"price": PT,
				},
				FastExchangeIndicators: map[string]interface{}{
					"amd": AMD,
				},
			},
			IsUpDirect:    isUpDirect,
			DealTimeout:   dealTimeout,
			DealFoundTime: time.Now(),
			Volume:        volume,
			PriceStart:    priceStart,
			PriceClose:    priceClose,
			PriceAbort:    priceAbort,
		}
		return &order
	}
	return nil
}
