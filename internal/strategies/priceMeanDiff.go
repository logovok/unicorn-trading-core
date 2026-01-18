package strategies

import (
	"fmt"
	"log"
	"math"
	"time"

	"trading/core/internal/types"
)

func StrategyPriceMeanDiffDirection(leadExchange types.ExchangeAvgMeanDiff, slowExchange types.ExchangePrice, coinName string, orderChan chan<- types.Order) {
	leadExchCoin, ok := leadExchange.GetCoin(coinName)
	if !ok {
		fmt.Println("Lead exchange doesn't has required coin")
		return
	}

	slowExchCoin, ok := slowExchange.GetCoin(coinName)
	if !ok {
		fmt.Println("Slow exchange doesn't has required coin")
		return
	}

	if leadExchCoin.Data.Avg == nil || slowExchCoin.Data.Price == nil {
		time.Sleep(time.Second)
		if leadExchCoin.Data.Avg == nil || slowExchCoin.Data.Price == nil {
			log.Panicln("Coin data distribution stream not initialized")
		}
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

		go func() {
			if AMD.Time.Sub(PT.Time).Abs() > time.Millisecond*400 {
				return
			} else {
				log.Printf("Time diff: %v", AMD.Time.Sub(PT.Time).Abs())
				log.Printf("AMD time diff: %v", time.Since(AMD.Time))
				log.Printf("PT time diff: %v", time.Since(PT.Time))
			}

			if (AMD != types.AvgMeanDiff{} && PT != types.PriceTime{}) {
				isUpDirect := AMD.Diff < 0
				isDiffOK := math.Abs(AMD.Diff) >= leadExchCoin.Thresholds.DiffThreshold
				if !isDiffOK {
					return
				}

				diffExchanges := math.Abs(leadExchCoin.GetPrice(AMD.Prc) - slowExchCoin.GetPrice(PT.Price))
				isCrossExchangePriceOK := diffExchanges >= slowExchCoin.GetPrice(slowExchCoin.Thresholds.CrossExchangePriceThreshold)
				if !isCrossExchangePriceOK {
					return
				}

				dealTimeout := types.AppConfig.OrderTimeout
				volume := types.AppConfig.OrderVolume
				priceStart := PT.Price
				priceClose := PT.Price - AMD.Diff/slowExchCoin.Multiplier
				priceAbort := PT.Price

				order := types.Order{
					Coin: slowExchCoin,
					Strategy: types.StrategyMonitoring{
						Name: "AvgMeanDiff",
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
				orderChan <- order
			}
		}()
	}
}
