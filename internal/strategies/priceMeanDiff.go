package strategies

import (
	"fmt"
	"log"
	"math"
	"time"
	"trading/core/internal/types"
)

func (pmd *PriceMeanDiff) Strategize(leadExchange types.ExchangeAvgMeanDiff, slowExchange types.ExchangePrice, coinName string, orderChan chan<- types.Order) {
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
			order := pmd.Process(AMD, PT, *leadExchCoin, *slowExchCoin)
			if order != nil {
				orderChan <- *order
			}
		}()
	}
}

func (pmd *PriceMeanDiff) Process(AMD types.AvgMeanDiff, PT types.PriceTime, leadExchCoin types.Coin, slowExchCoin types.Coin) *types.Order {
	if AMD.Time.Sub(PT.Time).Abs() > pmd.TS.CrossExchangeLag {
		return nil
	}

	if (AMD != types.AvgMeanDiff{} && PT != types.PriceTime{}) {
		isUpDirect := AMD.DiffPercent < 0
		isDiffOK := math.Abs(AMD.DiffPercent) >= pmd.TS.DiffThresholdPercent // TODO: Ensure I set the threshold
		if !isDiffOK {
			return nil
		}

		diffExchanges := math.Abs(leadExchCoin.GetPrice(AMD.Prc)-slowExchCoin.GetPrice(PT.Price)) / slowExchCoin.GetPrice(PT.Price)
		isCrossExchangePriceOK := diffExchanges >= pmd.TS.CrossExchangePriceThresholdPercent
		if !isCrossExchangePriceOK {
			return nil
		}

		dealTimeout := pmd.TS.DealTimeout
		volume := pmd.calcOrderVolume()
		priceStart := pmd.calcPriceStart(PT.Price)
		priceClose := pmd.calcPriceClose(PT.Price, AMD.DiffPercent)
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
