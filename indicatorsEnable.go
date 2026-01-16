package main

import (
	"fmt"
	"log"
	"trading/core/internal/distributors"
	"trading/core/internal/types"
)

func enableCoinAVG(exch types.ExchangeAvgMeanDiff, coin string) {
	ch := make(chan types.AvgMeanDiff)
	c, ok := exch.GetCoin(coin)
	if !ok {
		log.Panicf("Coin: %v doesn't exist for an Exchage: %v\n", coin, exch.GetName())
	}
	if c.Data.Avg == nil {
		c.Data.Avg = distributors.NewDataDistributor(ch)
		go exch.GetAvgMeanDiff(c, ch)
		go c.Data.Avg.Run()
	} else {
		fmt.Printf("Coin AVG already enabled for %v on Exchange: %v\n", coin, exch.GetName())
	}
}

func enableCoinPrice(exch types.ExchangePrice, coin string) {
	ch := make(chan types.PriceTime)
	c, ok := exch.GetCoin(coin)
	if !ok {
		log.Panicf("Coin: %v doesn't exist for an Exchage: %v\n", coin, exch.GetName())
	}
	if c.Data.Price == nil {
		c.Data.Price = distributors.NewDataDistributor(ch)
		go exch.GetPrice(c, ch)
		go c.Data.Price.Run()
	} else {
		fmt.Printf("Coin price already enabled for %v on Exchange: %v\n", coin, exch.GetName())
	}
}
