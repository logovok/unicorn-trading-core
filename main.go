package main

import (
	"log"
	_ "reflect"
	"time"
	"trading/core/exchanges/binance"
	"trading/core/exchanges/mexc"
	"trading/core/internal/orders"
	"trading/core/internal/strategies"
	"trading/core/internal/types"
)

func main() {
	binance := binance.Binance{
		Exchange: types.Exchange{
			Name:   "Binance",
			BaseWS: "fstream.binance.com",
			Coins:  map[string]*types.Coin{},
		},
	}
	mex := mexc.Mex{
		Exchange: types.Exchange{
			Name:   "Mex",
			BaseWS: "contract.mexc.com",
			Coins:  map[string]*types.Coin{},
		},
	}

	binance.Coins["btcusdt"] = &types.Coin{
		Symbol: "btcusdt",
		Window: (60 * time.Second),
		Data:   &types.CoinData{},
		Thresholds: types.Thresholds{
			DiffThreshold: 1,
		},
	}
	enableCoinAVG(&binance, "btcusdt")

	binance.Coins["shiba"] = &types.Coin{
		Symbol: "1000shibusdt",
		Window: (60 * time.Second),
		Data:   &types.CoinData{},
		Thresholds: types.Thresholds{
			DiffThreshold: 0.000001,
		},
	}
	enableCoinAVG(&binance, "shiba")

	mex.Coins["btcusdt"] = &types.Coin{
		Symbol: "BTC_USDT",
		Window: (60 * time.Second),
		Data:   &types.CoinData{},
		Thresholds: types.Thresholds{
			CrossExchangePriceThreshold: 1,
		},
	}
	enableCoinPrice(&mex, "btcusdt")

	mex.Coins["shiba"] = &types.Coin{
		Symbol:     "SHIB_USDT",
		Multiplier: 1000,
		Window:     (60 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			CrossExchangePriceThreshold: 0.000001,
		},
	}
	enableCoinPrice(&mex, "shiba")

	orderDistributor := orders.OrderDistributor{Ch: make(chan types.Order)}
	go orderDistributor.Run()

	go strategies.StrategyPriceMeanDiffDirection(&binance, &mex, "btcusdt", orderDistributor.Ch)
	go strategies.StrategyPriceMeanDiffDirection(&binance, &mex, "shiba", orderDistributor.Ch)

	log.Println("Bot started successfully")

	for {
		time.Sleep(1 * time.Second)
	}
}
