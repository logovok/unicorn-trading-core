package main

import (
	"log"
	_ "reflect"
	"time"
	"trading/core/exchanges/binance"
	"trading/core/exchanges/mexc"
	"trading/core/internal/orders"
	"trading/core/internal/storage"
	"trading/core/internal/strategies"
	"trading/core/internal/types"
)

func main() {
	db := storage.ClickHouse{}
	err := db.InitClickHouse()
	if err != nil {
		log.Println("Failed to start ClickHouse")
	}

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
		Symbol:     "btcusdt",
		Multiplier: 1,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
	}

	binance.Coins["shiba"] = &types.Coin{
		Symbol:     "1000shibusdt",
		Multiplier: 0.001,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
	}

	mex.Coins["btcusdt"] = &types.Coin{
		Symbol:     "BTC_USDT",
		Multiplier: 1,
		Commission: 0.0002,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
	}

	mex.Coins["shiba"] = &types.Coin{
		Symbol:     "SHIB_USDT",
		Multiplier: 1,
		Commission: 0.0002,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
	}

	aw1 := orders.AccountWorker{
		AccountID:   "worker1",
		IsLocked:    false,
		LockStream:  make(chan bool),
		OrderStream: make(chan orders.AccountOrder),
		Db:          &db,
	}
	go aw1.Run()
	aw2 := orders.AccountWorker{
		AccountID:   "worker2",
		IsLocked:    false,
		LockStream:  make(chan bool),
		OrderStream: make(chan orders.AccountOrder),
		Db:          &db,
	}
	go aw2.Run()

	orderDistributor := orders.OrderDistributor{
		Ch:             make(chan types.Order),
		AccountWorkers: []*orders.AccountWorker{&aw1, &aw2},
	}
	go orderDistributor.Run()

	pmd := strategies.PriceMeanDiffGlobals{
		ThreasholdSets: map[string]*strategies.PMDThreasholdSet{
			"profitable-2": {
				Name:                               "profitable-2",
				CrossExchangeLag:                   time.Millisecond * 400,
				DealTimeout:                        time.Millisecond * 15000,
				OrderVolume:                        10,
				DiffThresholdPercent:               0.0002,
				CrossExchangePriceThresholdPercent: 0.0002,
			},
			"profitable-4": {
				Name:                               "profitable-4",
				CrossExchangeLag:                   time.Millisecond * 400,
				DealTimeout:                        time.Millisecond * 15000,
				OrderVolume:                        10,
				DiffThresholdPercent:               0.0004,
				CrossExchangePriceThresholdPercent: 0.0004,
			},
			"profitable-8": {
				Name:                               "profitable-8",
				CrossExchangeLag:                   time.Millisecond * 400,
				DealTimeout:                        time.Millisecond * 15000,
				OrderVolume:                        10,
				DiffThresholdPercent:               0.0008,
				CrossExchangePriceThresholdPercent: 0.0008,
			},
			"profitable-16": {
				Name:                               "profitable-16",
				CrossExchangeLag:                   time.Millisecond * 400,
				DealTimeout:                        time.Millisecond * 15000,
				OrderVolume:                        10,
				DiffThresholdPercent:               0.0016,
				CrossExchangePriceThresholdPercent: 0.0016,
			},
			"profitable-32": {
				Name:                               "profitable-32",
				CrossExchangeLag:                   time.Millisecond * 400,
				DealTimeout:                        time.Millisecond * 15000,
				OrderVolume:                        10,
				DiffThresholdPercent:               0.0032,
				CrossExchangePriceThresholdPercent: 0.0032,
			},
		},
	}

	for _, TS := range pmd.ThreasholdSets {
		strat := strategies.PriceMeanDiff{
			Name: "Price mean diff",
			TS:   TS,
		}
		go strat.Strategize(&binance, &mex, "btcusdt", orderDistributor.Ch)
		go strat.Strategize(&binance, &mex, "shiba", orderDistributor.Ch)
	}

	enableCoinPrice(&mex, "shiba")
	enableCoinPrice(&mex, "btcusdt")
	enableCoinAVG(&binance, "shiba")
	enableCoinAVG(&binance, "btcusdt")

	log.Println("Bot started successfully")

	for {
		time.Sleep(1 * time.Second)
	}
}
