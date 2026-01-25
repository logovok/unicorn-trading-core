package main

import (
	"log"
	"math"
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
		Thresholds: types.Thresholds{
			DiffThresholdPercent: 5.65 * math.Pow10(-5),
		},
	}

	binance.Coins["shiba"] = &types.Coin{
		Symbol:     "1000shibusdt",
		Multiplier: 0.001,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			DiffThresholdPercent: 5.65 * math.Pow10(-5),
		},
	}

	mex.Coins["btcusdt"] = &types.Coin{
		Symbol:     "BTC_USDT",
		Multiplier: 1,
		Commission: 0.02,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			CrossExchangePriceThresholdPercent: 1.15 * math.Pow10(-5),
		},
	}

	mex.Coins["shiba"] = &types.Coin{
		Symbol:     "SHIB_USDT",
		Multiplier: 1,
		Commission: 0.02,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			CrossExchangePriceThresholdPercent: 1.15 * math.Pow10(-5),
		},
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
			"untuned": {
				Name:             "untuned",
				CrossExchangeLag: time.Millisecond * 400,
				DealTimeout:      time.Millisecond * 15000,
				OrderVolume:      10,
			},
		},
	}

	strat1 := strategies.PriceMeanDiff{
		Name: "Random params",
		TS:   pmd.ThreasholdSets["untuned"],
	}
	go strat1.Strategize(&binance, &mex, "btcusdt", orderDistributor.Ch)
	go strat1.Strategize(&binance, &mex, "shiba", orderDistributor.Ch)

	enableCoinPrice(&mex, "shiba")
	enableCoinPrice(&mex, "btcusdt")
	enableCoinAVG(&binance, "shiba")
	enableCoinAVG(&binance, "btcusdt")

	log.Println("Bot started successfully")

	for {
		time.Sleep(1 * time.Second)
	}
}
