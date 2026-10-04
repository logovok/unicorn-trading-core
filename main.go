package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/logovok/unicorn-trading-core/exchanges/binance"
	"github.com/logovok/unicorn-trading-core/exchanges/mexc"
	"github.com/logovok/unicorn-trading-core/internal/orders"
	"github.com/logovok/unicorn-trading-core/internal/storage"
	"github.com/logovok/unicorn-trading-core/internal/strategies"
	"github.com/logovok/unicorn-trading-core/internal/types"
)

func main() {
	db := storage.ClickHouse{}
	if err := db.InitClickHouse(); err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}

	binanceExch := binance.Binance{
		Exchange: types.Exchange{
			Name:   "Binance",
			BaseWS: "fstream.binance.com",
			Coins:  map[string]*types.Coin{},
		},
	}
	mexcExch := mexc.Mex{
		Exchange: types.Exchange{
			Name:   "Mex",
			BaseWS: "contract.mexc.com",
			Coins:  map[string]*types.Coin{},
		},
	}

	binanceExch.Coins["btcusdt"] = &types.Coin{
		Symbol:     "btcusdt",
		Multiplier: 1,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			DiffThreshold: 5,
		},
	}

	binanceExch.Coins["shiba"] = &types.Coin{
		Symbol:     "1000shibusdt",
		Multiplier: 0.001,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			DiffThreshold: 0.000000005,
		},
	}

	mexcExch.Coins["btcusdt"] = &types.Coin{
		Symbol:     "BTC_USDT",
		Multiplier: 1,
		Commission: 0.02,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			CrossExchangePriceThreshold: 1,
		},
	}

	mexcExch.Coins["shiba"] = &types.Coin{
		Symbol:     "SHIB_USDT",
		Multiplier: 1,
		Commission: 0.02,
		Window:     (15 * time.Second),
		Data:       &types.CoinData{},
		Thresholds: types.Thresholds{
			CrossExchangePriceThreshold: 0.000000000005,
		},
	}

	aw1 := orders.AccountWorker{
		AccountID:   "worker1",
		OrderStream: make(chan orders.AccountOrder),
		Db:          &db,
	}
	go aw1.Run()
	aw2 := orders.AccountWorker{
		AccountID:   "worker2",
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
		ThresholdSets: map[string]*strategies.PMDThresholdSet{
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
		TS:   pmd.ThresholdSets["untuned"],
	}
	enableCoinPrice(&mexcExch, "shiba")
	enableCoinPrice(&mexcExch, "btcusdt")
	enableCoinAVG(&binanceExch, "shiba")
	enableCoinAVG(&binanceExch, "btcusdt")

	go strat1.Strategize(&binanceExch, &mexcExch, "btcusdt", orderDistributor.Ch)
	go strat1.Strategize(&binanceExch, &mexcExch, "shiba", orderDistributor.Ch)

	log.Println("Bot started successfully")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down")
	if err := db.CHConn.Close(); err != nil {
		log.Println("ClickHouse close error:", err)
	}
}
