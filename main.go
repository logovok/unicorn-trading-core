package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	_ "reflect"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type Exchange struct {
	name   string
	baseWS string
	coins  map[string]Coin
}

func (exch Exchange) getCoin(coin string) (Coin, bool) {
	res, ok := exch.coins[coin]
	return res, ok
}

func (exch Exchange) getName() string {
	return exch.name
}

type Coin struct {
	symbol     string
	multiplier int64 `default:"1"`
	window     time.Duration
}

func (c Coin) getPrice(basePrice float64) float64 {
	return basePrice * float64(c.multiplier)
}

// ─────────────── STRUCTS ───────────────

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

type MexcMsg struct {
	Channel string `json:"channel"`
	Data    struct {
		Close float64 `json:"c"`
	} `json:"data"`
}

// ─────────────── MAIN ───────────────

func main() {
	binance := Exchange{
		name:   "Binance",
		baseWS: "fstream.binance.com",
		coins:  map[string]Coin{},
	}
	mex := Exchange{
		name:   "Mex",
		baseWS: "contract.mexc.com",
		coins:  map[string]Coin{},
	}
	binance.coins["btcusdt"] = Coin{symbol: "btcusdt", window: (60 * time.Second)}
	binance.coins["shiba"] = Coin{symbol: "1000shibusdt", window: (60 * time.Second)}
	mex.coins["btcusdt"] = Coin{symbol: "BTC_USDT", window: (60 * time.Second)}
	mex.coins["shiba"] = Coin{symbol: "SHIB_USDT", multiplier: 1000, window: (60 * time.Second)}

	go strategyPriceMeanDiffDirection(&binance, &mex, "btcusdt")
	go strategyPriceMeanDiffDirection(&binance, &mex, "shiba")
	for {
		time.Sleep(1 * time.Second)
	}
}

type ExchangeBase interface {
	getCoin(coin string) (Coin, bool)
	getName() string
}

type ExchangeAvgMeanDiff interface {
	ExchangeBase
	getAvgMeanDiff(c Coin, aggr chan<- AvgMeanDiff)
}

type ExchangePrice interface {
	ExchangeBase
	getPrice(c Coin, price chan<- PriceTime)
}

func strategyPriceMeanDiffDirection(leadExchange ExchangeAvgMeanDiff, slowExchange ExchangePrice, coinName string) {
	leadExchCoin, ok := leadExchange.getCoin(coinName)
	if !ok {
		fmt.Println("Lead exchange doesn't has required coin")
		return
	}

	slowExchCoin, ok := slowExchange.getCoin(coinName)
	if !ok {
		fmt.Println("Slow exchange doesn't has required coin")
		return
	}

	leadAvgMeanDiffChannel := make(chan AvgMeanDiff)
	leadPriceChannel := make(chan PriceTime)
	go leadExchange.getAvgMeanDiff(leadExchCoin, leadAvgMeanDiffChannel)
	go slowExchange.getPrice(slowExchCoin, leadPriceChannel)

	AMD := AvgMeanDiff{}
	PT := PriceTime{}
	for {
		select {
		case res := <-leadAvgMeanDiffChannel:
			AMD = res
		case res := <-leadPriceChannel:
			PT = res
		}
		if (AMD != AvgMeanDiff{} && PT != PriceTime{}) {
			fmt.Println("=============================")
			fmt.Printf("Strategy 1, coin %v\n", coinName)
			fmt.Printf("Price %v - %v %v\n", leadExchange.getName(), slowExchange.getName(), leadExchCoin.getPrice(AMD.Prc)-slowExchCoin.getPrice(PT.Price))
			fmt.Printf("%v diff %v\n", leadExchange.getName(), AMD.Diff)
		}
	}
}

func (exch *Exchange) getAvgMeanDiff(c Coin, aggr chan<- AvgMeanDiff) {
	u := url.URL{
		Scheme: "wss",
		Host:   exch.baseWS,
		Path:   "/ws/" + c.symbol + "@aggTrade",
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("WS error:", err)
	}
	defer conn.Close()

	// fmt.Printf("Connected to %v aggTrade: %v\n", exch.name, c.symbol)

	var trades []Trade
	var sumPV, sumV float64

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal("Read error:", err)
		}

		var agg AggTrade
		if err := json.Unmarshal(msg, &agg); err != nil {
			continue
		}

		price, _ := strconv.ParseFloat(agg.Price, 64)
		qty, _ := strconv.ParseFloat(agg.Qty, 64)
		t := time.UnixMilli(agg.TradeTime)

		trades = append(trades, Trade{
			Time:   t,
			Price:  price,
			Volume: qty,
		})
		sumPV += price * qty
		sumV += qty

		cutoff := time.Now().Add(-c.window)
		i := 0
		for i < len(trades) && !trades[i].Time.After(cutoff) {
			sumPV -= trades[i].Price * trades[i].Volume
			sumV -= trades[i].Volume
			i++
		}
		if i > 0 {
			trades = trades[i:]
		}

		if sumV == 0 {
			continue
		}

		vwap := sumPV / sumV
		dif := vwap - price
		aggregation := AvgMeanDiff{
			Avg:  vwap,
			Diff: dif,
			Prc:  price,
			Time: time.Now(),
		}

		aggr <- aggregation
	}
}

func (exch *Exchange) getPrice(c Coin, price chan<- PriceTime) {
	u := url.URL{
		Scheme: "wss",
		Host:   exch.baseWS,
		Path:   "edge",
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("MEXC WS error:", err)
	}
	defer conn.Close()

	// Subscribe
	sub := map[string]any{
		"method": "sub.kline",
		"param": map[string]string{
			"symbol":   c.symbol,
			"interval": "Min1",
		},
		"gzip": false,
	}
	conn.WriteJSON(sub)

	// Ping loop
	go func() {
		for {
			time.Sleep(15 * time.Second)
			conn.WriteJSON(map[string]string{"method": "ping"})
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var m MexcMsg
		if err := json.Unmarshal(msg, &m); err != nil {
			continue
		}

		if m.Channel == "push.kline" {
			res := PriceTime{
				Price: m.Data.Close,
				Time:  time.Now(),
			}
			price <- res
		}
	}
}
