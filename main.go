package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type Exchange struct {
	baseWS string
	window time.Duration
	coins  []Coin
}

type Coin struct {
	symbol string
}

const (
	baseWS = "wss://fstream.binance.com"
	symbol = "btcusdt"
	window = 60 * time.Second
)

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

type Strat1Agg struct {
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
		baseWS: "fstream.binance.com",
		window: (60 * time.Second),
		coins:  []Coin{},
	}
	mex := Exchange{
		baseWS: "contract.mexc.com",
		window: (60 * time.Second),
		coins:  []Coin{},
	}
	binance.coins = append(binance.coins, Coin{symbol: "btcusdt"})
	mex.coins = append(mex.coins, Coin{symbol: "BTC_USDT"})

	btcustdChannel := make(chan Strat1Agg)
	mexBtcustdPrice := make(chan PriceTime)
	go binance.leadingExchangeAvgWSS(binance.coins[0], btcustdChannel)
	go mex.startMexcWS(mex.coins[0], mexBtcustdPrice)

	for {
		select {
		case res := <-btcustdChannel:
			fmt.Printf("Binance %v\n", res)
			fmt.Println(res)
		case res := <-mexBtcustdPrice:
			fmt.Printf("Mex %v\n", res)
		}
	}
}

func (exch *Exchange) leadingExchangeAvgWSS(c Coin, aggr chan<- Strat1Agg) {
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

	fmt.Println("Connected to Binance aggTrade:", symbol)

	var trades []Trade

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

		// Remove expired trades
		cutoff := time.Now().Add(-window)
		filtered := trades[:0]
		for _, tr := range trades {
			if tr.Time.After(cutoff) {
				filtered = append(filtered, tr)
			}
		}
		trades = filtered

		// Calculate VWAP
		var sumPV, sumV float64
		for _, tr := range trades {
			sumPV += tr.Price * tr.Volume
			sumV += tr.Volume
		}

		if sumV == 0 {
			continue
		}

		vwap := sumPV / sumV
		dif := vwap - price
		aggregation := Strat1Agg{
			Avg:  vwap,
			Diff: dif,
			Prc:  price,
			Time: time.Now(),
		}

		aggr <- aggregation
	}
}

func (exch *Exchange) startMexcWS(c Coin, price chan<- PriceTime) {
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
