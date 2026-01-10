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

// ─────────────── MAIN ───────────────

func main() {
	u := url.URL{
		Scheme: "wss",
		Host:   "fstream.binance.com",
		Path:   "/ws/" + symbol + "@aggTrade",
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
		diff := vwap - price

		fmt.Printf(
			"%s | Price: %.2f | VWAP(1m): %.2f | Diff(VWAP-Price): %+0.5f | Trades: %d\n",
			time.Now().Format("15:04:05.000"),
			price,
			vwap,
			diff,
			len(trades),
		)
	}
}
