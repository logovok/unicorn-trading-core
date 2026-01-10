package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync/atomic"
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

/* ─────────────── MEXC STRUCT ─────────────── */

type MexcMsg struct {
	Channel string `json:"channel"`
	Data    struct {
		Close float64 `json:"c"`
	} `json:"data"`
}

// ─────────────── MAIN ───────────────

func main() {

	/* ───── MEXC PRICE HOLDER ───── */
	var mexcPrice atomic.Value
	mexcPrice.Store(float64(0))

	/* ───── START MEXC WS ───── */
	go startMexcWS(&mexcPrice)

	/* ───── BINANCE WS ───── */
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

		/* ───── READ MEXC PRICE ───── */
		mPrice := mexcPrice.Load().(float64)
		spread := price - mPrice

		fmt.Printf(
			"%s | BIN: %.2f | VWAP: %.2f | ΔVWAP: %+0.5f | MEXC: %.2f | Δ(BIN-MEXC): %+0.5f | Trades: %d\n",
			time.Now().Format("15:04:05.000"),
			price,
			vwap,
			diff,
			mPrice,
			spread,
			len(trades),
		)
	}
}

/* ─────────────── MEXC WS ─────────────── */

func startMexcWS(price *atomic.Value) {
	wsURL := "wss://contract.mexc.com/edge"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("MEXC WS error:", err)
	}
	defer conn.Close()

	// Subscribe
	sub := map[string]any{
		"method": "sub.kline",
		"param": map[string]string{
			"symbol":   "BTC_USDT",
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
			price.Store(m.Data.Close)
		}
	}
}
