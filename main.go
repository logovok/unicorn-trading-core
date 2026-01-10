package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsURL        = "wss://contract.mexc.com/edge"
	symbol       = "BTC_USDT"
	window       = 60 * time.Second
	pingInterval = 15 * time.Second
)

// ─────────────── STRUCTS ───────────────

type SubscribeDeal struct {
	Method string `json:"method"`
	Param  struct {
		Symbol string `json:"symbol"`
	} `json:"param"`
}

type PingMessage struct {
	Method string `json:"method"`
}

type DealMessage struct {
	Channel string `json:"channel"`
	Data    []struct {
		Price  float64 `json:"p"`
		Volume float64 `json:"v"`
		TimeMS int64   `json:"t"`
	} `json:"data"`
}

type Trade struct {
	Time   time.Time
	Price  float64
	Volume float64
}

// ─────────────── MAIN ───────────────

func main() {
	u, _ := url.Parse(wsURL)

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("WS error:", err)
	}
	defer conn.Close()

	// Subscribe to deals
	sub := SubscribeDeal{Method: "sub.deal"}
	sub.Param.Symbol = symbol

	if err := conn.WriteJSON(sub); err != nil {
		log.Fatal("Subscribe error:", err)
	}

	fmt.Println("Subscribed to deal stream:", symbol)

	// Ping loop
	go pingLoop(conn)

	var trades []Trade

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal("Read error:", err)
		}

		var base map[string]interface{}
		if err := json.Unmarshal(msg, &base); err != nil {
			continue
		}

		if base["channel"] == "pong" {
			continue
		}

		if base["channel"] != "push.deal" {
			continue
		}

		var deal DealMessage
		if err := json.Unmarshal(msg, &deal); err != nil {
			continue
		}

		now := time.Now()
		cutoff := now.Add(-window)

		// Add new trades
		for _, d := range deal.Data {
			trades = append(trades, Trade{
				Time:   time.UnixMilli(d.TimeMS),
				Price:  d.Price,
				Volume: d.Volume,
			})
		}

		// Remove expired trades
		filtered := trades[:0]
		for _, t := range trades {
			if t.Time.After(cutoff) {
				filtered = append(filtered, t)
			}
		}
		trades = filtered

		// Calculate VWAP
		var sumPV, sumV float64
		for _, t := range trades {
			sumPV += t.Price * t.Volume
			sumV += t.Volume
		}

		if sumV == 0 {
			continue
		}

		vwap := sumPV / sumV

		fmt.Printf(
			"%s | Trades: %d | VWAP(1m): %.4f\n",
			now.Format("15:04:05.000"),
			len(trades),
			vwap,
		)
	}
}

// ─────────────── PING ───────────────

func pingLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := conn.WriteJSON(PingMessage{Method: "ping"}); err != nil {
			log.Println("Ping failed:", err)
			return
		}
	}
}
