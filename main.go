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
	interval     = "Min1"
	window       = 60 * time.Second
	pingInterval = 15 * time.Second
)

// ─────────────── STRUCTS ───────────────

type PingMessage struct {
	Method string `json:"method"`
}

type KlineMessage struct {
	Channel string `json:"channel"`
	Data    struct {
		Close float64 `json:"c"`
		Time  int64   `json:"t"` // candle start (seconds)
	} `json:"data"`
}

type PricePoint struct {
	Time  time.Time
	Close float64
}

// ─────────────── MAIN ───────────────

func main() {
	u, _ := url.Parse(wsURL)
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("WS error:", err)
	}
	defer conn.Close()

	// Subscribe to K-line
	conn.WriteJSON(map[string]any{
		"method": "sub.kline",
		"param": map[string]string{
			"symbol":   symbol,
			"interval": interval,
		},
		"gzip": false,
	})

	fmt.Println("Subscribed to k-line:", symbol, interval)

	go pingLoop(conn)

	var prices []PricePoint

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal("Read error:", err)
		}

		var base map[string]any
		if json.Unmarshal(msg, &base) != nil {
			continue
		}

		if base["channel"] == "pong" {
			continue
		}

		if base["channel"] != "push.kline" {
			continue
		}

		var k KlineMessage
		if json.Unmarshal(msg, &k) != nil {
			continue
		}

		now := time.Now()
		prices = append(prices, PricePoint{
			Time:  now,
			Close: k.Data.Close,
		})

		// Remove expired values
		cutoff := now.Add(-window)
		filtered := prices[:0]
		for _, p := range prices {
			if p.Time.After(cutoff) {
				filtered = append(filtered, p)
			}
		}
		prices = filtered

		// Calculate average
		var sum float64
		for _, p := range prices {
			sum += p.Close
		}

		avg := sum / float64(len(prices))
		diff := avg - k.Data.Close

		fmt.Printf(
			"%s | Close: %.2f | Avg(1m): %.2f | Avg-C: %.5f | N=%d\n",
			now.Format("15:04:05.000"),
			k.Data.Close,
			avg,
			diff,
			len(prices),
		)
	}
}

// ─────────────── PING ───────────────

func pingLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		conn.WriteJSON(PingMessage{Method: "ping"})
	}
}
