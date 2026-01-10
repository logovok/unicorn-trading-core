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
	wsURL        = "wss://contract.mexc.com/edge"
	symbol       = "BTC_USDT"
	avgWindow    = 60 * time.Second
	pingInterval = 15 * time.Second
)

// ─────────────── DATA STRUCTS ───────────────

type PricePoint struct {
	Time  time.Time
	Price float64
}

type SubscribeMessage struct {
	Method string `json:"method"`
	Param  struct {
		Symbol string `json:"symbol"`
	} `json:"param"`
}

type PingMessage struct {
	Method string `json:"method"`
}

type TickerMessage struct {
	Channel string `json:"channel"`
	Data    struct {
		LastPrice float64 `json:"lastPrice"`
	} `json:"data"`
}

// ─────────────── MAIN ───────────────

func main() {
	u, _ := url.Parse(wsURL)

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("WS connection error:", err)
	}
	defer conn.Close()

	// Subscribe
	sub := SubscribeMessage{Method: "sub.ticker"}
	sub.Param.Symbol = symbol

	if err := conn.WriteJSON(sub); err != nil {
		log.Fatal("Subscription error:", err)
	}

	fmt.Println("Subscribed to", symbol)

	// Start ping loop
	go pingLoop(conn)

	var prices []PricePoint

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal("Read error:", err)
		}

		var base map[string]interface{}
		if err := json.Unmarshal(msg, &base); err != nil {
			continue
		}

		// Handle pong
		if base["channel"] == "pong" {
			continue
		}

		// Handle ticker
		if base["channel"] == "push.ticker" {
			var ticker TickerMessage
			if err := json.Unmarshal(msg, &ticker); err != nil {
				continue
			}

			price := ticker.Data.LastPrice
			now := time.Now()

			prices = append(prices, PricePoint{
				Time:  now,
				Price: price,
			})

			// Remove old prices
			cutoff := now.Add(-avgWindow)
			filtered := prices[:0]
			fmt.Printf("Before filter prices: %v\n", strconv.Itoa(len(prices)))

			for _, p := range prices {
				if p.Time.After(cutoff) {
					filtered = append(filtered, p)
				}
			}
			prices = filtered
			fmt.Println("After filter prices: " + strconv.Itoa(len(prices)))

			// Calculate average
			var sum float64
			for _, p := range prices {
				sum += p.Price
			}
			avg := sum / float64(len(prices))

			diff := avg - price

			fmt.Printf(
				"%s | Price: %.2f | Avg(1m): %.2f | Avg-Price: %.5f\n",
				now.Format("15:04:05"),
				price,
				avg,
				diff,
			)
		}
	}
}

// ─────────────── PING LOOP ───────────────

func pingLoop(conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		ping := PingMessage{Method: "ping"}
		if err := conn.WriteJSON(ping); err != nil {
			log.Println("Ping failed:", err)
			return
		}
	}
}
