package mexc

import (
	"encoding/json"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/logovok/unicorn-trading-core/internal/types"
)

const (
	minBackoff   = time.Second
	maxBackoff   = 30 * time.Second
	pingInterval = 15 * time.Second
	// Kline pushes and pong replies arrive every few seconds; silence this long means a dead connection.
	readTimeout = 30 * time.Second
)

func (exch *Mex) GetPrice(c *types.Coin, price chan<- types.PriceTime) {
	u := url.URL{
		Scheme: "wss",
		Host:   exch.BaseWS,
		Path:   "edge",
	}

	sub := map[string]any{
		"method": "sub.kline",
		"param": map[string]string{
			"symbol":   c.Symbol,
			"interval": "Min1",
		},
		"gzip": false,
	}

	backoff := minBackoff

	// Reconnect forever, re-subscribing on every new connection.
	for {
		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Printf("MEXC %v: dial error: %v, retrying in %v", c.Symbol, err, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("MEXC %v: subscribe error: %v, reconnecting in %v", c.Symbol, err, backoff)
		} else {
			done := make(chan struct{})
			go keepAlive(conn, done)

			for {
				conn.SetReadDeadline(time.Now().Add(readTimeout))
				_, msg, err := conn.ReadMessage()
				if err != nil {
					log.Printf("MEXC %v: read error: %v, reconnecting in %v", c.Symbol, err, backoff)
					break
				}
				backoff = minBackoff

				var m MexcMsg
				if err := json.Unmarshal(msg, &m); err != nil {
					continue
				}

				if m.Channel == "push.kline" {
					res := types.PriceTime{
						Price: m.Data.Close,
						Time:  time.Now(),
					}
					price <- res
				}
			}
			close(done)
		}

		conn.Close()
		time.Sleep(backoff)
		backoff = min(backoff*2, maxBackoff)
	}
}

// keepAlive pings the server until done is closed, as MEXC drops idle connections.
func keepAlive(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
				return
			}
		}
	}
}
