package mexc

import (
	"encoding/json"
	"log"
	"net/url"
	"time"
	"trading/core/internal/types"

	"github.com/gorilla/websocket"
)

func (exch *Mex) GetPrice(c *types.Coin, price chan<- types.PriceTime) {
	u := url.URL{
		Scheme: "wss",
		Host:   exch.BaseWS,
		Path:   "edge",
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("MEXC WS error:", err)
	}
	defer conn.Close()

	sub := map[string]any{
		"method": "sub.kline",
		"param": map[string]string{
			"symbol":   c.Symbol,
			"interval": "Min1",
		},
		"gzip": false,
	}
	conn.WriteJSON(sub)

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
			res := types.PriceTime{
				Price: m.Data.Close,
				Time:  time.Now(),
			}
			price <- res
		}
	}
}
