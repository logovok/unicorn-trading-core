package binance

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"github.com/logovok/unicorn-trading-core/internal/types"
)

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
	// A healthy stream sends trades every few seconds; silence this long means a dead connection.
	readTimeout = 30 * time.Second
)

func (exch *Binance) GetAvgMeanDiff(c *types.Coin, aggr chan<- types.AvgMeanDiff) {
	u := url.URL{
		Scheme: "wss",
		Host:   exch.BaseWS,
		Path:   "/market/ws/" + c.Symbol + "@aggTrade",
	}

	window := vwapWindow{size: c.Window}
	backoff := minBackoff

	// Reconnect forever; the VWAP window is kept across reconnects.
	for {
		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Printf("Binance %v: dial error: %v, retrying in %v", c.Symbol, err, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		for {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Binance %v: read error: %v, reconnecting in %v", c.Symbol, err, backoff)
				break
			}
			backoff = minBackoff

			var agg types.AggTrade
			if err := json.Unmarshal(msg, &agg); err != nil {
				continue
			}

			price, _ := strconv.ParseFloat(agg.Price, 64)
			qty, _ := strconv.ParseFloat(agg.Qty, 64)
			t := time.UnixMilli(agg.TradeTime)

			vwap, ok := window.add(types.Trade{Time: t, Price: price, Volume: qty}, time.Now())
			if !ok {
				continue
			}

			dif := vwap - price
			aggregation := types.AvgMeanDiff{
				Avg:  vwap,
				Diff: dif,
				Prc:  price,
				Time: time.Now(),
			}

			aggr <- aggregation
		}

		conn.Close()
		time.Sleep(backoff)
		backoff = min(backoff*2, maxBackoff)
	}
}
