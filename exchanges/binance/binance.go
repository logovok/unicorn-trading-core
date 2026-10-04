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
		Path:   "/ws/" + c.Symbol + "@aggTrade",
	}

	var trades []types.Trade
	var sumPV, sumV float64
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

			trades = append(trades, types.Trade{
				Time:   t,
				Price:  price,
				Volume: qty,
			})
			sumPV += price * qty
			sumV += qty

			cutoff := time.Now().Add(-c.Window)
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
