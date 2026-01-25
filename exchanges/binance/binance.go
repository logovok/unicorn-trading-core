package binance

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"
	"time"
	"trading/core/internal/types"

	"github.com/gorilla/websocket"
)

func (exch *Binance) GetAvgMeanDiff(c *types.Coin, aggr chan<- types.AvgMeanDiff) {
	u := url.URL{
		Scheme: "wss",
		Host:   exch.BaseWS,
		Path:   "/ws/" + c.Symbol + "@aggTrade",
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("WS error:", err)
	}
	defer conn.Close()

	var trades []types.Trade
	var sumPV, sumV float64

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal("Read error:", err)
		}

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
		dif := (vwap - price) / vwap
		aggregation := types.AvgMeanDiff{
			Avg:         vwap,
			DiffPercent: dif,
			Prc:         price,
			Time:        time.Now(),
		}

		aggr <- aggregation
	}
}
