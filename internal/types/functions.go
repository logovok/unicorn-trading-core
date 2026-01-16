package types

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

func (exch Exchange) GetCoin(coin string) (*Coin, bool) {
	res, ok := exch.Coins[coin]
	return res, ok
}

func (exch *Binance) GetAvgMeanDiff(c *Coin, aggr chan<- AvgMeanDiff) {
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

	var trades []Trade
	var sumPV, sumV float64

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
		aggregation := AvgMeanDiff{
			Avg:  vwap,
			Diff: dif,
			Prc:  price,
			Time: time.Now(),
		}

		aggr <- aggregation
	}
}

func (exch *Mex) GetPrice(c *Coin, price chan<- PriceTime) {
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
			res := PriceTime{
				Price: m.Data.Close,
				Time:  time.Now(),
			}
			price <- res
		}
	}
}

func (exch Exchange) GetName() string {
	return exch.Name
}

func (c Coin) GetPrice(basePrice float64) float64 {
	return basePrice * float64(c.Multiplier)
}
