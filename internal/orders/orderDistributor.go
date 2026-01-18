package orders

import (
	"encoding/json"
	"log"
	"math"
	"time"
	"trading/core/internal/storage"
	"trading/core/internal/types"
)

type OrderDistributor struct {
	Ch chan types.Order
}

func (od *OrderDistributor) Run() {
	for order := range od.Ch {
		buyVolume := order.Volume
		accountWorker := AccountWorker{
			AccountID: "TBD",
			AccountOrder: AccountOrder{
				BuyVolume: buyVolume,
				Order:     order,
			},
		}
		go accountWorker.ProcessOrder()
	}

}

type AccountWorker struct {
	AccountID    string `json:"account_id"`
	AccountOrder AccountOrder
}

type AccountOrder struct {
	types.Order   `json:"order"`
	BuyVolume     float64   `json:"buy_volume"`
	DealOpenTime  time.Time `json:"deal_open_time"`
	DealCloseTime time.Time `json:"deal_close_time"`
	Earned        float64   `json:"earned"`
}

func (aw *AccountWorker) ProcessOrder() {
	var ch = aw.AccountOrder.Coin.Data.Price.Subscribe()
	// Open AccountOrder in Exchange
	timeout := time.Now().Add(aw.AccountOrder.DealTimeout)
	aw.AccountOrder.DealOpenTime = time.Now()
	for value := range ch {
		if time.Now().After(timeout) {
			//close AccountOrder
			aw.storeMetrics(value, true)
			break
		}
		if aw.AccountOrder.IsUpDirect {
			if value.Price >= aw.AccountOrder.PriceClose {
				//close AccountOrder
				aw.storeMetrics(value, true)
				break
			}
			if value.Price < aw.AccountOrder.PriceAbort {
				//close AccountOrder
				aw.storeMetrics(value, false)
				break
			}
		} else {
			if value.Price <= aw.AccountOrder.PriceClose {
				//close AccountOrder
				aw.storeMetrics(value, true)
				break
			}
			if value.Price > aw.AccountOrder.PriceAbort {
				//close AccountOrder
				aw.storeMetrics(value, false)
				break
			}
		}
	}
}

func (aw *AccountWorker) storeMetrics(value types.PriceTime, isOK bool) {
	earned := math.Abs(value.Price/aw.AccountOrder.PriceStart-1) * aw.AccountOrder.BuyVolume
	if !isOK {
		earned = earned * -1
	}
	aw.AccountOrder.DealCloseTime = time.Now()
	aw.AccountOrder.Earned = earned

	go aw.insertMetrics()

	log.Printf("COIN: %v EARNED: %v", aw.AccountOrder.Coin.Symbol, earned)
}

func (aw *AccountWorker) insertMetrics() {
	outp, err := json.Marshal(aw)
	if err != nil {
		log.Println("json marshal error:", err)
		return
	}
	if err := storage.InsertAccountMetricsJSON(outp); err != nil {

		log.Println("Clickhouse insert error:", err)
	} else {
		log.Println("ClickHouse insert successful")
	}
}
