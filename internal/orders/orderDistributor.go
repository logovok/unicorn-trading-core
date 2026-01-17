package orders

import (
	"encoding/json"
	"log"
	"math"
	"time"
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
	outp, _ := json.Marshal(aw)
	log.Println(string(outp))
	// log.Println(json.Marshal(aw.AccountOrder.Order.Coin))
	// log.Println(json.Marshal(aw.AccountOrder.Order))
	// log.Println(json.Marshal(aw.AccountOrder))
	// log.Println(json.Marshal(aw))
	log.Printf("COIN: %v EARNED: %v", aw.AccountOrder.Coin.Symbol, earned)
}
