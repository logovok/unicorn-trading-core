package orders

import (
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
		accountWorker := AccountWorker{buyVolume: buyVolume, order: order}
		go accountWorker.ProcessOrder()
	}

}

type AccountWorker struct {
	buyVolume float64
	order     types.Order
}

func (aw *AccountWorker) ProcessOrder() {
	var ch = aw.order.Coin.Data.Price.Subscribe()
	timeout := time.Now().Add(aw.order.DealTimeout)
	for value := range ch {
		if time.Now().After(timeout) {
			//close order
			aw.storeMetrics(value, true)
			break
		}
		if aw.order.IsUpDirect {
			if value.Price >= aw.order.PriceClose {
				//close order
				aw.storeMetrics(value, true)
				break
			}
			if value.Price < aw.order.PriceAbort {
				//close order
				aw.storeMetrics(value, false)
				break
			}
		} else {
			if value.Price <= aw.order.PriceClose {
				//close order
				aw.storeMetrics(value, true)
				break
			}
			if value.Price > aw.order.PriceAbort {
				//close order
				aw.storeMetrics(value, false)
				break
			}
		}
	}
}

func (aw *AccountWorker) storeMetrics(value types.PriceTime, isOK bool) {
	//earned := math.Abs(value.Price - aw.order.priceStart)
	earned := math.Abs(value.Price/aw.order.PriceStart-1) * aw.buyVolume
	if !isOK {
		earned = earned * -1
	}
	log.Printf("COIN: %v EARNED: %v", aw.order.Coin.Symbol, earned)

}
