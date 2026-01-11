package main

import (
	"log"
	"math"
	"time"
)

type OrderDistributor struct {
	ch chan Order
}

func (od *OrderDistributor) Run() {
	for order := range od.ch {
		buyVolume := order.volume
		accountWorker := AccountWorker{buyVolume: buyVolume, order: order}
		go accountWorker.ProcessOrder()
	}

}

type AccountWorker struct {
	buyVolume float64
	order     Order
}

func (aw *AccountWorker) ProcessOrder() {
	ch := aw.order.coin.data.price.Subscribe()
	timeout := time.Now().Add(aw.order.dealTimeout)
	for value := range ch {
		if time.Now().After(timeout) {
			//close order
			aw.storeMetrics(value, true)
			break
		}
		if aw.order.isUpDirect {
			if value.Price >= aw.order.priceClose {
				//close order
				aw.storeMetrics(value, true)
				break
			}
			if value.Price < aw.order.priceAbort {
				//close order
				aw.storeMetrics(value, false)
				break
			}
		} else {
			if value.Price <= aw.order.priceClose {
				//close order
				aw.storeMetrics(value, true)
				break
			}
			if value.Price > aw.order.priceAbort {
				//close order
				aw.storeMetrics(value, false)
				break
			}
		}
	}
}

func (aw *AccountWorker) storeMetrics(value PriceTime, isOK bool) {
	//earned := math.Abs(value.Price - aw.order.priceStart)
	earned := math.Abs(value.Price/aw.order.priceStart-1) * aw.buyVolume
	if !isOK {
		earned = earned * -1
	}
	log.Printf("COIN: %v EARNED: %v", aw.order.coin.symbol, earned)

}
