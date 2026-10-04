package orders

import (
	"encoding/json"
	"log"
	"math"
	"time"

	"github.com/logovok/unicorn-trading-core/internal/storage"
	"github.com/logovok/unicorn-trading-core/internal/types"
)

type OrderDistributor struct {
	Ch             chan types.Order
	AccountWorkers []*AccountWorker
}

func (od *OrderDistributor) Run() {
	for order := range od.Ch {
		buyVolume := order.Volume // It will be calculated
		ao := AccountOrder{BuyVolume: buyVolume, Order: order}
		// A send on the unbuffered OrderStream succeeds only if the worker is idle
		// and waiting; busy workers are skipped. If all are busy, the order is dropped.
	dispatch:
		for _, worker := range od.AccountWorkers {
			select {
			case worker.OrderStream <- ao:
				break dispatch
			default:
			}
		}
	}
}

func (aw *AccountWorker) Run() {
	for accountOrder := range aw.OrderStream {
		aw.AccountOrder = accountOrder
		aw.ProcessOrder()
	}
}

type AccountWorker struct {
	AccountID    string            `json:"account_id"`
	OrderStream  chan AccountOrder `json:"-"`
	AccountOrder AccountOrder
	Db           *storage.ClickHouse
}

type AccountOrder struct {
	types.Order      `json:"order"`
	BuyVolume        float64   `json:"buy_volume"`
	DealOpenTime     time.Time `json:"deal_open_time"`
	DealCloseTime    time.Time `json:"deal_close_time"`
	Earned           float64   `json:"earned"`
	ResultClosePrice float64   `json:"res_close_price"`
}

func (aw *AccountWorker) ProcessOrder() {
	prices := aw.AccountOrder.SlowExchangeCoin.Data.Price
	ch := prices.Subscribe()
	defer prices.Unsubscribe(ch)
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
	earned := math.Abs(value.Price/aw.AccountOrder.PriceStart-1)*aw.AccountOrder.BuyVolume - aw.AccountOrder.BuyVolume*aw.AccountOrder.SlowExchangeCoin.Commission
	if !isOK {
		earned = earned * -1
	}
	aw.AccountOrder.DealCloseTime = time.Now()
	aw.AccountOrder.Earned = earned
	aw.AccountOrder.ResultClosePrice = value.Price

	go aw.insertMetrics()

	log.Printf("ACC: %v  COIN: %v EARNED: %v", aw.AccountID, aw.AccountOrder.SlowExchangeCoin.Symbol, earned)
}

func (aw *AccountWorker) insertMetrics() {
	outp, err := json.Marshal(aw)
	if err != nil {
		log.Println("json marshal error:", err)
		return
	}
	if err := aw.Db.InsertAccountMetricsJSON(outp); err != nil {

		log.Println("Clickhouse insert error:", err)
	} else {
		log.Println("ClickHouse insert successful")
	}
}
