package orders

import (
	"encoding/json"
	"log"
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
	Db           *storage.ClickHouse `json:"-"`
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
			aw.storeMetrics(value)
			break
		}
		if aw.AccountOrder.IsUpDirect {
			if value.Price >= aw.AccountOrder.PriceClose {
				//close AccountOrder
				aw.storeMetrics(value)
				break
			}
			if value.Price < aw.AccountOrder.PriceAbort {
				//close AccountOrder
				aw.storeMetrics(value)
				break
			}
		} else {
			if value.Price <= aw.AccountOrder.PriceClose {
				//close AccountOrder
				aw.storeMetrics(value)
				break
			}
			if value.Price > aw.AccountOrder.PriceAbort {
				//close AccountOrder
				aw.storeMetrics(value)
				break
			}
		}
	}
}

func (aw *AccountWorker) storeMetrics(value types.PriceTime) {
	ao := aw.AccountOrder
	earned := calcPnL(ao.PriceStart, value.Price, ao.BuyVolume, ao.SlowExchangeCoin.Commission, ao.IsUpDirect)
	aw.AccountOrder.DealCloseTime = time.Now()
	aw.AccountOrder.Earned = earned
	aw.AccountOrder.ResultClosePrice = value.Price

	// Marshal now: the worker reuses aw.AccountOrder for the next deal while the insert runs.
	payload, err := json.Marshal(aw)
	if err != nil {
		log.Println("json marshal error:", err)
	} else {
		go aw.insertMetrics(payload)
	}

	log.Printf("ACC: %v  COIN: %v EARNED: %v", aw.AccountID, aw.AccountOrder.SlowExchangeCoin.Symbol, earned)
}

func (aw *AccountWorker) insertMetrics(payload []byte) {
	if err := aw.Db.InsertAccountMetricsJSON(payload); err != nil {
		log.Println("Clickhouse insert error:", err)
	} else {
		log.Println("ClickHouse insert successful")
	}
}

// calcPnL returns the profit of a deal opened at start and closed at closePrice,
// net of commission. A long (isUp) deal profits when the price rises, a short one when it falls.
func calcPnL(start, closePrice, volume, commission float64, isUp bool) float64 {
	ret := closePrice/start - 1
	if !isUp {
		ret = -ret
	}
	return ret*volume - volume*commission
}
