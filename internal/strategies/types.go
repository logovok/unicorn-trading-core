package strategies

import "time"

type PriceMeanDiff struct {
	ThreasholdSets map[string]*PMDThreasholdSet
}

type PMDThreasholdSet struct {
	Window             time.Duration `json:"window"`
	CrossExchangeLag   time.Duration `json:"cross_exchange_lag"`
	DealTimeout        time.Duration `json:"deal_timeout"`
	OrderVolumePercent float64       `json:"order_volume_percent"`
}

// TODO: use more params to calculate close price
// Note: the func is bound to strategy, as we may want different implemetation of this per strategy
func (pmd *PriceMeanDiff) calcPriceStart(price float64) float64 {
	return price
}

func (pmd *PriceMeanDiff) calcPriceClose(price float64, diff float64) float64 {
	return price + diff
}

func (pmd *PriceMeanDiff) calcPriceAbort(price float64) float64 {
	return price
}
