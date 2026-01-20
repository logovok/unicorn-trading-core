package strategies

import "time"

type PriceMeanDiff struct {
	ThreasholdSets map[string]*PMDThreasholdSet
}

type PMDThreasholdSet struct {
	// TODO: Adjust value calc + distributor logic so that data is kept for longest window and distributed as asked
	// OR to do separate calcs for different windows
	// Window             time.Duration `json:"window"`
	CrossExchangeLag      time.Duration `json:"cross_exchange_lag"`
	DealTimeout           time.Duration `json:"deal_timeout"`
	OrderVolumeMultiplier float64       `json:"order_volume_percent"`
}

// TODO: use more params to calculate close price
// Note: the func is bound to strategy, as we may want different implemetation of this per strategy
func (pmd *ConcretePriceMeanDiff) calcPriceStart(price float64) float64 {
	return price
}

func (pmd *ConcretePriceMeanDiff) calcPriceClose(price float64, diff float64) float64 {
	return price + diff
}

func (pmd *ConcretePriceMeanDiff) calcPriceAbort(price float64) float64 {
	return price
}

func (pmd *ConcretePriceMeanDiff) calcOrderVolume(price float64, configSet string) float64 {
	return price * pmd.TS.OrderVolumeMultiplier
}

type ConcretePriceMeanDiff struct {
	TS      PMDThreasholdSet `json:"threashold_set"`
	Winrate float64          `json:"winrate"`
}
