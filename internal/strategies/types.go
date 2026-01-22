package strategies

import "time"

type PriceMeanDiffGlobals struct {
	ThreasholdSets map[string]*PMDThreasholdSet
}

type PMDThreasholdSet struct {
	// TODO: Adjust value calc + distributor logic so that data is kept for longest window and distributed as asked
	// OR to do separate calcs for different windows
	// Window             time.Duration `json:"window"`
	CrossExchangeLag            time.Duration `json:"cross_exchange_lag"`
	DealTimeout                 time.Duration `json:"deal_timeout"`
	OrderVolumeMultiplier       float64       `json:"order_volume_percent"`
	DiffThreshold               float64       `json:"diff_threashold"`
	CrossExchangePriceThreshold float64       `json:"cross_exchange_price_threshold"`
}

type PriceMeanDiff struct {
	Name string
	TS   *PMDThreasholdSet `json:"threashold_set"`
}

func (cpd *PriceMeanDiff) GetName() string {
	return cpd.Name
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

func (pmd *PriceMeanDiff) calcOrderVolume(price float64) float64 {
	return price * pmd.TS.OrderVolumeMultiplier
}
