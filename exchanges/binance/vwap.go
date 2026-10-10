package binance

import (
	"time"

	"github.com/logovok/unicorn-trading-core/internal/types"
)

// vwapWindow keeps the trades of the last size period and their running sums,
// so the volume-weighted average price is updated in O(1) per trade.
type vwapWindow struct {
	size   time.Duration
	trades []types.Trade
	sumPV  float64
	sumV   float64
}

// add records the trade, drops trades that are size or more older than now,
// and returns the VWAP of what is left. ok is false when the window is empty.
func (w *vwapWindow) add(trade types.Trade, now time.Time) (vwap float64, ok bool) {
	w.trades = append(w.trades, trade)
	w.sumPV += trade.Price * trade.Volume
	w.sumV += trade.Volume

	cutoff := now.Add(-w.size)
	i := 0
	for i < len(w.trades) && !w.trades[i].Time.After(cutoff) {
		w.sumPV -= w.trades[i].Price * w.trades[i].Volume
		w.sumV -= w.trades[i].Volume
		i++
	}
	if i > 0 {
		w.trades = w.trades[i:]
	}

	if w.sumV == 0 {
		return 0, false
	}
	return w.sumPV / w.sumV, true
}
