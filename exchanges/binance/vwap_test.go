package binance

import (
	"testing"
	"time"

	"github.com/logovok/unicorn-trading-core/internal/types"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func trade(offset time.Duration, price, volume float64) types.Trade {
	return types.Trade{Time: t0.Add(offset), Price: price, Volume: volume}
}

func TestVWAPWindowSingleTrade(t *testing.T) {
	w := vwapWindow{size: 15 * time.Second}

	got, ok := w.add(trade(0, 100, 2), t0)
	if !ok || got != 100 {
		t.Errorf("add() = %v, %v, want 100, true", got, ok)
	}
}

func TestVWAPWindowWeightsByVolume(t *testing.T) {
	w := vwapWindow{size: 15 * time.Second}

	w.add(trade(0, 100, 1), t0)
	got, ok := w.add(trade(time.Second, 200, 3), t0.Add(time.Second))
	if !ok || got != 175 {
		t.Errorf("add() = %v, %v, want 175, true", got, ok)
	}
}

func TestVWAPWindowDropsOldTrades(t *testing.T) {
	w := vwapWindow{size: 15 * time.Second}

	w.add(trade(0, 100, 1), t0)
	w.add(trade(10*time.Second, 200, 1), t0.Add(10*time.Second))
	// At 16s the first trade is out of the 15s window, the second is still in.
	got, ok := w.add(trade(16*time.Second, 400, 1), t0.Add(16*time.Second))
	if !ok || got != 300 {
		t.Errorf("add() = %v, %v, want 300, true", got, ok)
	}
	if len(w.trades) != 2 {
		t.Errorf("window holds %d trades, want 2", len(w.trades))
	}
}

func TestVWAPWindowDropsTradeExactlyAtCutoff(t *testing.T) {
	w := vwapWindow{size: 15 * time.Second}

	w.add(trade(0, 100, 1), t0)
	got, ok := w.add(trade(15*time.Second, 200, 1), t0.Add(15*time.Second))
	if !ok || got != 200 {
		t.Errorf("add() = %v, %v, want 200, true", got, ok)
	}
}

func TestVWAPWindowEmptyAfterStaleTrade(t *testing.T) {
	w := vwapWindow{size: 15 * time.Second}

	_, ok := w.add(trade(0, 100, 1), t0.Add(time.Minute))
	if ok {
		t.Error("add() reported a VWAP for a trade older than the window")
	}
}
