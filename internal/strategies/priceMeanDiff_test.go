package strategies

import (
	"math"
	"testing"
	"time"

	"github.com/logovok/unicorn-trading-core/internal/types"
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newStrategy() *PriceMeanDiff {
	return &PriceMeanDiff{
		Name: "test",
		TS: &PMDThresholdSet{
			Name:             "test",
			CrossExchangeLag: 400 * time.Millisecond,
			DealTimeout:      15 * time.Second,
			OrderVolume:      10,
		},
	}
}

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}

func TestProcess(t *testing.T) {
	btcLead := types.Coin{Symbol: "btcusdt", Multiplier: 1, Thresholds: types.Thresholds{DiffThreshold: 5}}
	btcSlow := types.Coin{Symbol: "BTC_USDT", Multiplier: 1, Thresholds: types.Thresholds{CrossExchangePriceThreshold: 1}}
	// Binance quotes 1000 SHIB per contract, MEXC quotes a single SHIB.
	shibLead := types.Coin{Symbol: "1000shibusdt", Multiplier: 0.001, Thresholds: types.Thresholds{DiffThreshold: 0.000000005}}
	shibSlow := types.Coin{Symbol: "SHIB_USDT", Multiplier: 1, Thresholds: types.Thresholds{CrossExchangePriceThreshold: 0.000000000005}}

	tests := []struct {
		name      string
		amd       types.AvgMeanDiff
		pt        types.PriceTime
		lead      types.Coin
		slow      types.Coin
		wantOrder bool
		wantUp    bool
		wantClose float64
	}{
		{
			name: "no data yet",
			lead: btcLead, slow: btcSlow,
		},
		{
			name: "lead and slow ticks too far apart",
			amd:  types.AvgMeanDiff{Diff: -10, Prc: 99990, Time: now.Add(500 * time.Millisecond)},
			pt:   types.PriceTime{Price: 100000, Time: now},
			lead: btcLead, slow: btcSlow,
		},
		{
			name: "diff below coin threshold",
			amd:  types.AvgMeanDiff{Diff: -4.9, Prc: 99990, Time: now},
			pt:   types.PriceTime{Price: 100000, Time: now},
			lead: btcLead, slow: btcSlow,
		},
		{
			name: "exchange prices too close",
			amd:  types.AvgMeanDiff{Diff: -10, Prc: 99999.5, Time: now},
			pt:   types.PriceTime{Price: 100000, Time: now},
			lead: btcLead, slow: btcSlow,
		},
		{
			name: "negative diff opens a long",
			amd:  types.AvgMeanDiff{Diff: -10, Prc: 99990, Time: now},
			pt:   types.PriceTime{Price: 100000, Time: now},
			lead: btcLead, slow: btcSlow,
			wantOrder: true, wantUp: true, wantClose: 100010,
		},
		{
			name: "positive diff opens a short",
			amd:  types.AvgMeanDiff{Diff: 10, Prc: 100010, Time: now},
			pt:   types.PriceTime{Price: 100000, Time: now},
			lead: btcLead, slow: btcSlow,
			wantOrder: true, wantUp: false, wantClose: 99990,
		},
		{
			name: "diff equal to threshold passes",
			amd:  types.AvgMeanDiff{Diff: 5, Prc: 100010, Time: now},
			pt:   types.PriceTime{Price: 100000, Time: now},
			lead: btcLead, slow: btcSlow,
			wantOrder: true, wantUp: false, wantClose: 99995,
		},
		{
			name: "close target is scaled by the lead multiplier",
			amd:  types.AvgMeanDiff{Diff: -0.00005, Prc: 0.01205, Time: now},
			pt:   types.PriceTime{Price: 0.000012, Time: now},
			lead: shibLead, slow: shibSlow,
			wantOrder: true, wantUp: true, wantClose: 0.00001205,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order := newStrategy().Process(tt.amd, tt.pt, tt.lead, tt.slow)
			if !tt.wantOrder {
				if order != nil {
					t.Fatalf("Process() returned an order, want nil: %+v", order)
				}
				return
			}
			if order == nil {
				t.Fatal("Process() returned nil, want an order")
			}
			if order.IsUpDirect != tt.wantUp {
				t.Errorf("IsUpDirect = %v, want %v", order.IsUpDirect, tt.wantUp)
			}
			if !approxEqual(order.PriceClose, tt.wantClose) {
				t.Errorf("PriceClose = %v, want %v", order.PriceClose, tt.wantClose)
			}
			if order.PriceStart != tt.pt.Price {
				t.Errorf("PriceStart = %v, want %v", order.PriceStart, tt.pt.Price)
			}
			if order.PriceAbort != tt.pt.Price {
				t.Errorf("PriceAbort = %v, want %v", order.PriceAbort, tt.pt.Price)
			}
			if order.Volume != 10 {
				t.Errorf("Volume = %v, want 10", order.Volume)
			}
			if order.DealTimeout != 15*time.Second {
				t.Errorf("DealTimeout = %v, want 15s", order.DealTimeout)
			}
		})
	}
}
