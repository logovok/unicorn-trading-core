package orders

import (
	"math"
	"testing"
)

func TestCalcPnL(t *testing.T) {
	tests := []struct {
		name       string
		start      float64
		closePrice float64
		volume     float64
		commission float64
		isUp       bool
		want       float64
	}{
		{"long, price rises", 100, 101, 10, 0.0002, true, 0.098},
		{"long, price falls", 100, 99, 10, 0.0002, true, -0.102},
		{"short, price falls", 100, 99, 10, 0.0002, false, 0.098},
		{"short, price rises", 100, 101, 10, 0.0002, false, -0.102},
		{"flat price costs the commission", 100, 100, 10, 0.0002, true, -0.002},
		{"no commission", 100, 101, 10, 0, true, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcPnL(tt.start, tt.closePrice, tt.volume, tt.commission, tt.isUp)
			if math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("calcPnL() = %v, want %v", got, tt.want)
			}
		})
	}
}
