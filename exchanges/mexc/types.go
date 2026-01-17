package mexc

import "trading/core/internal/types"

type Mex struct {
	types.Exchange
}

type MexcMsg struct {
	Channel string `json:"channel"`
	Data    struct {
		Close float64 `json:"c"`
	} `json:"data"`
}
