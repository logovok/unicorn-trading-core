package types

func (exch Exchange) GetCoin(coin string) (*Coin, bool) {
	res, ok := exch.Coins[coin]
	return res, ok
}

func (exch Exchange) GetName() string {
	return exch.Name
}

func (c Coin) GetPrice(basePrice float64) float64 {
	return basePrice * float64(c.Multiplier)
}
