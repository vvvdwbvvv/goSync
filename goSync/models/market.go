package models

type Trade struct {
	Symbol    string  `json:"symbol"`
	Type      string  `json:"type"`
	Price     float64 `json:"price"`
	Volume    float64 `json:"volume"`
	Timestamp int64   `json:"timestamp"`
}

type OrderBookUpdate struct {
	Symbol    string      `json:"symbol"`
	Type      string      `json:"type"`
	Bids      [][]float64 `json:"bids"`
	Asks      [][]float64 `json:"asks"`
	Timestamp int64       `json:"timestamp"`
}
