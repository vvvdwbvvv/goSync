package models

type BinanceTrade struct {
	E           string  `json:"e"`        // Event type, e.g. "trade"
	EvtTime     int64   `json:"E"`        // Event time (Unix毫秒)
	Symbol      string  `json:"s"`        // 交易對, e.g. "BNBBTC"
	TradeID     int64   `json:"t"`        // 交易ID
	Price       float64 `json:"p,string"` // 價格 (JSON是字串, 用 ,string 幫助自動轉 float64)
	Qty         float64 `json:"q,string"` // 數量
	TradeTime   int64   `json:"T"`        // 交易時間 (Unix毫秒)
	MarketMaker bool    `json:"m"`        // 是否為Maker
	Ignore      bool    `json:"M"`        // 無用欄位, 但我們仍可存下來
}
