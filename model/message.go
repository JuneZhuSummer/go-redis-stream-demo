package model

type Message struct {
	Event string `json:"event,omitempty"`
	Data  any    `json:"data,omitempty"`
}

type RegisterData struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}
