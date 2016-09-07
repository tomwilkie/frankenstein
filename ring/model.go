package ring

import (
	"time"
)

// Desc is the serialised state in Consul representing
// all ingesters (ie, the ring).
type Desc struct {
	Ingesters map[string]IngesterDesc `json:"ingesters"`
	Tokens    TokenDescs              `json:"tokens"`
}

// IngesterDesc describes a single ingester.
type IngesterDesc struct {
	Hostname  string    `json:"hostname"`
	Timestamp time.Time `json:"timestamp"`
}

// TokenDescs is a sortable list of TokenDescs
type TokenDescs []TokenDesc

func (ts TokenDescs) Len() int           { return len(ts) }
func (ts TokenDescs) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts TokenDescs) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// TokenDesc describes an individual token in the ring.
type TokenDesc struct {
	Token    uint32 `json:"tokens"`
	Ingester string `json:"ingester"`
}

func descFactory() interface{} {
	return newDesc()
}

func newDesc() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{},
	}
}
