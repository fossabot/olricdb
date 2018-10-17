package transport

import (
	"bytes"
	"sync"
)

type BufPool struct {
	p sync.Pool
}

func NewBufPool() *BufPool {
	return &BufPool{
		p: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *BufPool) Get() *bytes.Buffer {
	return p.p.Get().(*bytes.Buffer)
}

func (p *BufPool) Put(b *bytes.Buffer) {
	b.Reset()
	p.p.Put(b)
}
