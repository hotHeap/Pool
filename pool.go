package pool

import (
	"errors"
	"io"
	"sync"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid pool config")
	ErrPoolClosed    = errors.New("pool closed")
)

type Poolable interface {
	io.Closer
	GetActiveTime() time.Time
}

type factory func() (Poolable, error)

type Pool interface {
	Acquire() (Poolable, error) // get a connection
	Release(Poolable) error     // release a connection
	Close(Poolable) error       // close a connection
	Shutdown() error            // destory the pool
}

type GenericPool struct {
	sync.Mutex
	pool        chan Poolable
	maxOpen     int  // max connections in the pool
	numOpen     int  // current connections in the pool
	minOpen     int  // min connection in the pool
	closed      bool //  if closed
	maxLifetime time.Duration
	factory     factory // method to new a connection
}

func NewGenericPool(minOpen, maxOpen int, maxLifetime time.Duration, factory factory) (*GenericPool, error) {
	if maxOpen <= 0 || minOpen > maxOpen {
		return nil, ErrInvalidConfig
	}
	p := &GenericPool{
		maxOpen:     maxOpen,
		minOpen:     minOpen,
		maxLifetime: maxLifetime,
		factory:     factory,
		pool:        make(chan Poolable, maxOpen),
	}

	for i := 0; i < minOpen; i++ {
		closer, err := factory()
		if err != nil {
			continue
		}
		p.numOpen++
		p.pool <- closer
	}
	return p, nil
}

func (p *GenericPool) Acquire() (Poolable, error) {
	if p.closed {
		return nil, ErrPoolClosed
	}
	for {
		closer, err := p.getOrCreate()
		if err != nil {
			return nil, err
		}
		// Consider of timeout duration
		if p.maxLifetime > 0 && closer.GetActiveTime().Add(p.maxLifetime).Before(time.Now()) {
			p.Close(closer)
			continue
		}
		return closer, nil
	}
}

func (p *GenericPool) getOrCreate() (Poolable, error) {
	select {
	case closer := <-p.pool:
		return closer, nil
	default:
	}
	p.Lock()
	if p.numOpen >= p.maxOpen {
		closer := <-p.pool
		p.Unlock()
		return closer, nil
	}
	// new a connection
	closer, err := p.factory()
	if err != nil {
		p.Unlock()
		return nil, err
	}
	p.numOpen++
	p.Unlock()
	return closer, nil
}

// a connection put back to pool
func (p *GenericPool) Release(closer Poolable) error {
	if p.closed {
		return ErrPoolClosed
	}
	p.Lock()
	p.pool <- closer
	p.Unlock()
	return nil
}

// close a connection
func (p *GenericPool) Close(closer Poolable) error {
	p.Lock()
	closer.Close()
	p.numOpen--
	p.Unlock()
	return nil
}

// close the connection pool and release all resource
func (p *GenericPool) Shutdown() error {
	if p.closed {
		return ErrPoolClosed
	}
	p.Lock()
	close(p.pool)
	for closer := range p.pool {
		closer.Close()
		p.numOpen--
	}
	p.closed = true
	p.Unlock()
	return nil
}
