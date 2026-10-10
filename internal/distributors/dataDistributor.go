package distributors

import "sync"

type Distributor[T any] interface {
	Subscribe() <-chan T
	Unsubscribe(ch <-chan T)
	Run()
}

type subscriber[T any] struct {
	ch   chan T
	done chan struct{}
}

type DataDistributor[T any] struct {
	input   <-chan T
	mu      sync.RWMutex
	outputs []subscriber[T]
}

func NewDataDistributor[T any](input <-chan T) *DataDistributor[T] {
	return &DataDistributor[T]{
		input: input,
	}
}

func (d *DataDistributor[T]) Subscribe() <-chan T {
	s := subscriber[T]{ch: make(chan T), done: make(chan struct{})}
	d.mu.Lock()
	d.outputs = append(d.outputs, s)
	d.mu.Unlock()
	return s.ch
}

// Unsubscribe stops delivery to ch and releases any sends still pending for it.
func (d *DataDistributor[T]) Unsubscribe(ch <-chan T) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, s := range d.outputs {
		if (<-chan T)(s.ch) == ch {
			close(s.done)
			d.outputs = append(d.outputs[:i], d.outputs[i+1:]...)
			return
		}
	}
}

func (d *DataDistributor[T]) Run() {
	for val := range d.input {
		d.mu.RLock()
		for _, s := range d.outputs {
			go func(s subscriber[T], v T) {
				select {
				case s.ch <- v:
				case <-s.done:
				}
			}(s, val)
		}
		d.mu.RUnlock()
	}
}
