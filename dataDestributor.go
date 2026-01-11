package main

type Distributor[T any] interface {
	Subscribe() <-chan T
	Run()
}

type DataDistributor[T any] struct {
	input   <-chan T
	outputs []chan T
}

func NewDataDistributor[T any](input <-chan T) *DataDistributor[T] {
	return &DataDistributor[T]{
		input:   input,
		outputs: []chan T{},
	}
}

func (d *DataDistributor[T]) Subscribe() <-chan T {
	ch := make(chan T)
	d.outputs = append(d.outputs, ch)
	return ch
}

func (d *DataDistributor[T]) Run() {
	for val := range d.input {
		for _, ch := range d.outputs {
			go func(c chan T, v T) {
				c <- v
			}(ch, val)
		}
	}
}
