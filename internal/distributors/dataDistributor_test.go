package distributors

import (
	"runtime"
	"testing"
	"time"
)

const recvTimeout = time.Second

func recv(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(recvTimeout):
		t.Fatal("timed out waiting for a value")
		return 0
	}
}

func TestDataDistributorFanOut(t *testing.T) {
	input := make(chan int)
	defer close(input)
	d := NewDataDistributor(input)
	first := d.Subscribe()
	second := d.Subscribe()
	go d.Run()

	input <- 42

	if got := recv(t, first); got != 42 {
		t.Errorf("first subscriber got %v, want 42", got)
	}
	if got := recv(t, second); got != 42 {
		t.Errorf("second subscriber got %v, want 42", got)
	}
}

func TestDataDistributorUnsubscribe(t *testing.T) {
	input := make(chan int)
	defer close(input)
	d := NewDataDistributor(input)
	gone := d.Subscribe()
	kept := d.Subscribe()
	go d.Run()

	d.Unsubscribe(gone)

	input <- 1
	if got := recv(t, kept); got != 1 {
		t.Errorf("kept subscriber got %v, want 1", got)
	}
}

// A subscriber that stops reading leaves one blocked send per value behind.
// Unsubscribe must release them.
func TestDataDistributorUnsubscribeReleasesPendingSends(t *testing.T) {
	input := make(chan int)
	defer close(input)
	d := NewDataDistributor(input)
	sub := d.Subscribe()
	go d.Run()

	before := runtime.NumGoroutine()
	const pending = 10
	for i := 0; i < pending; i++ {
		input <- i
	}

	d.Unsubscribe(sub)

	deadline := time.Now().Add(recvTimeout)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines still running, want at most %d", runtime.NumGoroutine(), before)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDataDistributorUnsubscribeUnknownChannel(t *testing.T) {
	input := make(chan int)
	defer close(input)
	d := NewDataDistributor(input)
	sub := d.Subscribe()
	go d.Run()

	d.Unsubscribe(make(chan int))

	input <- 7
	if got := recv(t, sub); got != 7 {
		t.Errorf("subscriber got %v, want 7", got)
	}
}
