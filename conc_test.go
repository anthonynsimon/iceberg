package iceberg

import (
	"sort"
	"sync"
	"testing"
)

func BenchmarkMutex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		var list []int
		mu := sync.Mutex{}
		wg := sync.WaitGroup{}
		wg.Add(10000)

		b.StartTimer()
		for i := 0; i < 10000; i++ {
			go func(i int) {
				mu.Lock()
				list = append(list, i)
				mu.Unlock()
				wg.Done()
			}(i)
		}

		wg.Wait()
		b.StopTimer()

		sort.Ints(list)

		for i := 0; i < 10000; i++ {
			if list[i] != i {
				b.Fail()
			}
		}
	}
}

func BenchmarkChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		var list []int
		writeChan := make(chan int, 1)
		wg := sync.WaitGroup{}
		wg.Add(10000)

		b.StartTimer()
		go func() {
			for {
				i := <-writeChan
				list = append(list, i)
			}
		}()

		for i := 0; i < 10000; i++ {
			go func(i int) {
				writeChan <- i
				wg.Done()
			}(i)
		}
		wg.Wait()
		b.StopTimer()

		sort.Ints(list)

		for i := 0; i < 10000; i++ {
			if list[i] != i {
				b.Fail()
			}
		}
	}
}
