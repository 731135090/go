package main

import "fmt"

type Incr struct {
	startId int
	key     string
	ch      chan int
}

func main() {
	incr := GetIncr("redis:incr:key", 5)
	for i := 0; i < 100; i++ {
		fmt.Println(incr.AutoIncrement())
	}
}

func GetIncr(key string, id int) *Incr {
	incr := new(Incr)
	incr.key = key
	incr.startId = id
	incr.ch = make(chan int)
	go func() {
		for i := incr.startId; true; i++ {
			incr.ch <- i
		}
	}()
	return incr
}

func (incr *Incr) AutoIncrement() int {
	return <-incr.ch
}
