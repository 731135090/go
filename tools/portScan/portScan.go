package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	PORT_MIN = 1
	PORT_MAX = 65535

	WORK_NUM = 20
)

type Scan struct {
	Host      string
	StartPort uint
	EndPort   uint
	CurrPort  uint
	channel   chan uint
	wait      sync.WaitGroup
	lock      sync.Mutex
}

var scan = new(Scan)

func main() {
	flag.UintVar(&scan.StartPort, "s_p", PORT_MIN, "start port")
	flag.UintVar(&scan.EndPort, "e_p", PORT_MAX, "end port")
	flag.StringVar(&scan.Host, "h", "49.234.187.114", "host")
	flag.Parse()

	scan.init()

	scan.wait.Add(1)
	go scan.Produce()

	scan.wait.Add(1)
	go scan.Consumer()

	go func() {
		for {
			if scan.CurrPort >= scan.EndPort {
				break
			}
			time.Sleep(2 * time.Second)
			logInfo()
		}
	}()

	scan.wait.Wait()
	logInfo()
}

func logInfo() {
	log := fmt.Sprintf("start port：%d, end port：%d, curr port：%d", scan.StartPort, scan.EndPort, scan.CurrPort)
	fmt.Println(log)
}

func (s *Scan) init() {
	s.channel = make(chan uint, 1000)
	scan.StartPort = uint(math.Min(float64(scan.StartPort), float64(PORT_MIN)))
	scan.EndPort = uint(math.Min(float64(scan.EndPort), float64(PORT_MAX)))
}

func scanRun(address string, port string) (bool, error) {
	conn, err := net.DialTimeout("tcp", address+":"+port, 1*time.Second)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	return true, nil
}

func (s *Scan) Consumer() {
	defer func() {
		s.wait.Done()
	}()

	pNum := make(chan int, WORK_NUM)
	pWait := sync.WaitGroup{}
	for {
		port, ok := <-s.channel
		if !ok {
			break
		}
		pNum <- 1
		go func(p uint) {
			pWait.Add(1)
			pWait.Done()
			rs, _ := scanRun(s.Host, strconv.Itoa(int(p)))
			scan.CurrPort = p
			if rs {
				println("open port：" + strconv.Itoa(int(p)))
			}
			<-pNum
		}(port)
	}
	pWait.Wait()
}

func (s *Scan) Produce() {
	defer func() {
		s.wait.Done()
		close(s.channel)
	}()

	for i := s.StartPort; i <= s.EndPort; i++ {
		s.channel <- i
	}
}
