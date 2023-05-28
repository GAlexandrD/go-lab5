package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("http", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type serverType struct {
	dst             string
	dataTransferred int
	isWorking       bool
}

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []serverType{
		{
			dst:             "server1:8080",
			dataTransferred: 0,
			isWorking:       false,
		}, {
			dst:             "server2:8080",
			dataTransferred: 0,
			isWorking:       false,
		}, {
			dst:             "server3:8080",
			dataTransferred: 0,
			isWorking:       false,
		},
	}
)

type healthCheckerInterface interface {
	health(dst string) bool
}

type Balancer struct {
	pool []serverType
	hc   healthCheckerInterface
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func SetupBalancer() *Balancer{
	balancer := &Balancer{
		pool: serversPool,
		hc:   &healthChecker{},
	}
	balancer.runChecker()
	balancer.Start()
	return balancer
}

func (b *Balancer) runChecker() {
	for i := range b.pool {
		server := &b.pool[i]
		go func() {
			server.isWorking = b.hc.health(server.dst)
			for range time.Tick(10 * time.Second) {
				server.isWorking = b.hc.health(server.dst)
			}
		}()
	}
}

func (b *Balancer) Start() {
	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		index, err := b.getIndex()
		if err != nil {
			log.Println(err.Error())
		} else {
			b.forward(&b.pool[index], rw, r)
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

type healthChecker struct{}

func (hc *healthChecker) health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

var cnt int = 0

func (b *Balancer) forward(server *serverType, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = server.dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = server.dst
	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {

		// count length of server response and save it
		length := headerLength(resp.Header) + int(resp.ContentLength)
		server.dataTransferred += length

		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", server.dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", server.dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()
	SetupBalancer()
}

func (b *Balancer) getIndex() (int, error) {
	index := 0
	minData := math.MaxInt
	for i := range b.pool {
		if !b.pool[i].isWorking {
			continue
		}
		curData := b.pool[i].dataTransferred
		if curData < minData {
			index = i
			minData = curData
		}
	}
	if !b.pool[index].isWorking {
		return 0, errors.New("There are no servers available")
	}
	return index, nil
}

func headerLength(header http.Header) int {
	var str string
	for key, values := range header {
		for _, value := range values {
			str += fmt.Sprintf("%s: %s\n", key, value)
		}
	}
	byteSlice := []byte(str)
	byteCount := len(byteSlice)
	return byteCount
}
