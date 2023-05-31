package integration

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 10 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	resps := make(map[string]int)
	for i := 0; i <= 2; i++ {
		serv, size := sendAndGetInfo(t)
		resps[serv] += size
	}
	_, ok := resps["server1:8080"]
	assert.Equal(t, true, ok, "server1 wasn't used")
	_, ok = resps["server2:8080"]
	assert.Equal(t, true, ok, "server2 wasn't used")
	_, ok = resps["server3:8080"]
	assert.Equal(t, true, ok, "server3 wasn't used")

	for i := 0; i <= 1; i++ {
		serv, size := sendAndGetInfo(t)
		resps[serv] += size
	}
	assert.Equal(t, 3, len(resps), "unknown server used")
	serv := findMinimal(resps)
	s, sz := sendAndGetInfo(t)
	resps[s] += sz
	assert.Equal(t, serv, s, "balancer choose wrong server")
}

func findMinimal(resps map[string]int) string {
	var (
		serv string
		size int
	)
	for s, sz := range resps {
		if serv == "" {
			serv = s
			size = sz
			continue
		}
		if sz < size {
			serv = s
			size = sz
		}
	}
	return serv
}

func sendAndGetInfo(t *testing.T) (string, int) {
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
	assert.Nil(t, err, err)
	from := resp.Header.Get("lb-from")
	size, _ := strconv.Atoi(resp.Header.Get("lb-size"))
	return from, size
}

func BenchmarkBalancer(b *testing.B) {
	client := http.Client{Timeout: 10 * time.Second}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get("http://localhost:8090/api/v1/some-data")
			if err != nil {
				b.Fatal(err)
			}
			resp.Body.Close()
		}
	})
}
