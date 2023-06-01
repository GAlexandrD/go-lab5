package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var target = "http://database:9000"

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	h := new(http.ServeMux)
	currentTime := time.Now()
	dateString := currentTime.Format("2006-01-02")
	payload := []byte(`{"value":"` + dateString + `"}`)
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/db/team-name", target), bytes.NewBuffer(payload))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	type Record struct {
		key   string
		value string
	}

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}
		report.Process(r)

		queryParams := r.URL.Query()
		key := queryParams.Get("key")
		resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/db/%s", target, key))
		if err != nil {
			http.NotFound(rw, r)
			return
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			http.NotFound(rw, r)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		rw.Write(body)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
