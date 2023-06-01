package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/roman-mazur/design-practice-2-template/datastore"
)

var store *datastore.Db

type putReq struct {
	Value string	`json:"value"`
}

type getRes struct {
	Key   string	`json:"key"`
	Value string	`json:"value"`
}

const mb10 = 1024*1024*10

func main() {
	db, err := datastore.NewDb("./cmd/db/store", mb10)
	store = db
	if err != nil {
		log.Fatal(err.Error())
	}
	router := mux.NewRouter()
	router.HandleFunc("/db/{key}", getValue).Methods("GET")
	router.HandleFunc("/db/{key}", putValue).Methods("POST")

	log.Println("Database started")
	err = http.ListenAndServe(":9000", router)
	log.Fatal(err)
}

func getValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	value, err := store.Get(key)
	log.Printf("GET key %s from db", key)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	resS := getRes{Key: key, Value: value}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resS)
}

func putValue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	var putR putReq
	err = json.Unmarshal(body, &putR)
	if err != nil {
		http.Error(w, "Failed to parse JSON", http.StatusBadRequest)
		return
	}
	log.Printf("PUT %s: %s into db", key, putR.Value)
	store.Put(key, putR.Value)
	w.WriteHeader(http.StatusAccepted)
}
