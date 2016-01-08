package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type StatusType int

const (
	Ready    StatusType = 1
	Finished StatusType = 2
	Failed   StatusType = 3
)

type Job struct {
	Payload   string `json:"payload"`
	Callback  string `json:"callback"`
	Status    StatusType
	Timestamp time.Time
}

var jobs []Job

func handler(w http.ResponseWriter, r *http.Request) {

	if r.URL.Path[1:] == "jobs" {
		push(&w, r)
	} else if r.Method == "GET" {
		list(&w, r)
	}

}

func push(w *http.ResponseWriter, r *http.Request) (success bool) {
	var bodyBytes []byte

	if r.Body != nil {
		bodyBytes, _ = ioutil.ReadAll(r.Body)
	}

	job := Job{Timestamp: time.Now(), Status: Ready}
	err := json.Unmarshal(bodyBytes, &job)

	if err != nil {
		fmt.Fprintf(*w, "error: %s", err)
		return false
	}

	jobs = append(jobs, job)

	fmt.Fprintf(*w, "Added job!")
	return true
}

func list(w *http.ResponseWriter, r *http.Request) {
	for index, job := range jobs {
		fmt.Fprintf(*w, "Job: %d, %s, %s\n", index, job.Payload, job.Timestamp)
	}
}

func workJobs() {
	printJobs := 0
	for {
		if printJobs != len(jobs) {

			fmt.Printf("Working %d jobs!\n", len(jobs))

			printJobs = len(jobs)
			for i := range jobs {
				if jobs[i].Status == Ready {
					go doJob(&jobs[i])
				}
			}
		}
		clearFinishedJobs()
		time.Sleep(500 * time.Millisecond)
	}
}

func clearFinishedJobs() {
	for i := len(jobs) - 1; i >= 0; i-- {
		if jobs[i].Status == Finished {
			fmt.Printf("Removing job #%d\n", i)
			jobs = append(jobs[:i], jobs[i+1:]...)
		}
	}
}

func doJob(job *Job) {
	time.Sleep(2000 * time.Millisecond)
	job.Status = Finished
	fmt.Printf("Job finished!\n")
}

func main() {

	go workJobs()

	http.HandleFunc("/", handler)
	http.ListenAndServe(":9090", nil)

}
