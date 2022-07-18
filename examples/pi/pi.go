package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgruber/drmaa2interface"
	"github.com/dgruber/mpioperatortracker"
)

func main() {
	tracker, err := mpioperatortracker.NewMPIOperatorTracker("", true)
	if err != nil {
		panic(err)
	}
	jobTemplate := drmaa2interface.JobTemplate{
		JobCategory: "mpioperator/mpi-pi:intel",
		Args:        []string{"mpirun", "-n", "2", "hostname"},
		MinSlots:    2,
		MaxSlots:    2,
	}
	jobID, err := tracker.AddJob(jobTemplate)
	if err != nil {
		panic(err)
	}
	fmt.Printf("JobID: %s\n", jobID)
	state, _, err := tracker.JobState(jobID)
	for state != drmaa2interface.Done &&
		state != drmaa2interface.Failed {
		fmt.Printf("job is in state %s\n", state.String())
		state, _, err = tracker.JobState(jobID)
		<-time.Tick(time.Second)
	}
	fmt.Printf("job is in state %s\n", state.String())
	ji, err := tracker.JobInfo(jobID)
	if err != nil {
		panic(err)
	}
	formatted, _ := json.Marshal(ji)
	fmt.Printf("JobInfo: %s\n", string(formatted))
}
