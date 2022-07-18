package mpioperatortracker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dgruber/drmaa2interface"
	"github.com/dgruber/drmaa2os/pkg/helper"
	"github.com/dgruber/drmaa2os/pkg/jobtracker"
	clientset "github.com/kubeflow/mpi-operator/v2/pkg/client/clientset/versioned"
)

type MPIOperatorTracker struct {
	clientset clientset.Interface
}

func NewMPIOperatorTracker(kubeconfigPath string, testInstallMPIOperator bool) (*MPIOperatorTracker, error) {
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv("KUBECONFIG")
		if kubeconfigPath == "" {
			kubeconfigPath = os.Getenv("HOME") + "/.kube/config"
		}
	}

	// only for testing: Installs MPIOperator itself
	if testInstallMPIOperator {
		err := InstallMPIOperator(kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to install MPIOperator: %v\n", err)
		}
	}

	restConfig, err := NewRestConfig(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST config: %v\n", err)
	}
	cs, err := GetClient(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v\n", err)
	}
	return &MPIOperatorTracker{
		clientset: cs,
	}, nil
}

// ListJobs returns all visible job IDs or an error.
func (t *MPIOperatorTracker) ListJobs() ([]string, error) {
	jobs, err := ListJobs(context.Background(), t.clientset, "default")
	if err != nil {
		return nil, fmt.Errorf("failed to list MPIOperator jobs: %v\n", err)
	}
	names := make([]string, len(jobs))
	for job := range jobs {
		names = append(names, jobs[job].Name)
	}
	return names, nil
}

// ListArrayJobs returns all job IDs an job array ID (or array job ID)
// represents or an error.
func (t *MPIOperatorTracker) ListArrayJobs(arrayjobID string) ([]string, error) {
	return helper.ArrayJobID2GUIDs(arrayjobID)
}

// AddJob typically submits or starts a new job at the backend. The function
// returns the unique job ID or an error if job submission (or starting of
// the job in case there is no queueing system) has failed.
func (t *MPIOperatorTracker) AddJob(jobTemplate drmaa2interface.JobTemplate) (string, error) {
	spec, err := ConvertJobTemplateToMPIJob(jobTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to convert DRMAA2 job template to MPI job: %v\n", err)
	}
	job := NewMPIJob(spec)
	jobID, err := CreateJob(context.TODO(), t.clientset, &job, false)
	if err != nil {
		return "", fmt.Errorf("failed to create job: %v\n", err)
	}
	return jobID.Name, nil
}

// AddArrayJob makes a mass submission of jobs defined by the same job template.
// Many HPC workload manager support job arrays for submitting 10s of thousands
// of similar jobs by one call. The additional parameters define how many jobs
// are submitted by defining a TASK_ID range. Begin is the first task ID (like 1),
// end is the last task ID (like 10), step is a positive integeger which defines
// the increments from one task ID to the next task ID (like 1). maxParallel is
// an arguments representating an optional functionality which instructs the
// backend to limit maxParallel tasks of this job arary to run in parallel.
// Note, that jobs use the TASK_ID environment variable to identifiy which
// task they are and determine that way what to do (like which data set is
// accessed).
func (t *MPIOperatorTracker) AddArrayJob(jt drmaa2interface.JobTemplate, begin int, end int, step int, maxParallel int) (string, error) {
	return helper.AddArrayJobAsSingleJobs(jt, t, begin, end, step)
}

// JobState returns the DRMAA2 state and substate (free form string) of the job.
func (t *MPIOperatorTracker) JobState(jobID string) (drmaa2interface.JobState, string, error) {
	return GetJobState(context.Background(), t.clientset, "default", jobID)
}

// JobInfo returns the job status of a job in form of a JobInfo struct or an error.
func (t *MPIOperatorTracker) JobInfo(jobID string) (drmaa2interface.JobInfo, error) {
	return GetJobInfo(context.Background(), t.clientset, "default", jobID)
}

// JobControl sends a request to the backend to either "terminate", "suspend",
// "resume", "hold", or "release" a job. The strings are fixed and are defined
// by the JobControl constants. This could change in the future to be limited
// only to constants representing the actions. When the request is not accepted
// by the system the function must return an error.
func (t *MPIOperatorTracker) JobControl(jobID string, action string) error {
	switch action {
	case jobtracker.JobControlSuspend:
		return errors.New("unsupported operation")
	case jobtracker.JobControlResume:
		return errors.New("unsupported operation")
	case jobtracker.JobControlHold:
		return errors.New("unsupported operation")
	case jobtracker.JobControlRelease:
		return errors.New("unsupported operation")
	case jobtracker.JobControlTerminate:
		// there seems no way to stop a job
		return t.DeleteJob(jobID)
	}
	return fmt.Errorf("undefined job operation")
}

// Wait blocks until the job is either in one of the given states, the max.
// waiting time (specified by timeout) is reached or an other internal
// error occured (like job was not found). In case of a timeout also an
// error must be returned.
func (t *MPIOperatorTracker) Wait(jobID string, timeout time.Duration, states ...drmaa2interface.JobState) error {
	return helper.WaitForState(t, jobID, timeout, states...)
}

// DeleteJob removes a job from a potential internal database. It does not stop
// a job. A job must be in an endstate (terminated, failed) in order to call
// DeleteJob. In case of an error or the job is not in an end state error must be
// returned. If the backend does not support cleaning up resources for a finished
// job nil should be returned.
func (t *MPIOperatorTracker) DeleteJob(jobID string) error {
	err := DeleteJob(context.Background(), t.clientset, "default", jobID)
	if err != nil {
		return fmt.Errorf("failed to delete job: %v\n", err)
	}
	return nil
}

// ListJobCategories returns a list of job categories which can be used in the
// JobCategory field of the job template. The list is informational. An example
// is returning a list of supported container images. AddJob() and AddArrayJob()
// processes a JobTemplate and hence also the JobCategory field.
func (MPIOperatorTracker) ListJobCategories() ([]string, error) {
	// all kind of launcher images are supported
	return []string{}, nil
}
