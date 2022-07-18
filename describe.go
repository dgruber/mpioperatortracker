package mpioperatortracker

import (
	"context"
	"fmt"

	"github.com/dgruber/drmaa2interface"
	kubeflow "github.com/kubeflow/mpi-operator/v2/pkg/apis/kubeflow/v2beta1"
	clientset "github.com/kubeflow/mpi-operator/v2/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	common "github.com/kubeflow/common/pkg/apis/common/v1"
)

func ListJobs(ctx context.Context, mpiClient clientset.Interface, namespace string) ([]kubeflow.MPIJob, error) {
	if mpiClient == nil {
		return nil, fmt.Errorf("MPI client is nil")
	}
	kfClient := mpiClient.KubeflowV2beta1()
	mpiJobList, err := kfClient.MPIJobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return mpiJobList.Items, nil
}

func DescribeJob(ctx context.Context, mpiClient clientset.Interface, namespace, jobName string) (*kubeflow.MPIJob, error) {
	return mpiClient.KubeflowV2beta1().MPIJobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
}

func GetJobInfo(ctx context.Context, mpiClient clientset.Interface, namespace, jobName string) (drmaa2interface.JobInfo, error) {
	job, err := DescribeJob(ctx, mpiClient, namespace, jobName)
	if err != nil {
		return drmaa2interface.JobInfo{}, err
	}
	return JobInfoFromMPIJob(job), nil
}

func GetJobState(ctx context.Context, mpiClient clientset.Interface, namespace, jobName string) (drmaa2interface.JobState, string, error) {
	job, err := DescribeJob(ctx, mpiClient, namespace, jobName)
	if err != nil {
		return drmaa2interface.Undetermined, "unknown job", err
	}
	if len(job.Status.Conditions) == 0 {
		return drmaa2interface.Queued, "no condition", nil
	}
	return JobStateFromCondition(job.Status.Conditions[len(job.Status.Conditions)-1])
}

func JobStateFromCondition(lastCondition common.JobCondition) (drmaa2interface.JobState, string, error) {
	switch lastCondition.Type {
	case common.JobCreated:
		return drmaa2interface.Queued, "created", nil
	case common.JobRunning:
		return drmaa2interface.Running, "running", nil
	case common.JobRestarting:
		return drmaa2interface.Requeued, "restarting", nil
	case common.JobSucceeded:
		return drmaa2interface.Done, "succeeded", nil
	case common.JobFailed:
		return drmaa2interface.Failed, "failed", nil
	}
	return drmaa2interface.Undetermined, fmt.Sprintf("unknown condition type %v", lastCondition.Type), nil
}
