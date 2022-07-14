package mpioperatortracker

import (
	"context"
	"time"

	kubeflow "github.com/kubeflow/mpi-operator/v2/pkg/apis/kubeflow/v2beta1"
	clientset "github.com/kubeflow/mpi-operator/v2/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/util/wait"
)

func NewMPIJob(spec kubeflow.MPIJobSpec) (job kubeflow.MPIJob) {
	job.Namespace = "default"
	job.GenerateName = "drmaa2-mpioperator-job-"
	job.Spec = spec
	return
}

func CreateJob(ctx context.Context, mpiClient clientset.Interface, mpiJob *kubeflow.MPIJob, waitForJob bool) (*kubeflow.MPIJob, error) {
	mpiJob, err := mpiClient.KubeflowV2beta1().MPIJobs(mpiJob.Namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	// wait for the job to be finished
	if waitForJob {
		err = wait.Poll(time.Second*5, time.Hour*24*30, func() (bool, error) {
			updatedJob, err := mpiClient.KubeflowV2beta1().MPIJobs(mpiJob.Namespace).Get(ctx, mpiJob.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			mpiJob = updatedJob
			return mpiJob.Status.CompletionTime != nil, nil
		})
	}
	return mpiJob, err
}
