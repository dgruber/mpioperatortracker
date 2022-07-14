package mpioperatortracker

import (
	"context"

	clientset "github.com/kubeflow/mpi-operator/v2/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func DeleteJob(ctx context.Context, mpiClient clientset.Interface, namespace, jobName string) error {
	return mpiClient.KubeflowV2beta1().MPIJobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{})
}
