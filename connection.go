package mpioperatortracker

import (
	"fmt"

	clientset "github.com/kubeflow/mpi-operator/v2/pkg/client/clientset/versioned"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func NewRestConfig(kubeConfigPath string) (*rest.Config, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, err
	}
	return config, err
}

func GetClient(restConfig *rest.Config) (clientset.Interface, error) {
	mpiClient, err := clientset.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	if mpiClient == nil {
		return nil, fmt.Errorf("failed to create mpi client")
	}
	return mpiClient, nil
}
