package mpioperatortracker

import (
	"github.com/dgruber/drmaa2interface"
	kubeflow "github.com/kubeflow/mpi-operator/v2/pkg/apis/kubeflow/v2beta1"
)

func JobInfoFromMPIJob(mpiJob *kubeflow.MPIJob) (jobInfo drmaa2interface.JobInfo) {
	jobInfo = drmaa2interface.JobInfo{
		ID:    mpiJob.Name,
		Slots: int64(*mpiJob.Spec.SlotsPerWorker * *mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Replicas),
	}
	return jobInfo
}
