# mpioperatortracker

Experimental [MPIOperator](https://github.com/kubeflow/mpi-operator) support for [DRMAA2OS](https://github.com/dgruber/drmaa2os). This project evaluates
the usefulness of DRMAA2 for managing MPIOperator jobs running on Kubernetes.

## How MPIOperator Works

MPIOperator provides a framework / Kubernetes operator for running MPI jobs
based on IntelMPI or OpenMPI on Kubernetes. For that it implements a custom
resource definition (CRD) for the kind MPIJob. Submitting jobs require writing
these specific yaml files.

## What mpioperatortracker is

It is a DRMAA2 implementation of the jobtracker interface which allows
to hook MPIOpertor jobs into the DRMAA2 Go framework so that they
can be submitted, supervised, and managed from the well defined
DRMAA2 interfaces. The DRMAA2 [JobTemplate](https://github.com/dgruber/drmaa2interface/blob/master/jobtemplate.go) is used for submitting
MPIOperator jobs. The DRMAA2 [JobInfo](https://github.com/dgruber/drmaa2interface/blob/master/jobinfo.go) struct is used for getting the status of a job.

## How to use it

## Converting a DRMAA2 Job Template to an MPIOperator Job

## JobInfo Fields

## Job Control Mapping

## Job State Mapping

## Examples

