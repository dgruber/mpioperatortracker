package mpioperatortracker

import (
	"encoding/json"
	"fmt"

	"github.com/dgruber/drmaa2interface"
	common "github.com/kubeflow/common/pkg/apis/common/v1"
	kubeflow "github.com/kubeflow/mpi-operator/v2/pkg/apis/kubeflow/v2beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var _ = Describe("Mpioperator", func() {

	Context("Successfully converting job templates", func() {

		It("should convert a minimal job template", func() {
			var basicJobTemplate drmaa2interface.JobTemplate
			basicJobTemplate.RemoteCommand = "echo"
			basicJobTemplate.Args = []string{"hello", "world"}
			basicJobTemplate.JobCategory = "mpi-launcher"
			basicJobTemplate.MinSlots = 2
			_, err := ConvertJobTemplateToMPIJob(basicJobTemplate)
			Expect(err).To(BeNil())
		})

		It("should convert an example job", func() {
			var basicJobTemplate drmaa2interface.JobTemplate
			basicJobTemplate.JobCategory = "rongou/tensorflow_benchmarks:latest"
			basicJobTemplate.MinSlots = 2
			_, err := ConvertJobTemplateToMPIJob(basicJobTemplate)
			Expect(err).To(BeNil())
		})

		It("should convert the Pi job example", func() {
			piTemplate := drmaa2interface.JobTemplate{
				JobCategory:   "mpioperator/mpi-pi:intel",
				RemoteCommand: "",
				Args:          []string{"mpirun", "-n", "2", "hostname"},
				MinSlots:      2,
				MaxSlots:      2,
			}
			piTemplate = SetSSHMountPathExtension(piTemplate, "/home/mpiuser/.ssh")
			piTemplate = SetWorkerCommandExtension(piTemplate, "/usr/sbin/sshd", "-De", "-f", "/home/mpiuser/.sshd_config")
			piTemplate = SetRunAsUserExtension(piTemplate, 1000)
			piTemplate = SetSlotsPerWorkerExtension(piTemplate, 1)

			piJob, err := ConvertJobTemplateToMPIJob(piTemplate)
			Expect(err).To(BeNil())
			Expect(piJob.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers[0].Image).To(Equal("mpioperator/mpi-pi:intel"))
			Expect(piJob.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers[0].Command).To(BeNil())
			Expect(piJob.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers[0].Args).To(Equal([]string{"mpirun", "-n", "2", "hostname"}))
			Expect(*piJob.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers[0].SecurityContext.RunAsUser).To(BeNumerically("==", 1000))
			Expect(*piJob.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Containers[0].SecurityContext.RunAsUser).To(BeNumerically("==", 1000))
		})

		It("should convert an OpenFoam example job", func() {

			// example from:
			// https://github.com/OpenShiftDemos/kubeflow-mpi-openfoam/blob/main/manifests/mpijob-dambreak-example.yaml
			var basicJobTemplate drmaa2interface.JobTemplate

			basicJobTemplate.RemoteCommand = "/bin/bash"
			basicJobTemplate.Args = []string{"/home/openfoam/scripts/damBreak.sh"}
			basicJobTemplate.JobCategory = "quay.io/openshiftdemos/kubeflow-mpi-openfoam:latest"

			// resource requests are missing
			basicJobTemplate = SetLauncherResourceRequestsExtension(basicJobTemplate,
				corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceCPU):    *resource.NewQuantity(1, resource.DecimalSI),
					corev1.ResourceName(corev1.ResourceMemory): *resource.NewQuantity(1024*1024*1024, resource.BinarySI),
				})
			basicJobTemplate = SetLauncherResourceLimitExtension(basicJobTemplate,
				corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceCPU):    *resource.NewQuantity(1, resource.DecimalSI),
					corev1.ResourceName(corev1.ResourceMemory): *resource.NewQuantity(1024*1024*1024, resource.BinarySI),
				})
			basicJobTemplate = SetWorkerResourceRequestsExtension(basicJobTemplate,
				corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceCPU): *resource.NewQuantity(2, resource.DecimalSI),
				})
			basicJobTemplate = SetWorkerResourceLimitExtension(basicJobTemplate,
				corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceCPU): *resource.NewQuantity(2, resource.DecimalSI),
				})

			// file mounts
			basicJobTemplate = SetVolumeMounts(basicJobTemplate, []VolumeMountSpec{
				{
					MountPath:  "/home/openfoam/storage",
					ReadOnly:   false,
					VolumeType: "pvc",
					VolumeName: "foam-tutorials-claim",
				},
				{
					MountPath:  "/home/openfoam/scripts",
					ReadOnly:   true,
					VolumeType: "configmap",
					VolumeName: "dambreak-job:damBreak.sh:damBreak.sh", // items: key / path
				},
			})

			basicJobTemplate = SetWorkerCommandExtension(basicJobTemplate,
				"/usr/sbin/sshd", "-De", "-f", "/home/openfoam/.sshd_config")

			basicJobTemplate = SetSSHMountPathExtension(basicJobTemplate, "/home/openfoam/.ssh")
			basicJobTemplate = SetSlotsPerWorkerExtension(basicJobTemplate, 2)
			basicJobTemplate.MaxSlots = 2

			out, _ := json.Marshal(basicJobTemplate)
			fmt.Println(string(out))

			spec, err := ConvertJobTemplateToMPIJob(basicJobTemplate)
			Expect(err).To(BeNil())

			Expect(spec.SSHAuthMountPath).To(Equal("/home/openfoam/.ssh"))
			Expect(*spec.SlotsPerWorker).To(BeNumerically("==", 2))
			//Expect(spec.MPIImplementation).To(Equal("intelmpi"))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher]).NotTo(BeNil())
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]).NotTo(BeNil())
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].RestartPolicy).To(Equal(common.RestartPolicyNever))
			Expect(len(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Containers)).To(BeNumerically("==", 1))
			Expect(len(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers)).To(BeNumerically("==", 1))

			// launcher
			launcher := spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers[0]
			Expect(launcher.Command).To(Equal([]string{"/bin/bash"}))
			Expect(launcher.Args).To(Equal([]string{"/home/openfoam/scripts/damBreak.sh"}))
			Expect(launcher.Image).To(Equal("quay.io/openshiftdemos/kubeflow-mpi-openfoam:latest"))
			Expect(launcher.Resources.Requests.Cpu().String()).To(Equal("1"))
			Expect(launcher.Resources.Limits.Cpu().String()).To(Equal("1"))
			Expect(launcher.VolumeMounts[0].MountPath).To(Or(Equal("/home/openfoam/storage"), Equal("/home/openfoam/scripts")))
			//Expect(launcher.VolumeMounts[0].ReadOnly).To(Equal(false))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("foam-tutorials-claim"))
			Expect(launcher.VolumeMounts[1].MountPath).To(Or(Equal("/home/openfoam/storage"), Equal("/home/openfoam/scripts")))
			//Expect(launcher.VolumeMounts[1].ReadOnly).To(Equal(true))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Volumes[1].ConfigMap.Name).To(Equal("dambreak-job"))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Volumes[1].ConfigMap.Items[0].Key).To(Equal("damBreak.sh"))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Volumes[1].ConfigMap.Items[0].Path).To(Equal("damBreak.sh"))

			// worker
			worker := spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Containers[0]
			Expect(worker.Command).To(Equal([]string{"/usr/sbin/sshd"}))
			Expect(worker.Args).To(Equal([]string{"-De", "-f", "/home/openfoam/.sshd_config"}))
			Expect(worker.Image).To(Equal("quay.io/openshiftdemos/kubeflow-mpi-openfoam:latest"))
			Expect(worker.Resources.Requests.Cpu().String()).To(Equal("2"))
			Expect(worker.Resources.Limits.Cpu().String()).To(Equal("2"))
			Expect(worker.VolumeMounts[0].MountPath).To(Equal("/home/openfoam/storage"))
			Expect(worker.VolumeMounts[0].ReadOnly).To(Equal(false))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("foam-tutorials-claim"))
			Expect(worker.VolumeMounts[1].MountPath).To(Equal("/home/openfoam/scripts"))
			Expect(worker.VolumeMounts[1].ReadOnly).To(Equal(true))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Volumes[1].ConfigMap.Name).To(Equal("dambreak-job"))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Volumes[1].ConfigMap.Items[0].Key).To(Equal("damBreak.sh"))
			Expect(spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.Spec.Volumes[1].ConfigMap.Items[0].Path).To(Equal("damBreak.sh"))

		})

	})

	Context("Malformed job templates", func() {

		It("should fail to convert an unset job template", func() {
			var basicJobTemplate drmaa2interface.JobTemplate
			_, err := ConvertJobTemplateToMPIJob(basicJobTemplate)
			Expect(err).NotTo(BeNil())
		})

	})

})
