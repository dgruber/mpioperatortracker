package mpioperatortracker

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dgruber/drmaa2interface"
	common "github.com/kubeflow/common/pkg/apis/common/v1"
	kubeflow "github.com/kubeflow/mpi-operator/v2/pkg/apis/kubeflow/v2beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
)

const ExtensionWorkerImage = "workerImage"
const ExtensionWorkerCommand = "workerCommand"
const ExtensionSlotsPerWorker = "slotsPerWorker"
const ExtensionMPIImplementation = "mpiImplementation"
const ExtensionSSHMountPath = "sshMountPath"
const ExtensionRunAsUser = "runAsUser"

type VolumeMountSpec struct {
	MountPath  string // path to mount volume inside the container
	ReadOnly   bool
	VolumeType string // like pvc, or cm/configmap
	VolumeName string // like pvc-name or configmap-name
}

func toInt32(i int32) *int32 {
	return &i
}

// should be same as:
// https://github.com/dgruber/drmaa2os/tree/master/pkg/jobtracker/kubernetestracker
// https://github.com/dgruber/drmaa2os/blob/master/pkg/jobtracker/kubernetestracker/convert.go

func SetVolumeMounts(jt drmaa2interface.JobTemplate, vm []VolumeMountSpec) drmaa2interface.JobTemplate {
	if jt.StageInFiles == nil {
		jt.StageInFiles = make(map[string]string)
	}
	for _, v := range vm {
		vt := v.VolumeType
		if v.ReadOnly {
			vt += "-read"
		}

		// configmap can have items: key and path to mount single files
		// that is part of VolumeName
		jt.StageInFiles[v.MountPath] = vt + ":" + v.VolumeName
	}
	return jt
}

func GetVolumeMounts(jt drmaa2interface.JobTemplate) []VolumeMountSpec {
	if jt.StageInFiles == nil {
		return nil
	}
	vm := make([]VolumeMountSpec, 0)
	for k, v := range jt.StageInFiles {
		vt := strings.Split(v, ":")
		if len(vt) < 2 {
			continue
		}
		vm = append(vm, VolumeMountSpec{
			MountPath:  k,
			ReadOnly:   strings.HasSuffix(vt[0], "-read"),
			VolumeType: strings.TrimSuffix(vt[0], "-read"),
			VolumeName: strings.Join(vt[1:], ":"),
		})
	}
	return vm
}

func SetLauncherResourceLimitExtension(jt drmaa2interface.JobTemplate, limits v1.ResourceList) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	for k, v := range limits {
		jt.ExtensionList["resourceLimitLauncher-"+k.String()] = v.String()
	}
	return jt
}

func GetLauncherResourceLimitExtension(jt drmaa2interface.JobTemplate) v1.ResourceList {
	return getResourceExtension(jt, "resourceLimitLauncher-")
}

func SetWorkerResourceLimitExtension(jt drmaa2interface.JobTemplate, limits v1.ResourceList) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	for k, v := range limits {
		jt.ExtensionList["resourceLimitWorker-"+k.String()] = v.String()
	}
	return jt
}

func GetWorkerResourceLimitExtension(jt drmaa2interface.JobTemplate) v1.ResourceList {
	return getResourceExtension(jt, "resourceLimitWorker-")
}

func SetLauncherResourceRequestsExtension(jt drmaa2interface.JobTemplate, requests v1.ResourceList) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	for k, v := range requests {
		jt.ExtensionList["resourceRequestLauncher-"+k.String()] = v.String()
	}
	return jt
}

func getResourceExtension(jt drmaa2interface.JobTemplate, extensionPrefix string) v1.ResourceList {
	if jt.ExtensionList == nil {
		return nil
	}
	requests := make(v1.ResourceList)
	for k, v := range jt.ExtensionList {
		if strings.HasPrefix(k, extensionPrefix) {
			resourceName := strings.TrimPrefix(k, extensionPrefix)
			quantity, err := resource.ParseQuantity(v)
			if err != nil {
				fmt.Printf("Failed to parse resource quantity %s: %s\n", v, err)
				continue
			}
			requests[v1.ResourceName(resourceName)] = quantity
		}
	}
	return requests
}

func GetLauncherResourceRequestExtension(jt drmaa2interface.JobTemplate) v1.ResourceList {
	return getResourceExtension(jt, "resourceRequestLauncher-")
}

func SetWorkerResourceRequestsExtension(jt drmaa2interface.JobTemplate, requests v1.ResourceList) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	for k, v := range requests {
		jt.ExtensionList["resourceRequestWorker-"+k.String()] = v.String()
	}
	return jt
}

func GetWorkerResourceRequestExtension(jt drmaa2interface.JobTemplate) v1.ResourceList {
	return getResourceExtension(jt, "resourceRequestWorker-")
}

func SetWorkerImageExtension(jt drmaa2interface.JobTemplate, workerImage string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionWorkerImage] = workerImage
	return jt
}

func GetWorkerImageExtension(jt drmaa2interface.JobTemplate) string {
	if jt.ExtensionList == nil {
		return ""
	}
	return jt.ExtensionList[ExtensionWorkerImage]
}

func SetWorkerCommandExtension(jt drmaa2interface.JobTemplate, workerCommand ...string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionWorkerCommand] = strings.Join(workerCommand, "<!~!>")
	return jt
}

func GetWorkerCommandExtension(jt drmaa2interface.JobTemplate) []string {
	if jt.ExtensionList == nil {
		_, exists := jt.ExtensionList[ExtensionWorkerCommand]
		if !exists {
			return nil
		}

	}
	return strings.Split(jt.ExtensionList[ExtensionWorkerCommand], "<!~!>")
}

func SetSlotsPerWorkerExtension(jt drmaa2interface.JobTemplate, slotsPerWorker int32) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionSlotsPerWorker] = strconv.Itoa(int(slotsPerWorker))
	return jt
}

func GetSlotsPerWorkerExtension(jt drmaa2interface.JobTemplate) int32 {
	if jt.ExtensionList == nil {
		return 1
	}
	slotsPerWorker, err := strconv.Atoi(jt.ExtensionList[ExtensionSlotsPerWorker])
	if err != nil {
		return 1
	}
	return int32(slotsPerWorker)
}

func SetSSHMountPathExtension(jt drmaa2interface.JobTemplate, sshMountPath string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionSSHMountPath] = sshMountPath
	return jt
}

func GetSSHMountPathExtension(jt drmaa2interface.JobTemplate) string {
	if jt.ExtensionList == nil {
		return "/root/.ssh"
	}
	return jt.ExtensionList[ExtensionSSHMountPath]
}

func SetMPIImplementationExtension(jt drmaa2interface.JobTemplate, mpiImplementation string) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionMPIImplementation] = mpiImplementation
	return jt
}

func GetMPIImplementationExtension(jt drmaa2interface.JobTemplate) kubeflow.MPIImplementation {
	if jt.ExtensionList == nil {
		return kubeflow.MPIImplementationIntel
	}
	if jt.ExtensionList[ExtensionMPIImplementation] == "" {
		return kubeflow.MPIImplementationIntel
	}
	return kubeflow.MPIImplementation(jt.ExtensionList[ExtensionMPIImplementation])
}

func SetRunAsUserExtension(jt drmaa2interface.JobTemplate, runAsUser int64) drmaa2interface.JobTemplate {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[ExtensionRunAsUser] = fmt.Sprintf("%d", runAsUser)
	return jt
}

func GetRunAsUserExtension(jt drmaa2interface.JobTemplate) int64 {
	if jt.ExtensionList == nil {
		return -1
	}
	user := jt.ExtensionList[ExtensionRunAsUser]
	if userID, err := strconv.ParseInt(user, 10, 64); err == nil {
		return userID
	}
	return -1
}

func ConvertJobTemplateToMPIJob(jt drmaa2interface.JobTemplate) (kubeflow.MPIJobSpec, error) {
	if jt.JobCategory == "" {
		return kubeflow.MPIJobSpec{}, fmt.Errorf("JobCategory is required. It specifies the MPI launcher image")
	}
	launcherImage := jt.JobCategory // "mpi-launcher"
	workerImage := GetWorkerImageExtension(jt)
	if workerImage == "" {
		workerImage = jt.JobCategory
	}
	workerCommand := GetWorkerCommandExtension(jt) // // default not set / mpi-operator starts the sshd
	var workerArgs []string
	if len(workerCommand) > 1 {
		workerArgs = workerCommand[1:]
		workerCommand = workerCommand[0:1]
		if workerCommand[0] == "" {
			// Command in container spec needs not be set when worker command
			// is "", otherwise it will not let entrypoint to be used.
			workerCommand = nil
		}
	}
	sshAuthMountPath := GetSSHMountPathExtension(jt)
	slotsPerWorker := GetSlotsPerWorkerExtension(jt)
	mpiImplementation := GetMPIImplementationExtension(jt)

	workerReplicas := jt.MinSlots
	if jt.MaxSlots > jt.MinSlots {
		workerReplicas = jt.MaxSlots
	}
	if workerReplicas == 0 {
		return kubeflow.MPIJobSpec{}, fmt.Errorf("MinSlots or MaxSlots is required. It specifies the number of workers")
	}

	launcherTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:       "launcher",
					Image:      launcherImage,
					Command:    []string{jt.RemoteCommand},
					Args:       jt.Args,
					WorkingDir: jt.WorkingDirectory,
					Resources: v1.ResourceRequirements{
						Requests: GetLauncherResourceRequestExtension(jt),
						Limits:   GetLauncherResourceLimitExtension(jt),
					},
				},
			},
		},
	}
	if jt.RemoteCommand == "" {
		// Command needs to be not set otherwise entrypoint is not used
		launcherTemplate.Spec.Containers[0].Command = nil
	}

	workerTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:       "worker",
					Image:      workerImage,
					Command:    workerCommand,
					Args:       workerArgs,
					WorkingDir: jt.WorkingDirectory,
					Resources: v1.ResourceRequirements{
						Requests: GetWorkerResourceRequestExtension(jt),
						Limits:   GetWorkerResourceLimitExtension(jt),
					},
				},
			},
		},
	}

	user := GetRunAsUserExtension(jt)
	if user != -1 {
		launcherTemplate.Spec.Containers[0].SecurityContext = &v1.SecurityContext{
			RunAsUser: &user,
		}
		workerTemplate.Spec.Containers[0].SecurityContext = &v1.SecurityContext{
			RunAsUser: &user,
		}
	}

	// add same volume mounts to launcher and worker
	for i, volumeMount := range GetVolumeMounts(jt) {
		volumeName := fmt.Sprintf("volume-%d", i)
		switch volumeMount.VolumeType {
		case "configmap", "cm":
			// might have items listed
			volumeRefNames := strings.Split(volumeMount.VolumeName, ":")
			var items []v1.KeyToPath
			if len(volumeRefNames) == 3 {
				// add file names from config map as items
				items = make([]v1.KeyToPath, 1)
				items[0].Key = volumeRefNames[1]
				items[0].Path = volumeRefNames[2]
			}
			launcherTemplate.Spec.Containers[0].VolumeMounts =
				append(launcherTemplate.Spec.Containers[0].VolumeMounts,
					v1.VolumeMount{
						Name:      volumeName,
						MountPath: volumeMount.MountPath,
						ReadOnly:  volumeMount.ReadOnly,
					})

			workerTemplate.Spec.Containers[0].VolumeMounts = append(workerTemplate.Spec.Containers[0].VolumeMounts,
				v1.VolumeMount{
					Name:      volumeName,
					MountPath: volumeMount.MountPath,
					ReadOnly:  volumeMount.ReadOnly,
				})

			vol := v1.Volume{
				Name: volumeName,
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: volumeRefNames[0],
						},
						Items: items,
					},
				},
			}

			launcherTemplate.Spec.Volumes = append(workerTemplate.Spec.Volumes, vol)
			workerTemplate.Spec.Volumes = append(workerTemplate.Spec.Volumes, vol)

		case "pvc":
			launcherTemplate.Spec.Containers[0].VolumeMounts =
				append(launcherTemplate.Spec.Containers[0].VolumeMounts,
					v1.VolumeMount{
						Name:      volumeName,
						MountPath: volumeMount.MountPath,
						ReadOnly:  volumeMount.ReadOnly,
					})
			workerTemplate.Spec.Containers[0].VolumeMounts =
				append(workerTemplate.Spec.Containers[0].VolumeMounts,
					v1.VolumeMount{
						Name:      volumeName,
						MountPath: volumeMount.MountPath,
						ReadOnly:  volumeMount.ReadOnly,
					})
			launcherTemplate.Spec.Volumes = append(launcherTemplate.Spec.Volumes,
				v1.Volume{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: volumeMount.VolumeName,
							ReadOnly:  volumeMount.ReadOnly,
						},
					},
				})
			workerTemplate.Spec.Volumes = append(workerTemplate.Spec.Volumes,
				v1.Volume{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: volumeMount.VolumeName,
							ReadOnly:  volumeMount.ReadOnly,
						},
					},
				})
		default:
			klog.Errorf("Unsupported volume type (only configmap, pvc allowed): %s", volumeMount.VolumeType)
		}
	}

	spec := kubeflow.MPIJobSpec{
		RunPolicy: common.RunPolicy{
			BackoffLimit:   toInt32(0),
			CleanPodPolicy: newCleanPodPolicy(common.CleanPodPolicyRunning),
		},
		MPIImplementation: mpiImplementation,
		SlotsPerWorker:    toInt32(slotsPerWorker),
		SSHAuthMountPath:  sshAuthMountPath,
		MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*common.ReplicaSpec{
			kubeflow.MPIReplicaTypeLauncher: {
				RestartPolicy: common.RestartPolicyNever,
				Template:      launcherTemplate,
			},
			kubeflow.MPIReplicaTypeWorker: {
				RestartPolicy: common.RestartPolicyNever,
				Replicas:      toInt32(int32(workerReplicas)),
				Template:      workerTemplate,
			},
		},
	}

	return spec, nil
}

func newCleanPodPolicy(v common.CleanPodPolicy) *common.CleanPodPolicy {
	return &v
}
