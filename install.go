package mpioperatortracker

import (
	_ "embed"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/dgruber/drmaa2interface"
	"github.com/dgruber/wfl"
)

//go:embed mpi-operator.yaml
var mpiOperatorYaml string

func InstallMPIOperator(kubeconfigPath string) error {
	flow := wfl.NewWorkflow(wfl.NewProcessContextByCfg(
		wfl.ProcessConfig{
			DefaultTemplate: drmaa2interface.JobTemplate{
				OutputPath: "/dev/stdout",
				ErrorPath:  "/dev/stderr",
				JobEnvironment: map[string]string{
					"KUBECONFIG": kubeconfigPath,
				},
			},
		},
	))
	file, _ := ioutil.TempFile("", "mpiOperator.yaml")
	file.WriteString(mpiOperatorYaml)
	file.Close()
	defer os.Remove(file.Name())
	job := flow.Run("/bin/bash", "-c", `cat `+file.Name()+` | kubectl apply -f -`).Wait()
	if job.Success() {
		return nil
	}
	return fmt.Errorf("failed to install MPI Operator")
}
