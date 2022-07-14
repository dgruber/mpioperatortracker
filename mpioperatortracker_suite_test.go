package mpioperatortracker_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMpioperatortracker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mpioperatortracker Suite")
}
