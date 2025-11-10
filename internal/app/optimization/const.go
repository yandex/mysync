package optimization

import "errors"

const (
	// List of nodes which are going to run optimization mode. May be modified by external tools (e.g. add/remove node)
	// structure: pathOptimizationNodes/hostname -> nil
	pathOptimizationNodes = "optimization_nodes"
)

var errOptimizationWaitingDeadlineExceeded = errors.New("optimization waiting deadline exceeded")
