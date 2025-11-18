//go:generate mockgen -source=deps.go -destination=optimization_mocks_test.go -package=app_test . Syncer,Controller
package app

import (
	"context"

	"github.com/yandex/mysync/internal/app/optimization"
)

type OptimizationSyncer interface {
	Sync(c optimization.Cluster) error
}

type OptimizationController interface {
	Wait(ctx context.Context, node optimization.Node) error
	Enable(node optimization.Node) error
	Disable(master, node optimization.Node) error
	DisableAll(master optimization.Node, nodes []optimization.Node) error
}
