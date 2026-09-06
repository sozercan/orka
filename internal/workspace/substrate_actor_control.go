package workspace

import (
	"context"
	"sort"
	"strings"
)

// SubstrateActorPoolExecutor is a control-only adapter for controller-owned Substrate actor pools.
type SubstrateActorPoolExecutor struct {
	control substrateControlClient
}

// NewSubstrateActorPoolExecutor returns the control-only adapter used by the actor pool controller.
func NewSubstrateActorPoolExecutor(cfg SubstrateConfig) (*SubstrateActorPoolExecutor, error) {
	if cfg.ControlClient == nil {
		client, err := newGRPCSubstrateControlClient(cfg)
		if err != nil {
			return nil, err
		}
		cfg.ControlClient = client
	}
	return &SubstrateActorPoolExecutor{control: cfg.ControlClient}, nil
}

// Close releases network resources owned by this adapter.
func (e *SubstrateActorPoolExecutor) Close() error {
	if closer, ok := e.control.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// SubstratePoolTelemetry reports safe actor/worker density for a single Orka pool.
func (e *SubstrateActorPoolExecutor) SubstratePoolTelemetry(
	ctx context.Context,
	prefix string,
	template TemplateRef,
	workerPool TemplateRef,
) (Density, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "-")
	workers, err := e.control.ListWorkers(ctx)
	if err != nil {
		return Density{}, err
	}
	actors, err := e.control.ListActors(ctx)
	if err != nil {
		return Density{}, err
	}
	filteredActors := make([]substrateActor, 0, len(actors))
	actorIDs := make(map[string]struct{}, len(actors))
	for _, actor := range actors {
		actorID := strings.TrimSpace(actor.ActorID)
		if prefix != "" && !strings.HasPrefix(actorID, prefix+"-") {
			continue
		}
		if strings.TrimSpace(template.Namespace) != "" && strings.TrimSpace(actor.TemplateNamespace) != strings.TrimSpace(template.Namespace) {
			continue
		}
		if strings.TrimSpace(template.Name) != "" && strings.TrimSpace(actor.TemplateName) != strings.TrimSpace(template.Name) {
			continue
		}
		filteredActors = append(filteredActors, actor)
		actorIDs[actorID] = struct{}{}
	}
	filteredWorkers := make([]substrateWorker, 0, len(workers))
	for _, worker := range workers {
		if strings.TrimSpace(workerPool.Name) != "" && strings.TrimSpace(worker.WorkerPool) != strings.TrimSpace(workerPool.Name) {
			continue
		}
		if strings.TrimSpace(workerPool.Namespace) != "" && strings.TrimSpace(worker.WorkerNamespace) != strings.TrimSpace(workerPool.Namespace) {
			continue
		}
		if workerActorID := strings.TrimSpace(worker.ActorID); workerActorID != "" {
			if _, ok := actorIDs[workerActorID]; !ok {
				continue
			}
		} else if strings.TrimSpace(workerPool.Name) == "" {
			continue
		}
		filteredWorkers = append(filteredWorkers, worker)
	}
	return substrateDensity(filteredWorkers, filteredActors), nil
}

// EnsureSubstrateActors creates deterministic actor records for a pool target.
func (e *SubstrateActorPoolExecutor) EnsureSubstrateActors(
	ctx context.Context,
	prefix string,
	target int,
	template TemplateRef,
) (int, error) {
	if target <= 0 {
		return 0, nil
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "-")
	if prefix == "" {
		return 0, NewError("ensure substrate actors", ErrorKindInvalidArgument, "actor prefix is required", false, nil)
	}
	created := 0
	for i := range target {
		actorID := deterministicSubstratePoolActorID(prefix, i)
		if actor, err := e.control.GetActor(ctx, actorID); err == nil {
			if err := validateSubstrateActorTemplateForOp("ensure substrate actors", actor, template); err != nil {
				return created, err
			}
			continue
		} else if !IsKind(err, ErrorKindNotFound) {
			return created, err
		}
		if _, err := e.control.CreateActor(ctx, actorID, template.Namespace, template.Name); err != nil {
			if IsKind(err, ErrorKindAlreadyExists) {
				actor, getErr := e.control.GetActor(ctx, actorID)
				if getErr != nil {
					return created, getErr
				}
				if err := validateSubstrateActorTemplateForOp("ensure substrate actors", actor, template); err != nil {
					return created, err
				}
				continue
			}
			return created, err
		}
		created++
	}
	return created, nil
}

// ConvergeSubstrateActors creates missing deterministic actors below target and
// deletes deterministic pool actors at or above target.
func (e *SubstrateActorPoolExecutor) ConvergeSubstrateActors(
	ctx context.Context,
	prefix string,
	target int,
	template TemplateRef,
) (int, int, error) {
	if target < 0 {
		return 0, 0, NewError("converge substrate actors", ErrorKindInvalidArgument, "actor target must be non-negative", false, nil)
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "-")
	if prefix == "" {
		return 0, 0, NewError("converge substrate actors", ErrorKindInvalidArgument, "actor prefix is required", false, nil)
	}

	created := 0
	if target > 0 {
		var err error
		created, err = e.EnsureSubstrateActors(ctx, prefix, target, template)
		if err != nil {
			return created, 0, err
		}
	}

	deleted, err := e.PruneSubstrateActors(ctx, prefix, target)
	if err != nil {
		return created, deleted, err
	}
	return created, deleted, nil
}

// PruneSubstrateActors deletes deterministic pool actors at or above target.
func (e *SubstrateActorPoolExecutor) PruneSubstrateActors(
	ctx context.Context,
	prefix string,
	target int,
) (int, error) {
	if target < 0 {
		return 0, NewError("prune substrate actors", ErrorKindInvalidArgument, "actor target must be non-negative", false, nil)
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "-")
	if prefix == "" {
		return 0, NewError("prune substrate actors", ErrorKindInvalidArgument, "actor prefix is required", false, nil)
	}

	actors, err := e.control.ListActors(ctx)
	if err != nil {
		return 0, err
	}
	actorsByOrdinal := make(map[int]string)
	ordinals := make([]int, 0)
	for _, actor := range actors {
		ordinal, ok := substratePoolActorOrdinal(actor.ActorID, prefix)
		if !ok || ordinal < target {
			continue
		}
		if _, exists := actorsByOrdinal[ordinal]; exists {
			continue
		}
		actorsByOrdinal[ordinal] = strings.TrimSpace(actor.ActorID)
		ordinals = append(ordinals, ordinal)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ordinals)))

	deleted := 0
	for _, ordinal := range ordinals {
		if err := e.control.DeleteActor(ctx, actorsByOrdinal[ordinal]); err != nil {
			if IsKind(err, ErrorKindNotFound) {
				continue
			}
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
