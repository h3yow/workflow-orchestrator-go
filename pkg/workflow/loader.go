package workflow

import (
	"fmt"
)

// Workflow 是加载校验后的工作流：
// 任务按 ID 建立索引，并预计算确定性的拓扑顺序（定义顺序在前、
// 依赖在前的次序），供调度器稳定遍历。
type Workflow struct {
	def   WorkflowDef
	tasks map[string]TaskDef
	order []string
}

// Load 加载并校验工作流定义。
//
// 校验依次为：
//  1. 工作流必须至少包含一个任务；
//  2. 任务 ID 非空，且不得重复定义；
//  3. 每个依赖必须指向存在的任务；
//  4. 依赖关系不得成环（Kahn 拓扑排序判定）。
//
// 返回的错误均可用 errors.Is 区分为 ErrEmptyWorkflow / ErrInvalidTask /
// ErrDuplicateTask / ErrDependencyNotFound / ErrCyclicDependency。
func Load(def WorkflowDef) (*Workflow, error) {
	if len(def.Tasks) == 0 {
		return nil, ErrEmptyWorkflow
	}

	tasks := make(map[string]TaskDef, len(def.Tasks))
	for _, t := range def.Tasks {
		if t.ID == "" {
			return nil, fmt.Errorf("%w: task with empty id at index %d", ErrInvalidTask, len(tasks))
		}
		if _, exists := tasks[t.ID]; exists {
			return nil, fmt.Errorf("%w: task %q", ErrDuplicateTask, t.ID)
		}
		tasks[t.ID] = t
	}

	indegree := make(map[string]int, len(tasks))
	dependents := make(map[string][]string, len(tasks))
	for id := range tasks {
		indegree[id] = 0
	}
	for _, t := range def.Tasks {
		seen := make(map[string]struct{}, len(t.Deps))
		for _, dep := range t.Deps {
			if _, ok := tasks[dep]; !ok {
				return nil, fmt.Errorf("%w: task %q depends on %q", ErrDependencyNotFound, t.ID, dep)
			}
			if dep == t.ID {
				return nil, fmt.Errorf("%w: task %q depends on itself", ErrCyclicDependency, t.ID)
			}
			if _, dup := seen[dep]; dup {
				// 重复列出的依赖不影响正确性，但去重后再计边，避免入度虚高。
				continue
			}
			seen[dep] = struct{}{}
			indegree[t.ID]++
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}

	// Kahn 算法：以定义顺序作为同入度节点的打破次序，保证结果可复现。
	queue := make([]string, 0, len(tasks))
	for _, t := range def.Tasks {
		if indegree[t.ID] == 0 {
			queue = append(queue, t.ID)
		}
	}
	order := make([]string, 0, len(tasks))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		// 放松出边：dependents[id] 已按定义顺序排列，
		// 因而拓扑结果在任何机器上都是确定可复现的。
		for _, down := range dependents[id] {
			indegree[down]--
			if indegree[down] == 0 {
				queue = append(queue, down)
			}
		}
	}
	if len(order) != len(tasks) {
		var cyclic []string
		for _, t := range def.Tasks {
			if indegree[t.ID] > 0 {
				cyclic = append(cyclic, t.ID)
			}
		}
		return nil, fmt.Errorf("%w: tasks participating in cycle %v", ErrCyclicDependency, cyclic)
	}

	return &Workflow{def: def, tasks: tasks, order: order}, nil
}

func uniqueDeps(deps []string) []string {
	seen := make(map[string]struct{}, len(deps))
	out := deps[:0:0]
	for _, d := range deps {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

// Name 返回工作流名称。
func (w *Workflow) Name() string { return w.def.Name }

// Task 返回指定任务定义。
func (w *Workflow) Task(id string) (TaskDef, bool) {
	t, ok := w.tasks[id]
	return t, ok
}

// OrderedTasks 按确定性拓扑顺序返回全部任务定义。
func (w *Workflow) OrderedTasks() []TaskDef {
	out := make([]TaskDef, 0, len(w.order))
	for _, id := range w.order {
		out = append(out, w.tasks[id])
	}
	return out
}

// Dependents 返回直接依赖某任务的任务 ID 列表（按定义顺序，去重）。
func (w *Workflow) Dependents(id string) []string {
	out := make([]string, 0)
	for _, t := range w.def.Tasks {
		for _, dep := range uniqueDeps(t.Deps) {
			if dep == id {
				out = append(out, t.ID)
				break
			}
		}
	}
	return out
}
