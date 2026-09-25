package workflow

import (
	"errors"
	"fmt"
	"sort"
)

// 加载阶段三类可区分的错误原因。
var (
	// ErrDuplicateTask 工作流中存在重复定义的任务。
	ErrDuplicateTask = errors.New("duplicate task definition")
	// ErrMissingDependency 依赖指向了不存在的任务。
	ErrMissingDependency = errors.New("dependency points to missing task")
	// ErrCycleDetected 依赖图中存在环。
	ErrCycleDetected = errors.New("dependency cycle detected")
)

// ValidationError 包装校验错误，携带可区分的原因（Kind）与任务名。
type ValidationError struct {
	Kind     error
	TaskName string
	Message  string
}

func (e *ValidationError) Error() string { return e.Message }
func (e *ValidationError) Unwrap() error { return e.Kind }

func validationErrorf(kind error, task, format string, args ...any) *ValidationError {
	return &ValidationError{
		Kind:     kind,
		TaskName: task,
		Message:  fmt.Sprintf(format, args...),
	}
}

// loadAndValidate 加载并校验工作流定义：
//  1. 拒绝重复定义的任务；
//  2. 拒绝指向不存在任务的依赖；
//  3. 拒绝成环依赖（Kahn 拓扑排序）。
//
// 通过后返回按名称稳定排序的邻接表与拓扑序，保证调度可复现。
func loadAndValidate(def WorkflowDef) (*dag, error) {
	if def.Name == "" {
		return nil, validationErrorf(ErrDuplicateTask, "", "workflow name must not be empty")
	}
	d := &dag{
		name:     def.Name,
		tasks:    make(map[string]*TaskDef),
		deps:     make(map[string]map[string]struct{}),
		children: make(map[string]map[string]struct{}),
	}

	for i := range def.Tasks {
		t := def.Tasks[i]
		if t.Name == "" {
			return nil, validationErrorf(ErrDuplicateTask, "", "task name must not be empty")
		}
		if _, ok := d.tasks[t.Name]; ok {
			return nil, validationErrorf(ErrDuplicateTask, t.Name,
				"task %q is defined more than once", t.Name)
		}
		taskCopy := t
		d.tasks[t.Name] = &taskCopy
		d.deps[t.Name] = make(map[string]struct{})
		d.children[t.Name] = make(map[string]struct{})
	}

	for _, t := range def.Tasks {
		seen := make(map[string]struct{}, len(t.Dependencies))
		for _, dep := range t.Dependencies {
			if _, ok := d.tasks[dep]; !ok {
				return nil, validationErrorf(ErrMissingDependency, t.Name,
					"task %q depends on missing task %q", t.Name, dep)
			}
			if dep == t.Name {
				return nil, validationErrorf(ErrCycleDetected, t.Name,
					"task %q depends on itself", t.Name)
			}
			if _, dup := seen[dep]; dup {
				// 重复声明的同一条依赖不影响语义，去重即可，不视为错误。
				continue
			}
			seen[dep] = struct{}{}
			d.deps[t.Name][dep] = struct{}{}
			d.children[dep][t.Name] = struct{}{}
		}
	}

	// Kahn 算法：按名称排序保证同层处理顺序稳定。
	indegree := make(map[string]int, len(d.tasks))
	ready := make([]string, 0)
	for name := range d.tasks {
		indegree[name] = len(d.deps[name])
		if indegree[name] == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)

	for len(ready) > 0 {
		n := ready[0]
		ready = ready[1:]
		d.order = append(d.order, n)

		children := make([]string, 0, len(d.children[n]))
		for c := range d.children[n] {
			children = append(children, c)
		}
		sort.Strings(children)
		for _, c := range children {
			indegree[c]--
			if indegree[c] == 0 {
				ready = append(ready, c)
				sort.Strings(ready)
			}
		}
	}

	if len(d.order) != len(d.tasks) {
		var cyclic []string
		for name, deg := range indegree {
			if deg > 0 {
				cyclic = append(cyclic, name)
			}
		}
		sort.Strings(cyclic)
		return nil, validationErrorf(ErrCycleDetected, firstOrEmpty(cyclic),
			"dependency cycle detected involving tasks: %v", cyclic)
	}

	return d, nil
}

func firstOrEmpty(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	return xs[0]
}
