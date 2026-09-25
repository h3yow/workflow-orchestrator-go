package workflow

import "errors"

// 加载/校验阶段可区分的错误原因。
var (
	// ErrDuplicateTask 同一工作流内出现重复任务 ID。
	ErrDuplicateTask = errors.New("workflow: duplicate task definition")
	// ErrDependencyNotFound 任务的依赖指向不存在的任务。
	ErrDependencyNotFound = errors.New("workflow: dependency points to nonexistent task")
	// ErrCyclicDependency 依赖关系中存在环。
	ErrCyclicDependency = errors.New("workflow: cyclic dependency detected")
	// ErrEmptyWorkflow 工作流不包含任何任务。
	ErrEmptyWorkflow = errors.New("workflow: workflow has no tasks")
	// ErrInvalidTask 任务定义非法（如 ID 为空）。
	ErrInvalidTask = errors.New("workflow: invalid task definition")

	// ErrTaskNotFound 引用了工作流中不存在的任务。
	ErrTaskNotFound = errors.New("workflow: task not found")
	// ErrRunNotFound 存储中找不到指定的运行实例。
	ErrRunNotFound = errors.New("workflow: run not found")
)
