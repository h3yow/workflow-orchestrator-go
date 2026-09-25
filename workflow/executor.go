package workflow

// Execute 返回任务对应的执行函数。
func (m MapExecutor) Execute(name string) (ExecuteFunc, bool) {
	fn, ok := m[name]
	return fn, ok
}

var _ Executor = MapExecutor{}
