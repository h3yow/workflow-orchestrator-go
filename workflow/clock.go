package workflow

import "time"

// clockNow 是包级时间源，测试可替换以获得确定性时间戳。
var clockNow = time.Now

func timeNow() time.Time { return clockNow().UTC() }
