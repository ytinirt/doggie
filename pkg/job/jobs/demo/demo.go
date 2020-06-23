package demo

import (
	"github.com/ytinirt/doggie/pkg/job"
	"math/rand"
	"github.com/ytinirt/doggie/pkg/log"
	"fmt"
	"time"
)

const (
	JobName = "demo"
	defaultScheduleSpec = "*/10 * * * *"
	scope = job.NodeScope
)

func init() {
	job.RegisterJob(JobName, scope, defaultScheduleSpec, func() (error) {
		log.Info("demo job start")

		// 随机延迟0~19秒
		time.Sleep(time.Duration(rand.Intn(20)) * time.Second)

		if rand.Intn(10) < 7 {
			log.Info("demo job success")
			return nil
		} else {
			log.Info("demo job failed")
			return fmt.Errorf("demo job failed")
		}
	})
}
