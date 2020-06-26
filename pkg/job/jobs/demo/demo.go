package demo

import (
	"github.com/ytinirt/doggie/pkg/job"
	"math/rand"
	"github.com/ytinirt/doggie/pkg/log"
	"fmt"
	"time"
)

func init() {
	job.RegisterJob("demo-node-scope", job.NodeScope, "*/10 * * * *", func() (error) {
		log.Info("demo-node-scope job start")

		// 随机延迟0~19秒
		time.Sleep(time.Duration(rand.Intn(20)) * time.Second)

		if rand.Intn(10) < 7 {
			log.Info("demo-node-scope job success")
			return nil
		} else {
			log.Info("demo-node-scope job failed")
			return fmt.Errorf("demo-node-scope job failed")
		}
	})

	job.RegisterJob("demo-cluster-scope", job.ClusterScope, "*/23 * * * *", func() (error) {
		log.Info("demo-cluster-scope job start")

		// 随机延迟0~29秒
		time.Sleep(time.Duration(rand.Intn(30)) * time.Second)

		if rand.Intn(10) < 8 {
			log.Info("demo-cluster-scope job success")
			return nil
		} else {
			log.Info("demo-cluster-scope job failed")
			return fmt.Errorf("demo-cluster-scope job failed")
		}
	})
}
