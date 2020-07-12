package demo

import (
	"github.com/ytinirt/doggie/pkg/job"
	"math/rand"
	"github.com/ytinirt/doggie/pkg/log"
	"fmt"
	"time"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Conf struct {
	MaxDelaySecs 	int	`yaml:"maxDelaySecs"`
	SuccessPercent 	int	`yaml:"successPercent"`
}

const (
	defaultDemoNodeScopeMaxDelay = 20
	defaultDemoNodeScopeSuccessPercent = 70

	defaultDemoClusterScopeMaxDelay = 30
	defaultDemoClusterScopeSuccessPercent = 80
)

func init() {
	job.RegisterJob("demo-node-scope", job.NodeScope, "*/10 * * * *", func(confFile *string) (error) {
		conf := Conf{MaxDelaySecs: 0, SuccessPercent: 0}
		yamlConf, err := ioutil.ReadFile(*confFile)
		if err == nil {
			err := yaml.Unmarshal(yamlConf, &conf)
			if err != nil {
				log.Error("Unmarshal configuration file failed: %s, %v", *confFile, err)
			}
		}
		if conf.MaxDelaySecs == 0 {
			conf.MaxDelaySecs = defaultDemoNodeScopeMaxDelay
			log.Debug("demo-node-scope using default maxDelaySecs: %d", conf.MaxDelaySecs)
		}
		if conf.SuccessPercent == 0 {
			conf.SuccessPercent = defaultDemoNodeScopeSuccessPercent
			log.Debug("demo-node-scope using default successPercent: %d", conf.SuccessPercent)
		}

		log.Info("demo-node-scope job start")

		time.Sleep(time.Duration(rand.Intn(conf.MaxDelaySecs)) * time.Second)

		if rand.Intn(100) < conf.SuccessPercent {
			log.Info("demo-node-scope job success")
			return nil
		} else {
			log.Info("demo-node-scope job failed")
			return fmt.Errorf("demo-node-scope job failed")
		}
	}, job.Opts{})

	job.RegisterJob("demo-cluster-scope", job.ClusterScope, "*/23 * * * *", func(confFile *string) (error) {
		conf := Conf{MaxDelaySecs: 0, SuccessPercent: 0}
		yamlConf, err := ioutil.ReadFile(*confFile)
		if err == nil {
			err := yaml.Unmarshal(yamlConf, &conf)
			if err != nil {
				log.Error("Unmarshal configuration file failed: %s, %v", *confFile, err)
			}
		}
		if conf.MaxDelaySecs == 0 {
			conf.MaxDelaySecs = defaultDemoClusterScopeMaxDelay
			log.Debug("demo-cluster-scope using default maxDelaySecs: %d", conf.MaxDelaySecs)
		}
		if conf.SuccessPercent == 0 {
			conf.SuccessPercent = defaultDemoClusterScopeSuccessPercent
			log.Debug("demo-cluster-scope using default successPercent: %d", conf.SuccessPercent)
		}

		log.Info("demo-cluster-scope job start")

		time.Sleep(time.Duration(rand.Intn(conf.MaxDelaySecs)) * time.Second)

		if rand.Intn(100) < conf.SuccessPercent {
			log.Info("demo-cluster-scope job success")
			return nil
		} else {
			log.Info("demo-cluster-scope job failed")
			return fmt.Errorf("demo-cluster-scope job failed")
		}
	}, job.Opts{})
}
