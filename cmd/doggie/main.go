package main

import (
    "flag"
    "fmt"
    "os"
    "time"
    "os/signal"
    "syscall"
    "net/http"

    "github.com/robfig/cron"
    "github.com/ytinirt/doggie/pkg/job"
    _ "github.com/ytinirt/doggie/pkg/job/jobs"
    "github.com/ytinirt/doggie/pkg/log"
    "github.com/ytinirt/doggie/pkg/etcdclient"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
    etcdEndpoint := flag.String("etcd-endpoint", "", "etcd endpoint(e.g. https://127.0.0.1:2379)")
    etcdCACert := flag.String("etcd-ca-cert", "", "etcd server CA cert")
    etcdClientCert := flag.String("etcd-client-cert", "", "etcd client cert")
    etcdClientKey := flag.String("etcd-client-key", "", "etcd client key")
    debug := flag.Bool("debug", false, "debug")
    help := flag.Bool("help", false, "help")
    flag.Parse()

    if *help {
        fmt.Printf("Usage: %s [Options]\n\nOptions:\n", os.Args[0])
        flag.PrintDefaults()
        os.Exit(0)
    }

    log.Init(*debug)

    var ec *etcdclient.EtcdClient = nil
    if *etcdEndpoint != "" {
        var err error
        ec, err = etcdclient.New(*etcdEndpoint, *etcdCACert, *etcdClientCert, *etcdClientKey)
        if ec == nil || err != nil {
            log.Fatal("create etcd client failed, %v", err)
        }
    }

    c := cron.New()

    job.Init(ec)
    jobs := job.GetAllJobs()
    for _, j := range jobs {
        if j.IsClusterScope() && *etcdEndpoint == "" {
            log.Info("bypass cluster scope %s job on non-etcd node", j.Name())
            continue
        }

        c.AddJob(j.ScheduleSpec(), j)
        log.Info("added %s job (%s)", j.Name(), j.ScheduleSpec())
    }

    sigChan := make(chan os.Signal, 1)
    go func() {
        for sig := range sigChan {
            switch sig {
            case syscall.SIGUSR1:
                fallthrough
            case syscall.SIGUSR2:
                job.DumpAllJobs()
            }
        }
    }()
    signal.Notify(sigChan, syscall.SIGUSR1, syscall.SIGUSR2)

    c.Start()
    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(":2112", nil)

    for {
        time.Sleep(time.Hour)
    }
}
