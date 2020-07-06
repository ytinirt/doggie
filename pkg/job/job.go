package job

import (
    "github.com/ytinirt/doggie/pkg/log"
    "sync"
    "time"
    "github.com/robfig/cron"
    "fmt"
    "github.com/ytinirt/doggie/pkg/etcdclient"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

type Stats struct {
    scheduleCounter prometheus.Counter
    postponeCounter prometheus.Counter
    failCounter prometheus.Counter
    successCounter prometheus.Counter

    firstScheduleTime time.Time
    lastScheduleTime time.Time
    lastFailTime time.Time
    lastSuccessTime time.Time

    longestDuration time.Duration
    longestDurationTime time.Time
}

type Exec func() error

type Job struct {
    name string
    scope string
    schedule string // "*/ * * * *"
    exec Exec

    lock sync.Mutex
    running bool
    stats Stats
}

const (
    ClusterScope = "cluster"
    NodeScope = "node"
    jobStrBannerFormatMeta = "%%-%ds Scope   %%-%ds FirstSchedTime  LastSchedTime   LastFailTime    LastSuccTime    LongestDurTime  LongestDur"
    jobStrFormatMeta = "%%-%ds %%-7s %%-%ds %%-15s %%-15s %%-15s %%-15s %%-15s %%s"
    jobStrFormat = "name(%s) scope(%s) sched(%s) firstSchedTime(%s) lastSchedTime(%s) lastFailTime(%s) lastSuccTime(%s) longestDurTime(%s) longestDur(%s)"
)

var (
    jobsMutex sync.Mutex
    jobs = make(map[string]*Job)

    etcdClient *etcdclient.EtcdClient = nil
)

func Init(ec *etcdclient.EtcdClient) {
    etcdClient = ec
}

func initJobStats(j *Job) {
    labels := prometheus.Labels{"job": j.name, "scope": j.scope}

    j.stats.scheduleCounter = promauto.NewCounter(prometheus.CounterOpts{
        Name: "doggie_job_scheduled_total",
        Help: "The total scheduled number of job",
        ConstLabels: labels,
    })
    j.stats.postponeCounter = promauto.NewCounter(prometheus.CounterOpts{
        Name: "doggie_job_postponed_total",
        Help: "The total postponed number of job",
        ConstLabels: labels,
    })
    j.stats.failCounter = promauto.NewCounter(prometheus.CounterOpts{
        Name: "doggie_job_failed_total",
        Help: "The total failed number of job",
        ConstLabels: labels,
    })
    j.stats.successCounter = promauto.NewCounter(prometheus.CounterOpts{
        Name: "doggie_job_success_total",
        Help: "The total success number of job",
        ConstLabels: labels,
    })

    j.stats.firstScheduleTime = time.Time{}
    j.stats.lastScheduleTime = time.Time{}
    j.stats.lastFailTime = time.Time{}
    j.stats.lastSuccessTime = time.Time{}

    j.stats.longestDuration = 0
    j.stats.longestDurationTime = time.Time{}
}

func RegisterJob(name, scope, schedule string, exec Exec) {
    jobsMutex.Lock()
    defer jobsMutex.Unlock()

    if _, found := jobs[name]; found {
        log.Fatal("Job %s was register twice", name)
    }
    if (scope != ClusterScope) && (scope != NodeScope) {
        log.Fatal("Job %s invalid scope %s", name, scope)
    }
    if _, err := cron.Parse(schedule); err != nil {
        log.Fatal("Job %s invalid schedule %s, %v", name, schedule, err)
    }

    log.Info("Registered job %s", name)
    jobs[name] = &Job {
        name: name,
        scope: scope,
        schedule: schedule,
        exec: exec,

        lock: sync.Mutex{},
        running: false,
    }
    initJobStats(jobs[name])
}

func DumpAllJobs() {
    longestNameLen := 0
    longestSchedLen := 0
    for _, j := range jobs {
        nameLen := len(j.name)
        if nameLen > longestNameLen {
            longestNameLen = nameLen
        }
        schedLen := len(j.schedule)
        if schedLen > longestSchedLen {
            longestSchedLen = schedLen
        }
    }

    bannerFormat := fmt.Sprintf(jobStrBannerFormatMeta, longestNameLen, longestSchedLen)
    log.Raw(fmt.Sprintf(bannerFormat, "Name", "Schedule"))

    jobFormat := fmt.Sprintf(jobStrFormatMeta, longestNameLen, longestSchedLen)
    // FIXME: 此处未进行锁保护，读取的数据可能不一致
    for _, j := range jobs {
        out := fmt.Sprintf(jobFormat, j.name, j.scope, j.schedule,
            j.stats.firstScheduleTime.Format(time.Stamp),
            j.stats.lastScheduleTime.Format(time.Stamp),
            j.stats.lastFailTime.Format(time.Stamp),
            j.stats.lastSuccessTime.Format(time.Stamp),
            j.stats.longestDurationTime.Format(time.Stamp),
            j.stats.longestDuration.String(),
        )
        log.Raw(out)
    }
}

func GetAllJobs() (ret []*Job) {
    for _, v := range jobs {
        ret = append(ret, v)
    }

    return
}

func (j *Job) permitRun() bool {
    if j.IsClusterScope() {
        if etcdClient == nil {
            log.Bug("job %s (cluster scope) is not permitted running, while etcd client is nil", j.name)
            return false
        }

        if !etcdClient.IsLeader() {
            log.Debug("job %s (cluster scope) is not permitted running, etcd node is not leader now", j.name)
            return false
        }
    }

    return true
}

func (j *Job) IsClusterScope() bool {
    return j.scope == ClusterScope
}

func (j *Job) Name() string {
    return j.name
}

func (j *Job) ScheduleSpec() string {
    return j.schedule
}

func (j *Job) String() string {
    // FIXME: 此处未进行锁保护，读取的数据可能不一致

    return fmt.Sprintf(jobStrFormat, j.name, j.scope, j.schedule,
        j.stats.firstScheduleTime.Format(time.RFC3339),
        j.stats.lastScheduleTime.Format(time.RFC3339),
        j.stats.lastFailTime.Format(time.RFC3339),
        j.stats.lastSuccessTime.Format(time.RFC3339),
        j.stats.longestDurationTime.Format(time.RFC3339),
        j.stats.longestDuration.String(),
    )
}

func (j *Job) Run() {
    if !j.permitRun() {
        log.Info("job not permit running: %s", j.name)
        return
    }

    zeroTime := time.Time{}
    scheduleTime := time.Now()
    j.lock.Lock()
    if j.stats.firstScheduleTime == zeroTime {
        j.stats.firstScheduleTime = scheduleTime
    }
    j.stats.lastScheduleTime = scheduleTime
    j.stats.scheduleCounter.Inc()
    if j.running {
        j.stats.postponeCounter.Inc()
        j.lock.Unlock()
        log.Warn("job %s is running, postponed", j.name)
        return
    }

    j.running = true
    j.lock.Unlock()

    log.Info("%s job start run", j.name)
    start := time.Now()
    err := j.exec()
    end := time.Now()
    duration := end.Sub(start)
    if duration > j.stats.longestDuration {
        j.stats.longestDuration = duration
        j.stats.longestDurationTime = end
    }
    if err == nil {
        j.stats.successCounter.Inc()
        j.stats.lastSuccessTime = end
        log.Info("%s job finished", j.name)
    } else {
        j.stats.failCounter.Inc()
        j.stats.lastFailTime = end
        log.Info("%s job return error: %v", j.name, err)
    }

    j.lock.Lock()
    j.running = false
    j.lock.Unlock()
}
