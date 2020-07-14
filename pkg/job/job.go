package job

import (
    "github.com/ytinirt/doggie/pkg/log"
    "sync"
    "time"
    "github.com/robfig/cron"
    "fmt"
    "path/filepath"
    "github.com/ytinirt/doggie/pkg/etcdclient"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

type Stats struct {
    scheduleCounter prometheus.Counter
    postponeCounter prometheus.Counter
    failCounter prometheus.Counter
    successCounter prometheus.Counter
    duration prometheus.Histogram

    firstScheduleTime time.Time
    lastScheduleTime time.Time
    lastFailTime time.Time
    lastSuccessTime time.Time

    longestDuration time.Duration
    longestDurationTime time.Time
}

type Exec func(confFile *string) error

type Opts struct {
    durationStart float64 // secs
    durationFactor float64
    durationCount int
}

type Job struct {
    name string
    scope string
    schedule string // "*/ * * * *"
    exec Exec

    confFile string
    opts Opts

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
    defaultDurationStart = 0.01 // secs
    defaultDurationFactor = 10
    defaultDurationCount = 8
    confFileSuffix = ".conf"
)

var (
    jobsMutex sync.Mutex
    jobs = make(map[string]*Job)

    confFileDir = ""
    etcdClient *etcdclient.EtcdClient = nil
)

func Init(path *string, ec *etcdclient.EtcdClient) {
    confFileDir = *path
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
    j.stats.duration = promauto.NewHistogram(prometheus.HistogramOpts{
        Name: "doggie_job_execution_duration",
        Help: "The duration of job execution",
        ConstLabels: labels,
        Buckets: prometheus.ExponentialBuckets(j.opts.durationStart, j.opts.durationFactor, j.opts.durationCount),
    })

    j.stats.firstScheduleTime = time.Time{}
    j.stats.lastScheduleTime = time.Time{}
    j.stats.lastFailTime = time.Time{}
    j.stats.lastSuccessTime = time.Time{}

    j.stats.longestDuration = 0
    j.stats.longestDurationTime = time.Time{}
}

func RegisterJob(name, scope, schedule string, exec Exec, opts Opts) {
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

    if opts.durationStart == 0 {
        opts.durationStart = defaultDurationStart
        log.Debug("using default duration start %f for job %s", defaultDurationStart, name)
    }
    if opts.durationFactor == 0 {
        opts.durationFactor = defaultDurationFactor
        log.Debug("using default duration factor %f for job %s", defaultDurationFactor, name)
    }
    if opts.durationCount == 0 {
        opts.durationCount = defaultDurationCount
        log.Debug("using default duration count %d for job %s", defaultDurationCount, name)
    }

    log.Info("Registered job %s", name)
    jobs[name] = &Job {
        name: name,
        scope: scope,
        schedule: schedule,
        exec: exec,

        confFile: "",
        opts: opts,

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
    log.Debug("scheduled running job: %s", j.name)

    if !j.permitRun() {
        log.Info("not permitted running job: %s", j.name)
        return
    }

    zeroTime := time.Time{}
    scheduleTime := time.Now()
    j.lock.Lock()
    if j.stats.firstScheduleTime == zeroTime {
        j.stats.firstScheduleTime = scheduleTime
        log.Debug("first scheduled job: %s @ %v", j.name, scheduleTime)
    }
    j.stats.lastScheduleTime = scheduleTime
    j.stats.scheduleCounter.Inc()
    if j.running {
        j.stats.postponeCounter.Inc()
        j.lock.Unlock()
        log.Warn("another is running, postponed job: %s", j.name)
        return
    }

    j.running = true
    j.lock.Unlock()

    log.Info("start running job: %s", j.name)
    start := time.Now()
    if j.confFile == "" && confFileDir != "" {
        // 只有在init()阶段之后，初始化confFileDir后，才能为job.confFile赋值
        // 否则confFileDir还未被初始化
        j.confFile = filepath.Join(confFileDir, j.name + confFileSuffix)
    }
    err := j.exec(&j.confFile)
    end := time.Now()
    duration := end.Sub(start)
    if duration > j.stats.longestDuration {
        j.stats.longestDuration = duration
        j.stats.longestDurationTime = end
    }
    if err == nil {
        j.stats.successCounter.Inc()
        j.stats.lastSuccessTime = end
        log.Info("finished job: %s", j.name)
    } else {
        j.stats.failCounter.Inc()
        j.stats.lastFailTime = end
        log.Info("failed job: %s, return error %v", j.name, err)
    }

    j.stats.duration.Observe(float64(duration / time.Second))

    j.lock.Lock()
    j.running = false
    j.lock.Unlock()
}
