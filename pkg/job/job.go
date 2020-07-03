package job

import (
    "github.com/ytinirt/doggie/pkg/log"
    "sync"
    "time"
    "github.com/robfig/cron"
    "fmt"
    "github.com/ytinirt/doggie/pkg/etcdclient"
)

type Stats struct {
    scheduleCount uint
    postponeCount uint
    failCount uint
    successCount uint

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
    jobStrBannerFormatMeta = "%%-%ds Scope   %%-%ds SchCnt PCnt FCnt SucCnt FirstSchedTime  LastSchedTime   LastFailTime    LastSuccTime    LongestDurTime  LongestDur"
    jobStrFormatMeta = "%%-%ds %%-7s %%-%ds %%-6d %%-4d %%-4d %%-6d %%-15s %%-15s %%-15s %%-15s %%-15s %%s"
    jobStrFormat = "name(%s) scope(%s) sched(%s) schCnt(%d) pCnt(%d) fCnt(%d) sucCnt(%d) firstSchedTime(%s) lastSchedTime(%s) lastFailTime(%s) lastSuccTime(%s) longestDurTime(%s) longestDur(%s)"
)

var (
    jobsMutex sync.Mutex
    jobs = make(map[string]*Job)

    etcdClient *etcdclient.EtcdClient = nil
)

func Init(ec *etcdclient.EtcdClient) {
    etcdClient = ec
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
        stats: Stats{},
    }
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
            j.stats.scheduleCount, j.stats.postponeCount, j.stats.failCount, j.stats.successCount,
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
        j.stats.scheduleCount, j.stats.postponeCount, j.stats.failCount, j.stats.successCount,
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
    j.stats.scheduleCount++
    if j.running {
        j.stats.postponeCount++
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
        j.stats.successCount++
        j.stats.lastSuccessTime = end
        log.Info("%s job finished", j.name)
    } else {
        j.stats.failCount++
        j.stats.lastFailTime = end
        log.Info("%s job return error: %v", j.name, err)
    }

    j.lock.Lock()
    j.running = false
    j.lock.Unlock()
}
