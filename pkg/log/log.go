package log

import (
    "log"
    "fmt"
    "os"
)

var isDebug = true

func Init(debugging bool) {
    isDebug = debugging

    if isDebug {
        log.SetFlags(log.Lshortfile)
    } else {
        log.SetFlags(0)
    }
}

func Debug(format string, v ...interface{}) {
    if isDebug {
        log.Output(2, fmt.Sprintf("DBG: " + format, v...))
    }
}

func Raw(format string, v ...interface{}) {
    log.Output(2, fmt.Sprintf(format, v...))
}

func Info(format string, v ...interface{}) {
    log.Output(2, fmt.Sprintf("INF: " + format, v...))
}

func Warn(format string, v ...interface{}) {
    log.Output(2, fmt.Sprintf("WRN: " + format, v...))
}

func Error(format string, v ...interface{}) {
    log.Output(2, fmt.Sprintf("ERR: " + format, v...))
}

func Fatal(format string, v ...interface{}) {
    log.Output(2, fmt.Sprintf("FTL: " + format, v...))
    os.Exit(1)
}
