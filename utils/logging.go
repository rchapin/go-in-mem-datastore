package utils

import (
	"os"
	"strings"

	log "github.com/rchapin/rlog"
)

func SetupLogging(level string) {
	os.Setenv("RLOG_LOG_LEVEL", strings.ToUpper(level))
	os.Setenv("RLOG_CALLER_INFO", "1")
	os.Setenv("RLOG_TIME_FORMAT", "2006-01-02 15:04:05.000")
	os.Setenv("RLOG_LOG_STREAM", "STDOUT")
	log.UpdateEnv()
}
