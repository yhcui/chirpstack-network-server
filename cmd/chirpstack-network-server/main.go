package main

import (
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/grpclog"

	"github.com/brocaar/chirpstack-network-server/v3/cmd/chirpstack-network-server/cmd"
)

// grpcLogger implements a wrapper around the logrus Logger to make it
// compatible with the grpc LoggerV2. It seems that V is not (always)
// called, therefore the Info* methods are overridden as we want to
// log these as debug info.
type grpcLogger struct {
	*log.Logger
}

func (gl *grpcLogger) V(l int) bool {
	level, ok := map[log.Level]int{
		log.DebugLevel: 0,
		log.InfoLevel:  1,
		log.WarnLevel:  2,
		log.ErrorLevel: 3,
		log.FatalLevel: 4,
	}[log.GetLevel()]
	if !ok {
		return false
	}

	return l >= level
}

func (gl *grpcLogger) Info(args ...interface{}) {
	if log.GetLevel() == log.DebugLevel {
		log.Debug(args...)
	}
}

func (gl *grpcLogger) Infoln(args ...interface{}) {
	if log.GetLevel() == log.DebugLevel {
		log.Debug(args...)
	}
}

func (gl *grpcLogger) Infof(format string, args ...interface{}) {
	if log.GetLevel() == log.DebugLevel {
		log.Debugf(format, args...)
	}
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	grpclog.SetLoggerV2(&grpcLogger{log.StandardLogger()})
}

var version string // set by the compiler

/*
一定要结合NS整个配置文件来看
https://www.chirpstack.io/network-server/install/config/

packet_forward的一个协议文档
https://github.com/Lora-net/packet_forwarder/blob/master/PROTOCOL.TXT

其它相关网址
LoraWAN规范				https://lora-alliance.org/lorawan-for-developers/
chirpstack作者github		https://github.com/brocaar
semtech lora			http://www.semtech.com/lora
lora-net				https://github.com/Lora-net
lora aliliance			https://lora-alliance.org
*/
func main() {
	cmd.Execute(version)
}
