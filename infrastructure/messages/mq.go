package messages

import (
	"errors"
	"io/ioutil"
	"strings"

	kfklib "github.com/opensourceways/kafka-lib/agent"
	kfkmq "github.com/opensourceways/kafka-lib/mq"
	redislib "github.com/opensourceways/redis-lib"
)

const (
	kfkQueueName      = "xihe-kafka-queue"
	kfkDefaultVersion = "2.3.0"
)

func InitKfkLib(kfkCfg kfklib.Config, log kfkmq.Logger) (err error) {
	return kfklib.Init(&kfkCfg, log, redislib.DAO(), kfkQueueName, true)
}

func KfkLibExit() {
	kfklib.Exit()
}

func LoadKafkaConfig(file string) (cfg kfklib.Config, err error) {
	v, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}

	contentStr := string(v)
	kafkaAddress := extractKafkaAddress(contentStr)
	tlsCertPath := extractTlsCertPath(contentStr)

	if kafkaAddress == "" || tlsCertPath == "" {
		err = errors.New("missing address or tlsCert")

		return
	}

	cfg.Address = kafkaAddress
	cfg.Version = kfkDefaultVersion
	cfg.MQCert = tlsCertPath

	return
}

func extractKafkaAddress(content string) string {
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		if strings.Contains(line, "address") {
			kafkaAddress := strings.TrimSpace(strings.Split(line, ":")[1])
			return kafkaAddress
		}
	}

	return ""
}

func extractTlsCertPath(content string) string {
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		if strings.Contains(line, "mq_cert") {
			tlsCertPath := strings.TrimSpace(strings.Split(line, ":")[1])
			return tlsCertPath
		}
	}

	return ""
}
