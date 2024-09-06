package configs

import (
	"os"
)

type Config struct {
	natsAddress       string
	magnetarAddress   string
	agentQueueAddress string
	quasarAddress     string
	oortAddress       string
	etcdAddress       string
	serverAddress     string
	webhooksAddress   string
	webhookUrl        string
	tokenKey          string
}

func (c *Config) NatsAddress() string {
	return c.natsAddress
}

func (c *Config) MagnetarAddress() string {
	return c.magnetarAddress
}

func (c *Config) AgentQueueAddress() string {
	return c.agentQueueAddress
}

func (c *Config) OortAddress() string {
	return c.oortAddress
}

func (c *Config) QuasarAddress() string {
	return c.quasarAddress
}

func (c *Config) EtcdAddress() string {
	return c.etcdAddress
}

func (c *Config) ServerAddress() string {
	return c.serverAddress
}

func (c *Config) WebhooksAddress() string {
	return c.webhooksAddress
}

func (c *Config) WebhookUrl() string {
	return c.webhookUrl
}

func (c *Config) TokenKey() string {
	return c.tokenKey
}

func NewFromEnv() (*Config, error) {
	return &Config{
		natsAddress:       os.Getenv("NATS_ADDRESS"),
		magnetarAddress:   os.Getenv("MAGNETAR_ADDRESS"),
		agentQueueAddress: os.Getenv("AGENT_QUEUE_ADDRESS"),
		quasarAddress:     os.Getenv("QUASAR_ADDRESS"),
		oortAddress:       os.Getenv("OORT_ADDRESS"),
		etcdAddress:       os.Getenv("ETCD_ADDRESS"),
		serverAddress:     os.Getenv("KUIPER_ADDRESS"),
		webhooksAddress:   os.Getenv("WEBHOOK_ADDRESS"),
		webhookUrl:        os.Getenv("WEBHOOK_URL"),
		tokenKey:          os.Getenv("SECRET_KEY"),
	}, nil
}
