package main

import (
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// ConsumerOld 就版本的Consumer，使用 sarama-cluster"
// 使用旧的版本号
type ConsumerOld struct {
	opt *ConsumerOption
	cfg *cluster.Config
	cb  func(msg *sarama.ConsumerMessage)
}

func (c *ConsumerOld) Run() error {
	consumer, err := cluster.NewConsumer(c.opt.Brokers, c.opt.GroupId, c.opt.Topics, c.cfg)
	if err != nil {
		return err
	}

	for msg := range consumer.Messages() {
		c.cb(msg)
		consumer.MarkOffset(msg, "")
	}

	return nil
}

func (c *ConsumerOld) Init(opt *ConsumerOption) {
	c.opt = opt
	config := cluster.NewConfig()
	config.Consumer.Offsets.AutoCommit.Interval = time.Second
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version, _ = c.Version()

	if c.opt.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.User = c.opt.Username
		config.Net.SASL.Password = c.opt.Password
	}
	c.cfg = config
}

func (c *ConsumerOld) SetProcessor(cb func(msg *sarama.ConsumerMessage)) {
	c.cb = cb
}

func (c *ConsumerOld) Version() (sarama.KafkaVersion, error) {
	return sarama.ParseKafkaVersion("0.10.2.1")
}

func CreateConsumerOld() ConsumerInterface {
	return &ConsumerOld{}
}
