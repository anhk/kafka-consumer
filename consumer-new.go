package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// ConsumerNew 新版本的Consumer，Sarama自带Group功能
// 使用新的版本号
type ConsumerNew struct {
	opt *ConsumerOption
	cfg *sarama.Config
	cb  func(msg *sarama.ConsumerMessage)
}

func (c *ConsumerNew) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerNew) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerNew) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		c.cb(msg)
		//fmt.Printf("%s - %s\n", msg.Key, msg.Value)
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *ConsumerNew) Run() error {
	client, err := sarama.NewConsumerGroup(c.opt.Brokers, c.opt.GroupId, c.cfg)
	if err != nil {
		return err
	}

	for {
		if err := client.Consume(context.Background(), c.opt.Topics, c); err != nil {
			fmt.Printf("consume failed: %v, sleep 5 seconds", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *ConsumerNew) Init(opt *ConsumerOption) {
	c.opt = opt
	config := sarama.NewConfig()
	config.Version, _ = c.Version()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if opt.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.User = opt.Username
		config.Net.SASL.Password = opt.Password
	}
	c.cfg = config
}

func (c *ConsumerNew) SetProcessor(cb func(msg *sarama.ConsumerMessage)) {
	c.cb = cb
}

func (c *ConsumerNew) Version() (sarama.KafkaVersion, error) {
	return sarama.ParseKafkaVersion("2.8.0")
}

func CreateConsumerNew() ConsumerInterface {
	return &ConsumerNew{}
}
