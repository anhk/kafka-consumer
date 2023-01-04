package main

import "github.com/Shopify/sarama"

type ConsumerInterface interface {
	SetProcessor(cb func(msg *sarama.ConsumerMessage))
	Init(c *ConsumerOption)
	Run() error
}

type ConsumerOption struct {
	Brokers  []string
	Topics   []string
	GroupId  string
	ClientId string

	Username string
	Password string
}
