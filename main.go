package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func usage() {
	flag.Usage()
	os.Exit(-1)
}

func main() {
	topics := flag.String("topics", "", "Kafka consumer's topics, separate by comma")
	groupId := flag.String("groupId", "", "Kafka consumer's group Id")
	brokers := flag.String("brokers", "", "Kafka Brokers, separate by comma")

	user := flag.String("user", "", "Kafka username")
	pass := flag.String("pass", "", "Kafka password")

	flag.Parse()

	if *topics == "" || *groupId == "" || *brokers == "" {
		usage()
	}

	sarama.Logger = log.New(os.Stdout, "[consumer]", log.LstdFlags) // 日志

	version, err := sarama.ParseKafkaVersion("2.8.0")
	PanicIfError(err)

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if *user != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.User = *user
		config.Net.SASL.Password = *pass
	}

	ctx, cancel := context.WithCancel(context.Background())

	client, err := sarama.NewConsumerGroup(strings.Split(*brokers, ","), *groupId, config)
	PanicIfError(err)

	go func() {
		for {
			if err := client.Consume(ctx, strings.Split(*topics, ","), &Consumer{}); err != nil {
				fmt.Printf("consume failed: %v, sleep 5 seconds", err)
				time.Sleep(5 * time.Second)
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	cancel()

	_ = client.Close()
}

type Consumer struct{}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("%s - %s\n", msg.Key, msg.Value)
		session.MarkMessage(msg, "")
	}
	return nil
}
