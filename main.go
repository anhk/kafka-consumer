package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

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

func If[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func main() {
	topics := flag.String("topics", "", "Kafka consumer's topics, separate by comma")
	brokers := flag.String("brokers", "", "Kafka Brokers, separate by comma")

	user := flag.String("user", "", "Kafka username")
	pass := flag.String("pass", "", "Kafka password")
	old := flag.Bool("old", false, "use old kafka version")

	flag.Parse()

	if *topics == "" || *brokers == "" {
		usage()
	}

	sarama.Logger = log.New(os.Stdout, "[consumer]", log.LstdFlags) // 日志

	consumer := If(*old, CreateConsumerOld(), CreateConsumerNew())
	consumer.Init(&ConsumerOption{
		Brokers:  strings.Split(*brokers, ","),
		Topics:   strings.Split(*topics, ","),
		GroupId:  RandomString(16),
		ClientId: RandomString(16),
		Username: *user,
		Password: *pass,
	})

	consumer.SetProcessor(callback)
	PanicIfError(consumer.Run())

	//ctx, cancel := context.WithCancel(context.Background())

	//client, err := sarama.NewConsumerGroup(strings.Split(*brokers, ","), *groupId, config)
	//PanicIfError(err)
	//
	//go func() {
	//	for {
	//		if err := client.Consume(ctx, strings.Split(*topics, ","), &Consumer{}); err != nil {
	//			fmt.Printf("consume failed: %v, sleep 5 seconds", err)
	//			time.Sleep(5 * time.Second)
	//		}
	//	}
	//}()

	//sigterm := make(chan os.Signal, 1)
	//signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	//<-sigterm

	//cancel()

	//_ = client.Close()
}

func callback(msg *sarama.ConsumerMessage) {
	fmt.Printf("%s - %s\n", msg.Key, msg.Value)
}

//type Consumer struct{}
//
//func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
//	return nil
//}
//
//func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
//	return nil
//}
//
//func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
//	for msg := range claim.Messages() {
//		fmt.Printf("%s - %s\n", msg.Key, msg.Value)
//		session.MarkMessage(msg, "")
//	}
//	return nil
//}
