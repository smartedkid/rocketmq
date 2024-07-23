package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// rocketmq顺序模式消费消息
const (
	GroupName = "order-consumer-group"
)

func main() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group2"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"10.2.171.20:9876"})),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerOrder(true), // 开启顺序消费
	)

	err := c.Subscribe("sequence", consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range msgs {
				log.Printf("%s Body: %s", getBlank(m.GetShardingKey()), m.Body)
			}
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		fmt.Println("读取消息失败")
	}
	_ = c.Start()
	fmt.Println("开始读取消息 queue")
	select {}
}

func getBlank(s string) (rs string) {
	n, _ := strconv.Atoi(s)
	for i := 0; i < n; i++ {
		rs += `    `
	}
	return
}
