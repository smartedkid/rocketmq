package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// rocketmq广播模式消费消息
func main() {
	// 创建消费者
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{"10.2.171.20:9876"}),
		consumer.WithGroupName("broadcast_consumer_group"),
		consumer.WithConsumerModel(consumer.BroadCasting),
	)

	// 订阅主题
	err := c.Subscribe("BroadcastTopic", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			fmt.Printf("subscribe callback: %s\n", msg.Body)
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Printf("subscribe error: %s\n", err.Error())
		return
	}

	// 启动消费者
	err = c.Start()
	if err != nil {
		fmt.Printf("start consumer error: %s\n", err.Error())
		return
	}

	fmt.Println("consumer started...")

	// 阻塞主线程
	select {}
}
