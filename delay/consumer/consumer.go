package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// rocketmq延迟模式消费消息
func main() {
	// 创建消费者
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("delay_message_consumer_group"),
		consumer.WithNameServer([]string{"10.2.171.20:9876"}),
	)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err.Error())
		return
	}

	// 注册消息监听器
	err = c.Subscribe("DelayTopic", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			fmt.Printf("Received message: %s\n", string(msg.Body))
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err.Error())
		return
	}

	// 启动消费者
	err = c.Start()
	if err != nil {
		fmt.Printf("Failed to start consumer: %s\n", err.Error())
		return
	}

	// 为了持续接收消息，让主线程阻塞
	select {}
}
