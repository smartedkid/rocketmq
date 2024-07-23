package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// rocketmq延迟模式生产消息
func main() {
	// 创建生产者
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{"10.2.171.20:9876"}),
		producer.WithGroupName("delay_message_producer_group"),
	)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err.Error())
		return
	}

	// 启动生产者
	err = p.Start()
	if err != nil {
		fmt.Printf("Failed to start producer: %s\n", err.Error())
		return
	}

	// 创建消息
	msg := &primitive.Message{
		Topic: "DelayTopic",
		Body:  []byte("Hello RocketMQ with delay"),
	}

	// 设置延时等级 (例如，延时等级3代表10秒后推送)
	// messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	msg.WithDelayTimeLevel(3)

	// 发送消息
	res, err := p.SendSync(context.Background(), msg)
	if err != nil {
		fmt.Printf("Failed to send message: %s\n", err.Error())
	} else {
		fmt.Printf("Message sent successfully: result=%s\n", res.String())
	}

	// 关闭生产者
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("Failed to shut down producer: %s\n", err.Error())
	}
}
