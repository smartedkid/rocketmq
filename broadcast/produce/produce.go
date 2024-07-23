package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// rocketmq广播模式生产消息
func main() {
	// 创建生产者
	p, _ := rocketmq.NewProducer(
		producer.WithNameServer([]string{"10.2.171.20:9876"}),
		producer.WithGroupName("broadcast_producer_group"),
	)

	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s\n", err.Error())
		return
	}

	// 创建消息
	msg := &primitive.Message{
		Topic: "BroadcastTopic",
		Body:  []byte("Hello RocketMQ Broadcast!"),
	}

	// 发送消息
	res, err := p.SendSync(context.Background(), msg)
	if err != nil {
		fmt.Printf("send message error: %s\n", err.Error())
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}

	// 关闭生产者
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s\n", err.Error())
	}
}
