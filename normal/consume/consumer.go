package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// rocketmq普通模式消费消息
func main() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("testGroup1"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"10.2.171.20:9876"})),
	)
	err := c.Subscribe("goods-test", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {
			fmt.Printf("subscribe callback: %v \n", msgs[i])
			//todo gorm 连表 插入到es
			//todo 删除key
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	// Note: start after subscribe
	err = c.Start()
	select {}
}
