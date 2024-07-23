package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

// rocketmq顺序模式生产消息
func main() {
	rlog.SetLogLevel("warn") // error warn
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"10.2.171.20:9876"})),
		producer.WithRetry(2),
	)
	if err != nil {
		fmt.Printf("NewProducer  error: %s\n", err.Error())
		os.Exit(1)
	}
	err = p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s\n", err.Error())
		os.Exit(1)
	}
	// 能保证同一orderId下的消息是顺序的
	for i := 0; i < 3; i++ {
		orderId := strconv.Itoa(i)
		for j := 1; j < 5; j++ {
			msg := &primitive.Message{
				Topic: "sequence",
				Body:  []byte("订单: " + orderId + " 步骤: " + strconv.Itoa(j)),
			}
			msg.WithShardingKey(orderId)                   // *关键 用于分片
			_, err = p.SendSync(context.Background(), msg) // 不能用单向
			if err != nil {
				log.Printf("send message err: %s", err)
				continue
			}
		}
	}

	log.Println("发布任务")
}
