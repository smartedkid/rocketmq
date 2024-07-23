package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2/admin"     // 这个是连接mq服务器的
	"github.com/apache/rocketmq-client-go/v2/primitive" // 这个是定义消息体的
)

// 主函数 rocketmq 创建主题
func main() {
	topic := "newFour"
	//clusterName := "DefaultCluster"
	nameSrvAddr := []string{"10.2.171.20:9876"}
	brokerAddr := "10.2.171.20:10911"

	testAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	if err != nil {
		fmt.Println(err.Error())
	}

	//create topic
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topic),
		admin.WithBrokerAddrCreate(brokerAddr),
	)
	if err != nil {
		fmt.Println("Create topic error:", err.Error())
	}

}
