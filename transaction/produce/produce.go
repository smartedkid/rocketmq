package main

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// MyTransactionListener 实现了事务监听器接口
type MyTransactionListener struct{}

// ExecuteLocalTransaction 执行本地事务逻辑
func (l *MyTransactionListener) ExecuteLocalTransaction(msg *primitive.Message) primitive.LocalTransactionState {
	fmt.Printf("Executing local transaction for message: %s\n", msg)

	// 模拟本地事务操作
	success := performLocalTransaction()

	if success {
		// 事务成功，返回 CommitMessageState
		return primitive.CommitMessageState
	} else {
		// 事务失败，返回 RollbackMessageState
		return primitive.RollbackMessageState
	}
}

// performLocalTransaction 模拟本地事务操作
func performLocalTransaction() bool {
	// 模拟事务操作
	//todo 本地事务一: 可以是api、grpc等远程调用 调用 or 数据库（mysql、redis、es、mongodb）操
	fmt.Println("========>1")
	fmt.Println("========>2")
	// 返回 true 表示事务成功，false 表示事务失败
	return true
}

// CheckLocalTransaction 检查本地事务状态
func (l *MyTransactionListener) CheckLocalTransaction(msg *primitive.MessageExt) primitive.LocalTransactionState {
	fmt.Printf("Checking local transaction state for message: %s\n", msg)

	// 这里根据实际业务逻辑判断事务的状态
	// 假设事务状态是已提交
	if isTransactionCommitted() {
		return primitive.CommitMessageState
	} else {
		return primitive.RollbackMessageState
	}
}

// isTransactionCommitted 模拟检查事务是否已提交
func isTransactionCommitted() bool {
	// 实际业务中应替换为具体的事务状态检查逻辑
	// 这里简单返回 true 表示事务已提交
	return true
}

// rocketmq事务生产消费消息
func main() {
	// 创建事务生产者
	p, err := rocketmq.NewTransactionProducer(
		&MyTransactionListener{},
		producer.WithNameServer([]string{"10.2.171.20:9876"}),
		producer.WithGroupName("transaction_producer_group"),
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

	// 创建事务消息
	msg := &primitive.Message{
		Topic: "TransactionTopic",
		Body:  []byte("Hello RocketMQ with transaction"),
	}

	// 发送事务消息
	res, err := p.SendMessageInTransaction(context.Background(), msg)
	if err != nil {
		fmt.Printf("Failed to send transactional message: %s\n", err.Error())
	} else {
		fmt.Printf("Transactional message sent successfully: result=%s\n", res.String())
	}

	// 模拟一些操作，等待事务检查
	time.Sleep(10 * time.Second)

	// 关闭生产者
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("Failed to shut down producer: %s\n", err.Error())
	}
}
