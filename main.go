package main

import (
	"context"
	"log"

	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"e/kafka"
)

func main() {
	reader := kafka.NewKafkaReader()
	writer := kafka.NewKafkaWriter()

	ctx := context.Background()
	messages := make(chan kafkago.Message, 1000)
	messageCommitChan := make(chan kafkago.Message, 1000)

	g, ctx := errgroup.WithContext(ctx)  //errgroup is created to synchronize the execution of multiple goroutines

	g.Go(func() error {  //fetching message from the kafka
		return reader.FetchMessage(ctx, messages) //calls the FetchMessage method of the reader 
	})

	g.Go(func() error { //writing message to kafka
		return writer.WriteMessages(ctx, messages, messageCommitChan) //calls the writemessage method of writer
	})

	g.Go(func() error {  //commiting message to kafka
		return reader.CommitMessages(ctx, messageCommitChan) //calls the commitmessage method of the reader
	})

	err := g.Wait()  //Any error returned by any of the goroutines will be returned by the Wait method
	if err != nil {
		log.Fatalln(err)
	}
}
