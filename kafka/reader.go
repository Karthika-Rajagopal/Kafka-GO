package kafka

import (
	"context"
	"log"

	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
)

type Reader struct {
	Reader *kafkago.Reader
}

func NewKafkaReader() *Reader {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "user_ids",
		GroupID: "group",
	})

	return &Reader{
		Reader: reader,
	}
}

func (k *Reader) FetchMessage(ctx context.Context, messages chan<- kafkago.Message) error {  // It takes a context and a channel of Kafka messages as input parameters. It uses an infinite loop to fetch messages from the Kafka, and then selects either a context or the channel of messages
	for {
		message, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages <- message:
			log.Printf("message fetched and sent to a channel: %v \n", string(message.Value))
		}
	}
}

func (k *Reader) CommitMessages(ctx context.Context, messageCommitChan <-chan kafkago.Message) error {  // commits Kafka messages that have been read by the reader
	for {
		select {
		case <-ctx.Done():
		case msg := <-messageCommitChan:
			err := k.Reader.CommitMessages(ctx, msg)
			if err != nil {
				return errors.Wrap(err, "Reader.CommitMessages")
			}
			log.Printf("committed an msg: %v \n", string(msg.Value))
		}
	}
}
