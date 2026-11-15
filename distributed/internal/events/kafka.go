package events

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"time"
)

type PaymentEvent struct {
	PaymentID   string `json:"payment_id"`
	AmountCents int64  `json:"amount_cents"`
}

type Publisher struct{ Writer *kafka.Writer }

func NewPublisher(broker, topic string) *Publisher {
	return &Publisher{Writer: &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 10 * time.Millisecond}}
}
func (p *Publisher) Close() error { return p.Writer.Close() }
func (p *Publisher) Publish(ctx context.Context, id string, data []byte) error {
	var event PaymentEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	return p.Writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.PaymentID), Value: data, Headers: []kafka.Header{{Key: "event_id", Value: []byte(id)}}})
}
func Publish(ctx context.Context, broker, topic, id string, data []byte) error {
	p := NewPublisher(broker, topic)
	defer p.Close()
	return p.Publish(ctx, id, data)
}
func Decode(data []byte) (PaymentEvent, error) {
	var p PaymentEvent
	err := json.Unmarshal(data, &p)
	return p, err
}
