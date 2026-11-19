package seckill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

const (
	orderTopic      = "hmdp.seckill.order.v1"
	deadLetterTopic = "hmdp.seckill.order.dlq.v1"
)

func CreateTopics(ctx context.Context, client *kafka.Client) error {
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: orderTopic, NumPartitions: 3, ReplicationFactor: -1},
		{Topic: deadLetterTopic, NumPartitions: 1, ReplicationFactor: -1},
	}})
	if err != nil {
		return err
	}
	for topic, topicErr := range response.Errors {
		if topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", topic, topicErr)
		}
	}
	return nil
}

// RunWorkers returns after all loops and their Kafka clients have stopped.
func (s *Service) RunWorkers(ctx context.Context, brokers []string, transport *kafka.Transport,
	relayWorkers, consumerWorkers int) error {
	if relayWorkers < 1 {
		relayWorkers = 1
	}
	if consumerWorkers < 1 {
		consumerWorkers = 1
	}
	writer := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Transport: transport, Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchSize: 64, BatchTimeout: 10 * time.Millisecond, MaxAttempts: 3,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
	}
	readers := make([]*kafka.Reader, 0, consumerWorkers)
	var workers sync.WaitGroup
	for i := 0; i < relayWorkers; i++ {
		workers.Go(func() { s.relay(ctx, writer) })
	}
	for i := 0; i < consumerWorkers; i++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: orderTopic, GroupID: "hmdp-seckill-orders-v1",
			StartOffset: kafka.FirstOffset, MaxWait: 500 * time.Millisecond,
		})
		readers = append(readers, reader)
		workers.Go(func() { s.consume(ctx, reader, writer) })
	}
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: deadLetterTopic, GroupID: "hmdp-seckill-dlq-v1",
		StartOffset: kafka.FirstOffset, MaxWait: time.Second,
	})
	workers.Go(func() { s.reconcile(ctx, dlqReader) })
	workers.Wait()
	closeErrs := []error{dlqReader.Close(), writer.Close()}
	for _, reader := range readers {
		closeErrs = append(closeErrs, reader.Close())
	}
	return errors.Join(closeErrs...)
}

func (s *Service) relay(ctx context.Context, writer *kafka.Writer) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := s.publishNext(operation, writer)
		cancel()
		switch {
		case err == nil:
			continue
		case errors.Is(err, redis.Nil):
			wait(ctx, 100*time.Millisecond)
		default:
			s.logger.Warn("seckill relay will retry", "error", err)
			wait(ctx, time.Second)
		}
	}
}

func (s *Service) publishNext(ctx context.Context, writer *kafka.Writer) error {
	id, err := claimOutbox.Run(ctx, s.redis, []string{outboxKey}, 15000).Text()
	if err != nil {
		return err
	}
	payload, err := s.redis.Get(ctx, orderKey(id)).Bytes()
	if err != nil {
		return fmt.Errorf("load outbox event %s: %w", id, err)
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Topic: orderTopic, Key: []byte(id), Value: payload}); err != nil {
		return fmt.Errorf("publish order %s: %w", id, err)
	}
	// If the broker accepted the event but removal fails, a later claim republishes it.
	return s.redis.ZRem(ctx, outboxKey, id).Err()
}

func (s *Service) consume(ctx context.Context, reader *kafka.Reader, writer *kafka.Writer) {
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("seckill fetch will retry", "error", err)
			wait(ctx, time.Second)
			continue
		}
		if !s.retry(ctx, "process order", func(operation context.Context) error {
			return s.processMessage(operation, writer, message)
		}) {
			return
		}
		// Reading and committing sequentially keeps a later offset from skipping this message.
		if !s.retry(ctx, "commit offset", func(operation context.Context) error {
			return reader.CommitMessages(operation, message)
		}) {
			return
		}
	}
}

func (s *Service) processMessage(ctx context.Context, writer *kafka.Writer, message kafka.Message) error {
	var event orderEvent
	reason := "invalid_order_event"
	if err := json.Unmarshal(message.Value, &event); err == nil && event.Version == 1 &&
		orderIDPattern.MatchString(event.ID) && string(message.Key) == event.ID &&
		event.VoucherID > 0 && event.UserID > 0 && !event.CreatedAt.IsZero() {
		err := persistOrder(ctx, s.db, event)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, errOrderConflict):
			reason = "order_conflict"
		case errors.Is(err, errInventoryMismatch):
			reason = "inventory_mismatch"
		default:
			return err
		}
	}
	// Offset commit waits for Kafka to confirm that the dead-letter record exists.
	return writer.WriteMessages(ctx, kafka.Message{
		Topic:   deadLetterTopic,
		Key:     []byte(fmt.Sprintf("%s/%d/%d", message.Topic, message.Partition, message.Offset)),
		Value:   message.Value,
		Headers: []kafka.Header{{Key: "reason", Value: []byte(reason)}},
	})
}

func (s *Service) reconcile(ctx context.Context, reader *kafka.Reader) {
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("seckill DLQ fetch will retry", "error", err)
			wait(ctx, time.Second)
			continue
		}
		if !s.retry(ctx, "reconcile dead letter", func(operation context.Context) error {
			return s.reconcileDeadLetter(operation, message)
		}) {
			return
		}
		if !s.retry(ctx, "commit DLQ offset", func(operation context.Context) error {
			return reader.CommitMessages(operation, message)
		}) {
			return
		}
	}
}

func (s *Service) reconcileDeadLetter(ctx context.Context, message kafka.Message) error {
	var event orderEvent
	reason := "invalid_order_event"
	for _, header := range message.Headers {
		if header.Key == "reason" {
			reason = string(header.Value)
			break
		}
	}
	if err := json.Unmarshal(message.Value, &event); err != nil || event.Version != 1 ||
		!orderIDPattern.MatchString(event.ID) || event.VoucherID == 0 || event.UserID == 0 {
		return s.recordDeadLetter(ctx, message, event, reason, "manual_review")
	}
	if reason != "order_conflict" {
		return s.recordDeadLetter(ctx, message, event, reason, "manual_review")
	}

	var existing Order
	err := s.db.WithContext(ctx).Table("seckill_orders").Take(&existing,
		"voucher_id = ? AND user_id = ?", event.VoucherID, event.UserID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return s.recordDeadLetter(ctx, message, event, reason, "manual_review")
	}
	if err != nil {
		return err
	}
	if existing.ID == event.ID {
		return s.recordDeadLetter(ctx, message, event, reason, "already_persisted")
	}
	if err := s.compensate(ctx, event); err != nil {
		return fmt.Errorf("restore reservation for order %s: %w", event.ID, err)
	}
	return s.recordDeadLetter(ctx, message, event, reason, "compensated")
}

func (s *Service) recordDeadLetter(ctx context.Context, message kafka.Message, event orderEvent, reason, outcome string) error {
	return s.db.WithContext(ctx).Exec(`
		INSERT INTO seckill_dead_letters
			(dlq_partition, dlq_offset, source_key, order_id, voucher_id, user_id, reason, outcome, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE outcome = VALUES(outcome), reason = VALUES(reason)
	`, message.Partition, message.Offset, message.Key, nullable(event.ID), nullableID(event.VoucherID),
		nullableID(event.UserID), reason, outcome, message.Value).Error
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableID(value uint64) any {
	if value == 0 {
		return nil
	}
	return value
}

func (s *Service) retry(ctx context.Context, name string, run func(context.Context) error) bool {
	for delay := 200 * time.Millisecond; ; delay = min(2*delay, 5*time.Second) {
		operation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := run(operation)
		cancel()
		if err == nil {
			return true
		}
		s.logger.Warn("seckill consumer will retry", "operation", name, "error", err)
		if !wait(ctx, delay) {
			return false
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
