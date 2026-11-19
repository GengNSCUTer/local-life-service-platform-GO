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
func (s *Service) RunWorkers(ctx context.Context, brokers []string, client *kafka.Client, transport *kafka.Transport,
	relayWorkers, consumerWorkers int) error {
	var workers sync.WaitGroup
	workers.Go(func() { s.measuredWorker("expiration", func() { s.expireReservations(ctx) }) })
	workers.Go(func() { s.measuredWorker("compensation", func() { s.reconcileResolutions(ctx) }) })
	if !s.retry(ctx, "provision topics", func(operation context.Context) error {
		return CreateTopics(operation, client)
	}) {
		workers.Wait()
		return nil
	}
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
	for i := 0; i < relayWorkers; i++ {
		workers.Go(func() { s.measuredWorker("relay", func() { s.relay(ctx, writer) }) })
	}
	for i := 0; i < consumerWorkers; i++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: orderTopic, GroupID: orderGroup,
			StartOffset: kafka.FirstOffset, MaxWait: 500 * time.Millisecond,
		})
		readers = append(readers, reader)
		workers.Go(func() { s.measuredWorker("consumer", func() { s.consume(ctx, reader, writer) }) })
	}
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: deadLetterTopic, GroupID: dlqGroup,
		StartOffset: kafka.FirstOffset, MaxWait: time.Second,
	})
	workers.Go(func() { s.measuredWorker("dlq", func() { s.reconcile(ctx, dlqReader) }) })
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
			s.retryMetric("relay")
			s.logger.Warn("seckill relay will retry", "error", err)
			wait(ctx, time.Second)
		}
	}
}

func (s *Service) publishNext(ctx context.Context, writer *kafka.Writer) error {
	id, err := claimDue.Run(ctx, s.redis, []string{outboxKey}, 15000).Text()
	if err != nil {
		return err
	}
	_, payload, err := s.reservation(ctx, id)
	if errors.Is(err, errUnreadableReservation) {
		return s.retireUnreadableReservation(ctx, id)
	}
	if err != nil {
		return fmt.Errorf("load outbox event %s: %w", id, err)
	}
	started := time.Now()
	err = writer.WriteMessages(ctx, kafka.Message{Topic: orderTopic, Key: []byte(id), Value: payload})
	s.observe(started, "kafka_publish", operationOutcome(err))
	if err != nil {
		return fmt.Errorf("publish order %s: %w", id, err)
	}
	// If the broker accepted the event but removal fails, a later claim republishes it.
	return s.redis.ZRem(ctx, outboxKey, id).Err()
}

func (s *Service) expireReservations(ctx context.Context) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := s.expireNext(operation)
		cancel()
		switch {
		case err == nil:
			continue
		case errors.Is(err, redis.Nil):
			wait(ctx, 100*time.Millisecond)
		default:
			s.retryMetric("expiration")
			s.logger.Warn("seckill timeout worker will retry", "error", err)
			wait(ctx, time.Second)
		}
	}
}

func (s *Service) expireNext(ctx context.Context) error {
	id, err := claimDue.Run(ctx, s.redis, []string{timeoutKey}, 15000).Text()
	if err != nil {
		return err
	}
	event, _, err := s.reservation(ctx, id)
	if errors.Is(err, errUnreadableReservation) {
		return s.retireUnreadableReservation(ctx, id)
	}
	if err != nil {
		return err
	}
	order, err := s.cancelOrder(ctx, event, reasonPendingTimeout)
	if err != nil {
		return err
	}
	if order.Status == statusConfirmed {
		return s.finalize(ctx, id)
	}
	_, err = s.syncResolution(ctx, event)
	return err
}

func (s *Service) retireUnreadableReservation(ctx context.Context, id string) error {
	_, err := loadOrder(ctx, s.db, id)
	if errors.Is(err, errOrderNotFound) {
		if err := s.db.WithContext(ctx).Exec(`
			INSERT INTO seckill_reservation_anomalies (order_id) VALUES (?)
			ON DUPLICATE KEY UPDATE order_id = order_id
		`, id).Error; err != nil {
			return err
		}
		s.logger.Warn("unreadable Redis reservation requires manual review", "order_id", id)
	} else if err != nil {
		return err
	}
	// Remove the queue entries only after preserving the audit record or finding a stored order.
	return s.finalize(ctx, id)
}

func (s *Service) reconcileResolutions(ctx context.Context) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		events, err := s.pendingResolutions(operation, 20)
		cancel()
		if err != nil {
			s.logger.Warn("seckill compensation scan will retry", "error", err)
			wait(ctx, time.Second)
			continue
		}
		if len(events) == 0 {
			wait(ctx, 500*time.Millisecond)
			continue
		}
		for _, event := range events {
			if ctx.Err() != nil {
				return
			}
			operation, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			_, err := s.syncResolution(operation, event)
			cancel()
			if err != nil {
				s.logger.Warn("seckill Redis compensation will retry", "order_id", event.ID, "error", err)
			}
		}
		wait(ctx, 500*time.Millisecond)
	}
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
	reason := reasonInvalidEvent
	if err := json.Unmarshal(message.Value, &event); err == nil && event.valid(string(message.Key)) {
		started := time.Now()
		err := persistOrder(ctx, s.db, event)
		outcome := operationOutcome(err)
		switch {
		case errors.Is(err, errOrderConflict):
			outcome = reasonOrderConflict
		case errors.Is(err, errInventoryMismatch):
			outcome = reasonInventoryMismatch
		}
		s.observe(started, "mysql_persist", outcome)
		switch {
		case err == nil:
			return s.finalize(ctx, event.ID)
		case errors.Is(err, errOrderConflict):
			reason = reasonOrderConflict
		case errors.Is(err, errInventoryMismatch):
			reason = reasonInventoryMismatch
		default:
			return err
		}
	}
	// Offset commit waits for Kafka to confirm that the dead-letter record exists.
	started := time.Now()
	err := writer.WriteMessages(ctx, kafka.Message{
		Topic:   deadLetterTopic,
		Key:     []byte(fmt.Sprintf("%s/%d/%d", message.Topic, message.Partition, message.Offset)),
		Value:   message.Value,
		Headers: []kafka.Header{{Key: "reason", Value: []byte(reason)}},
	})
	outcome := reason
	if err != nil {
		outcome = "error"
	}
	s.observe(started, "kafka_dlq_publish", outcome)
	return err
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
	reason := reasonInvalidEvent
	for _, header := range message.Headers {
		if header.Key == "reason" {
			switch string(header.Value) {
			case reasonInventoryMismatch, reasonOrderConflict:
				reason = string(header.Value)
			}
			break
		}
	}
	var event orderEvent
	outcome := "manual_review"
	if err := json.Unmarshal(message.Value, &event); err == nil && event.valid(event.ID) && reason != reasonInvalidEvent {
		var err error
		outcome, err = s.resolveDeadLetterOrder(ctx, event, reason)
		if err != nil {
			return err
		}
	}
	started := time.Now()
	err := s.recordDeadLetter(ctx, message, event, reason, outcome)
	if err != nil {
		outcome = "error"
	}
	s.observe(started, "mysql_dlq_audit", outcome)
	return err
}

func (s *Service) resolveDeadLetterOrder(ctx context.Context, event orderEvent, reason string) (string, error) {
	var activity Activity
	err := s.db.WithContext(ctx).Table("seckill_activities").Select("id").Take(&activity, "id = ?", event.VoucherID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "manual_review", nil
	}
	if err != nil {
		return "", err
	}
	order, err := s.cancelOrder(ctx, event, reason)
	if errors.Is(err, errOrderNotFound) || errors.Is(err, errOrderState) {
		return "manual_review", nil
	}
	if err != nil {
		return "", err
	}
	if order.Status == statusConfirmed {
		return "already_persisted", s.finalize(ctx, event.ID)
	}
	appliedReason, err := s.syncResolution(ctx, event)
	if errors.Is(err, errReservationMismatch) {
		return "manual_review", nil
	}
	if err != nil {
		return "", err
	}
	if appliedReason == reasonInventoryMismatch {
		return "inventory_reconciled", nil
	}
	return "compensated", nil
}

func (s *Service) recordDeadLetter(ctx context.Context, message kafka.Message, event orderEvent, reason, outcome string) error {
	var orderID any
	if orderIDPattern.MatchString(event.ID) {
		orderID = event.ID
	}
	// Empty Kafka keys and tombstone payloads still need a durable audit row.
	return s.db.WithContext(ctx).Exec(`
		INSERT INTO seckill_dead_letters
			(dlq_partition, dlq_offset, source_key, order_id, voucher_id, user_id, reason, outcome, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE id = id
	`, message.Partition, message.Offset, string(message.Key[:min(len(message.Key), 255)]), orderID, nullableID(event.VoucherID),
		nullableID(event.UserID), reason, outcome, string(message.Value)).Error
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
		s.retryMetric(name)
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
