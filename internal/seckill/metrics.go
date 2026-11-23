package seckill

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

const (
	orderGroup = "hmdp-seckill-orders-v1"
	dlqGroup   = "hmdp-seckill-dlq-v1"
)

type metrics struct {
	operations     *prometheus.HistogramVec
	retries        *prometheus.CounterVec
	workers        *prometheus.GaugeVec
	queues         *prometheus.GaugeVec
	shardQueues    *prometheus.GaugeVec
	shardDelay     *prometheus.GaugeVec
	shardUp        *prometheus.GaugeVec
	outboxDueDelay prometheus.Gauge
	lag            *prometheus.GaugeVec
	fresh          *prometheus.GaugeVec
	audits         *prometheus.CounterVec
}

func (s *Service) RegisterMetrics(reg prometheus.Registerer, withWorkers bool) {
	m := &metrics{
		operations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "hmdp_seckill_operation_duration_seconds", Help: "Seconds spent on each seckill operation, by outcome.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
		}, []string{"operation", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hmdp_seckill_retries_total", Help: "Failed worker attempts scheduled for retry.",
		}, []string{"operation"}),
		workers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_workers", Help: "Running seckill worker loops in this process.",
		}, []string{"worker"}),
		queues: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_queue_size", Help: "Number of reservations in the Redis outbox or timeout index.",
		}, []string{"queue"}),
		shardQueues: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_shard_queue_size", Help: "Number of reservations per Redis shard and queue.",
		}, []string{"shard", "queue"}),
		shardDelay: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_shard_outbox_due_delay_seconds", Help: "Seconds since the earliest relay due time per Redis shard.",
		}, []string{"shard"}),
		shardUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_shard_queue_sample_up", Help: "Whether the most recent Redis queue sample succeeded per shard.",
		}, []string{"shard"}),
		outboxDueDelay: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hmdp_seckill_outbox_due_delay_seconds", Help: "Seconds since the earliest current relay due time, including lease rescheduling.",
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_kafka_lag", Help: "Kafka end offset minus the next offset to consume, by group and partition.",
		}, []string{"group", "topic", "partition"}),
		fresh: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hmdp_seckill_monitor_last_success_timestamp_seconds", Help: "Unix time of last complete sample, zero before first success.",
		}, []string{"source"}),
		audits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hmdp_seckill_inventory_checks_total", Help: "Completed inventory checks by status.",
		}, []string{"status"}),
	}
	reg.MustRegister(m.operations)
	s.metrics = m
	if !withWorkers {
		return
	}
	reg.MustRegister(m.retries, m.workers, m.queues, m.shardQueues, m.shardDelay, m.shardUp, m.outboxDueDelay, m.lag, m.fresh, m.audits)
	for _, source := range []string{"redis", "kafka", "inventory"} {
		m.fresh.WithLabelValues(source).Set(0)
	}
	for _, status := range []string{auditConsistent, auditRepairable, auditManual, auditBlocked} {
		m.audits.WithLabelValues(status).Add(0)
	}
	m.operations.WithLabelValues("mysql_dlq_audit", "manual_review")
}

func (s *Service) observe(start time.Time, operation, outcome string) {
	if s.metrics != nil {
		s.metrics.operations.WithLabelValues(operation, outcome).Observe(time.Since(start).Seconds())
	}
}

func (s *Service) retryMetric(operation string) {
	if s.metrics != nil {
		s.metrics.retries.WithLabelValues(operation).Inc()
	}
}

func (s *Service) measuredWorker(name string, run func()) {
	if s.metrics != nil {
		active := s.metrics.workers.WithLabelValues(name)
		active.Inc()
		defer active.Dec()
	}
	run()
}

// RunMonitoring samples dependencies outside of the HTTP scrape path.
func (s *Service) RunMonitoring(ctx context.Context, client *kafka.Client, auditInterval time.Duration) {
	if s.metrics == nil {
		return
	}
	if auditInterval <= 0 {
		auditInterval = time.Minute
	}
	poll := time.NewTicker(10 * time.Second)
	defer poll.Stop()
	audit := time.NewTicker(auditInterval)
	defer audit.Stop()
	var cursor uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			s.sample(ctx, "redis", s.sampleQueues)
			s.sample(ctx, "kafka", func(ctx context.Context) error { return s.sampleLag(ctx, client) })
		case <-audit.C:
			s.sample(ctx, "inventory", func(ctx context.Context) error {
				next, err := s.auditBatch(ctx, cursor)
				cursor = next
				return err
			})
		}
	}
}

func (s *Service) sample(parent context.Context, source string, collect func(context.Context) error) {
	ctx, cancel := context.WithTimeout(parent, 4*time.Second)
	defer cancel()
	if err := collect(ctx); err != nil {
		if parent.Err() == nil {
			s.logger.Warn("seckill monitoring sample failed", "source", source, "error", err)
		}
		return
	}
	s.metrics.fresh.WithLabelValues(source).SetToCurrentTime()
}

func (s *Service) sampleQueues(ctx context.Context) error {
	var totalOutbox, totalPending int64
	var maxDelay float64
	var sampleErr error
	for _, shard := range s.routes() {
		label := strconv.Itoa(int(shard))
		operation, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		pipe := s.redisFor(shard).TxPipeline()
		outbox := pipe.ZCard(operation, shard.outbox())
		pending := pipe.ZCard(operation, shard.timeouts())
		earliest := pipe.ZRangeWithScores(operation, shard.outbox(), 0, 0)
		_, err := pipe.Exec(operation)
		cancel()
		if err != nil {
			s.metrics.shardUp.WithLabelValues(label).Set(0)
			sampleErr = errors.Join(sampleErr, fmt.Errorf("shard %s: %w", label, err))
			continue
		}
		var delay float64
		if entries := earliest.Val(); len(entries) > 0 {
			delay = float64(max(0, time.Now().UnixMilli()-int64(entries[0].Score))) / 1000
		}
		maxDelay = max(maxDelay, delay)
		totalOutbox += outbox.Val()
		totalPending += pending.Val()
		s.metrics.shardUp.WithLabelValues(label).Set(1)
		s.metrics.shardQueues.WithLabelValues(label, "outbox").Set(float64(outbox.Val()))
		s.metrics.shardQueues.WithLabelValues(label, "timeout_index").Set(float64(pending.Val()))
		s.metrics.shardDelay.WithLabelValues(label).Set(delay)
	}
	if sampleErr != nil {
		return sampleErr
	}
	s.metrics.outboxDueDelay.Set(maxDelay)
	s.metrics.queues.WithLabelValues("outbox").Set(float64(totalOutbox))
	s.metrics.queues.WithLabelValues("timeout_index").Set(float64(totalPending))
	return nil
}

func (s *Service) sampleLag(ctx context.Context, client *kafka.Client) error {
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{orderTopic, deadLetterTopic}})
	if err != nil {
		return err
	}
	requests := make(map[string][]kafka.OffsetRequest, 2)
	partitions := make(map[string][]int, 2)
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return topic.Error
		}
		for _, partition := range topic.Partitions {
			if partition.Error != nil {
				return partition.Error
			}
			requests[topic.Name] = append(requests[topic.Name], kafka.FirstOffsetOf(partition.ID), kafka.LastOffsetOf(partition.ID))
			partitions[topic.Name] = append(partitions[topic.Name], partition.ID)
		}
	}
	type lagSample struct {
		group, topic string
		partition    int
		lag          int64
	}
	var samples []lagSample
	for topic, group := range map[string]string{orderTopic: orderGroup, deadLetterTopic: dlqGroup} {
		if len(partitions[topic]) == 0 {
			return fmt.Errorf("no Kafka partitions for %s", topic)
		}
		// Read the committed offset first so consumption during sampling cannot run past the sampled end.
		committed, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			GroupID: group, Topics: map[string][]int{topic: partitions[topic]},
		})
		if err != nil {
			return err
		}
		if committed.Error != nil {
			return committed.Error
		}
		ends, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: requests[topic]}})
		if err != nil {
			return err
		}
		byPartition := make(map[int]int64, len(partitions[topic]))
		for _, partition := range committed.Topics[topic] {
			if partition.Error != nil {
				return partition.Error
			}
			byPartition[partition.Partition] = partition.CommittedOffset
		}
		if len(ends.Topics[topic]) != len(partitions[topic]) {
			return fmt.Errorf("incomplete Kafka end offsets for %s", topic)
		}
		for _, partition := range ends.Topics[topic] {
			if partition.Error != nil {
				return partition.Error
			}
			offset, ok := byPartition[partition.Partition]
			if !ok || partition.FirstOffset < 0 || partition.LastOffset < partition.FirstOffset {
				return fmt.Errorf("incomplete Kafka offsets for %s/%d", topic, partition.Partition)
			}
			samples = append(samples, lagSample{group, topic, partition.Partition,
				max(0, partition.LastOffset-max(partition.FirstOffset, offset))})
		}
	}
	for _, item := range samples {
		s.metrics.lag.WithLabelValues(item.group, item.topic, strconv.Itoa(item.partition)).Set(float64(item.lag))
	}
	return nil
}

func operationOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
