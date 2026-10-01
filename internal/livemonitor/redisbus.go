package livemonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBus keeps each stream's ring buffer in a sorted set scored by
// sequence number and fans events out over Redis pub/sub, so every
// live-monitor replica's viewers see every event whichever replica consumed
// it from Kafka.
type RedisBus struct {
	rdb      *redis.Client
	prefix   string
	capacity int
	ttl      time.Duration
}

// NewRedisBus keeps the newest capacity events per stream for ttl after the
// stream's last event.
func NewRedisBus(rdb *redis.Client, prefix string, capacity int, ttl time.Duration) *RedisBus {
	return &RedisBus{rdb: rdb, prefix: prefix, capacity: capacity, ttl: ttl}
}

// Keys share a {company:interview} hash tag so a stream's ring and channel
// land on one Redis Cluster slot.
func (b *RedisBus) ringKey(s Stream) string {
	return b.prefix + "{" + s.CompanyID.String() + ":" + s.InterviewID.String() + "}:ring"
}

func (b *RedisBus) channel(s Stream) string {
	return b.prefix + "{" + s.CompanyID.String() + ":" + s.InterviewID.String() + "}:events"
}

// publishScript appends to the ring unless its sequence number is already
// buffered, trims the ring to capacity, refreshes its TTL, and publishes —
// atomically, so subscribers never see an event the ring lacks.
var publishScript = redis.NewScript(`
if redis.call("ZCOUNT", KEYS[1], ARGV[1], ARGV[1]) > 0 then
	return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
redis.call("PEXPIRE", KEYS[1], ARGV[4])
redis.call("PUBLISH", KEYS[2], ARGV[2])
return 1`)

func (b *RedisBus) Publish(ctx context.Context, stream Stream, event Event) (bool, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("live bus: encode event: %w", err)
	}
	added, err := publishScript.Run(ctx, b.rdb, []string{b.ringKey(stream), b.channel(stream)},
		event.SequenceNumber, data, b.capacity, b.ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("live bus: publish: %w", err)
	}
	return added == 1, nil
}

func (b *RedisBus) Recent(ctx context.Context, stream Stream, afterSeq int64) ([]Event, int64, error) {
	key := b.ringKey(stream)
	pipe := b.rdb.Pipeline()
	oldestCmd := pipe.ZRangeWithScores(ctx, key, 0, 0)
	rangeCmd := pipe.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: "(" + strconv.FormatInt(afterSeq, 10), Max: "+inf"})
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, 0, fmt.Errorf("live bus: read ring: %w", err)
	}
	var oldest int64
	if first := oldestCmd.Val(); len(first) > 0 {
		oldest = int64(first[0].Score)
	}
	raw := rangeCmd.Val()
	events := make([]Event, 0, len(raw))
	for _, member := range raw {
		var event Event
		if err := json.Unmarshal([]byte(member), &event); err != nil {
			return nil, 0, fmt.Errorf("live bus: decode ring entry: %w", err)
		}
		events = append(events, event)
	}
	return events, oldest, nil
}

// Subscribe returns once Redis has confirmed the subscription, so a caller
// that reads Recent afterwards cannot miss an event published in between.
func (b *RedisBus) Subscribe(ctx context.Context, stream Stream) (Subscription, error) {
	pubsub := b.rdb.Subscribe(ctx, b.channel(stream))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("live bus: subscribe: %w", err)
	}
	sub := &redisSubscription{pubsub: pubsub, ch: make(chan Event, subscriptionBuffer), done: make(chan struct{})}
	go sub.forward()
	return sub, nil
}

type redisSubscription struct {
	pubsub *redis.PubSub
	ch     chan Event
	done   chan struct{}
	once   sync.Once
}

func (s *redisSubscription) forward() {
	defer close(s.ch)
	messages := s.pubsub.Channel()
	for {
		select {
		case <-s.done:
			return
		case message, ok := <-messages:
			if !ok {
				return
			}
			var event Event
			if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
				continue
			}
			select {
			case s.ch <- event:
			case <-s.done:
				return
			default: // slow subscriber: the handler repairs the gap from the ring
			}
		}
	}
}

func (s *redisSubscription) Events() <-chan Event { return s.ch }

func (s *redisSubscription) Close() error {
	var err error
	s.once.Do(func() {
		close(s.done)
		err = s.pubsub.Close()
	})
	return err
}
