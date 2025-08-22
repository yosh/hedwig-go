package redis

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/cloudchacho/hedwig-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type Backend struct {
	group string

	mainStream       string
	deadLetterStream string

	settings Settings
	logger   hedwig.Logger

	client *redis.Client
	once   sync.Once
}

var _ = hedwig.ConsumerBackend(&Backend{})
var _ = hedwig.PublisherBackend(&Backend{})

// Metadata is additional metadata associated with a message
type Metadata struct {
	// Redis Stream that was the source of this message
	Stream string

	// Redis message ID
	MessageID string

	// Number of delivery attempts
	DeliveryAttempt int
}

// Publish a message represented by the payload, with specified attributes to the specific topic
func (b *Backend) Publish(ctx context.Context, _ *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	if err := b.ensureClient(ctx); err != nil {
		return "", err
	}

	stream := "hedwig:" + topic

	values := maps.Clone(attributes)

	// Redis requires UTF-8 encoded strings
	if !utf8.Valid(payload) {
		values["hedwig_encoding"] = "base64"
		values["hedwig_payload"] = base64.StdEncoding.EncodeToString(payload)
	} else {
		values["hedwig_payload"] = string(payload)
	}

	messageID, err := b.client.XAdd(
		ctx,
		&redis.XAddArgs{
			Stream: stream,
			Values: values,
		},
	).Result()
	if err != nil {
		return "", errors.Wrap(err, "Failed to publish message to Redis")
	}
	return messageID, nil
}

func (b *Backend) processMessages(ctx context.Context, stream string, messages []redis.XMessage, messageCh chan<- hedwig.ReceivedMessage) error {
	for _, message := range messages {
		// manual tracking of delivery attempts
		results, err := b.client.XPendingExt(
			ctx,
			&redis.XPendingExtArgs{
				Group:  b.group,
				Stream: stream,
				Start:  message.ID,
				End:    message.ID,
				Count:  1,
			},
		).Result()
		if err != nil {
			return errors.Wrap(err, "Failed to get message metadata from Redis")
		}

		for _, result := range results {
			deliveryAttempt := int(result.RetryCount)

			if deliveryAttempt > b.settings.MaxDeliveryAttempts {
				_, err := b.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.XAdd(ctx, &redis.XAddArgs{Stream: b.deadLetterStream, Values: message.Values})
					pipe.XAck(ctx, stream, b.group, message.ID)
					return nil
				})
				if err != nil {
					return errors.Wrap(err, "Failed to requeue message onto DLQ")
				}
			}

			metadata := Metadata{
				Stream:          stream,
				MessageID:       message.ID,
				DeliveryAttempt: deliveryAttempt,
			}

			messagePayload, ok := message.Values["hedwig_payload"].(string)
			if !ok {
				b.logger.Error(
					ctx,
					err,
					"Missing message payload",
					"message_id",
					message.ID,
				)
				continue
			}

			var payload []byte
			if encoding, ok := message.Values["hedwig_encoding"]; ok && encoding == "base64" {
				payload, err = base64.StdEncoding.DecodeString(messagePayload)
				if err != nil {
					b.logger.Error(
						ctx,
						err,
						"Invalid message payload - couldn't decode using base64",
						"message_id",
						message.ID,
					)
					continue
				}
			} else {
				payload = []byte(messagePayload)
			}

			attributes := make(map[string]string, len(message.Values)-1)
			for key, value := range message.Values {
				if key != "hedwig_payload" {
					attributes[key] = value.(string)
				}
			}

			messageCh <- hedwig.ReceivedMessage{
				Payload:          payload,
				Attributes:       attributes,
				ProviderMetadata: metadata,
			}
		}
	}

	return nil
}

// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
func (b *Backend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- hedwig.ReceivedMessage) error {
	if b.settings.MaxDeliveryAttempts == 0 {
		return errors.New("MaxDeliveryAttempts must be set for Redis backend")
	}

	if b.settings.VisibilityTimeout == 0 {
		return errors.New("VisibilityTimeout must be set for Redis backend")
	}

	if visibilityTimeout == 0 {
		visibilityTimeout = b.settings.VisibilityTimeout
	}

	if err := b.ensureClient(ctx); err != nil {
		return err
	}

	consumerID := uuid.NewString()

	streams := []string{}
	for _, subscription := range b.settings.Subscriptions {
		streams = append(streams, "hedwig:"+subscription)
	}
	// main queue is for DLQ re-queued messages
	streams = append(streams, b.mainStream)

	xReadGroupStreams := append([]string{}, streams...)
	for range streams {
		xReadGroupStreams = append(xReadGroupStreams, ">")
	}

	receiveMessageCount := int64(numMessages)

	for {
		for _, stream := range streams {
			messages, _, err := b.client.XAutoClaim(
				ctx,
				&redis.XAutoClaimArgs{
					Group:    b.group,
					Stream:   stream,
					MinIdle:  visibilityTimeout,
					Start:    "0-0",
					Count:    receiveMessageCount,
					Consumer: consumerID,
				},
			).Result()
			if err != nil {
				return errors.Wrap(err, "Failed to claim timed out messages via XAUTOCLAIM")
			}
			if err := b.processMessages(ctx, stream, messages, messageCh); err != nil {
				return err
			}
		}

		results, err := b.client.XReadGroup(
			ctx,
			&redis.XReadGroupArgs{
				Group:    b.group,
				Consumer: consumerID,
				Streams:  xReadGroupStreams,
				Count:    receiveMessageCount,
				Block:    500 * time.Millisecond,
			},
		).Result()
		if err != nil && err != redis.Nil {
			return errors.Wrap(err, "Failed to read messages via XREADGROUP")
		}
		for _, xstream := range results {
			if err := b.processMessages(ctx, xstream.Stream, xstream.Messages, messageCh); err != nil {
				return err
			}
		}
	}
}

func (b *Backend) requeueMessages(ctx context.Context, messages []redis.XMessage) (int, error) {
	messageIDs := []string{}

	pipe := b.client.Pipeline()

	for _, message := range messages {
		messageIDs = append(messageIDs, message.ID)
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: b.mainStream, Values: message.Values})
	}

	pipe.XAck(ctx, b.deadLetterStream, b.group, messageIDs...)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "Failed to requeue DLQ messages")
	}

	return len(messageIDs), nil
}

// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
func (b *Backend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, numConcurrency uint32) error {
	if err := b.ensureClient(ctx); err != nil {
		return err
	}

	consumerID := uuid.NewString()

	receiveMessageCount := int64(numMessages)

	var numMessagesRequeued = 0

	for {
		messages, _, err := b.client.XAutoClaim(
			ctx,
			&redis.XAutoClaimArgs{
				Group:    b.group,
				Stream:   b.deadLetterStream,
				MinIdle:  0,
				Start:    "0-0",
				Count:    receiveMessageCount,
				Consumer: consumerID,
			},
		).Result()
		if err != nil {
			return errors.Wrap(err, "Failed to claim DLQ messages via XAUTOCLAIM")
		}

		if len(messages) == 0 {
			break
		}

		requeued, err := b.requeueMessages(ctx, messages)
		if err != nil {
			return err
		}

		numMessagesRequeued += requeued
		b.logger.Debug(ctx, "Re-queue DLQ progress", "num_messages", numMessagesRequeued)
	}

	xReadGroupStreams := []string{b.deadLetterStream, ">"}

	for {
		results, err := b.client.XReadGroup(
			ctx,
			&redis.XReadGroupArgs{
				Group:    b.group,
				Consumer: consumerID,
				Streams:  xReadGroupStreams,
				Count:    receiveMessageCount,
				Block:    500 * time.Millisecond,
			},
		).Result()
		if err != nil && err != redis.Nil {
			return errors.Wrap(err, "Failed to read DLQ messages via XREADGROUP")
		}

		if len(results) == 0 {
			break
		}

		messages := []redis.XMessage{}
		for _, xstream := range results {
			messages = append(messages, xstream.Messages...)
		}

		if len(messages) == 0 {
			break
		}

		requeued, err := b.requeueMessages(ctx, messages)
		if err != nil {
			return err
		}

		numMessagesRequeued += requeued
		b.logger.Debug(ctx, "Re-queue DLQ progress", "num_messages", numMessagesRequeued)
	}

	return nil
}

// NackMessage nacks a message on the queue
func (b *Backend) NackMessage(_ context.Context, _ any) error {
	// Not supported by Redis streams
	return nil
}

// AckMessage acknowledges a message on the queue
func (b *Backend) AckMessage(ctx context.Context, providerMetadata any) error {
	if err := b.ensureClient(ctx); err != nil {
		return err
	}

	metadata := providerMetadata.(Metadata)
	err := b.client.XAck(ctx, metadata.Stream, b.group, metadata.MessageID).Err()
	return errors.Wrap(err, "Failed to acknowledge message via XACK")
}

func (b *Backend) ensureClient(ctx context.Context) error {
	var err error
	b.once.Do(func() {
		b.client = redis.NewClient(b.settings.RedisClientOptions)
		err = b.client.Ping(ctx).Err()
		if err != nil {
			err = errors.Wrap(err, "Could not connect to Redis")
		}
	})
	return err
}

// Settings for Redis backend
type Settings struct {
	// Hedwig queue name. Exclude the `HEDWIG-` prefix
	QueueName string

	// Subscriptions is a list of all the Hedwig topics that the app is subscribed to (exclude the ``hedwig-`` prefix).
	Subscriptions []string

	// Options for redis.NewClient. Default is to connect to localhost:6379
	RedisClientOptions *redis.Options

	// Number of attempts to try to successfully process a message before it is put on the DLQ
	MaxDeliveryAttempts int

	// Amount of time allotted to process a message before a failure retry is attempted
	VisibilityTimeout time.Duration
}

func (b *Backend) initDefaults() {
	if b.settings.RedisClientOptions == nil {
		b.settings.RedisClientOptions = &redis.Options{}
	}
	if b.logger == nil {
		b.logger = &hedwig.StdLogger{}
	}
}

// NewBackend creates a Backend for publishing and consuming from Redis
// The provider metadata produced by this Backend will have concrete type: redis.Metadata
func NewBackend(settings Settings, logger hedwig.Logger) *Backend {
	group := settings.QueueName

	mainStream := fmt.Sprintf("hedwig:%s", settings.QueueName)
	deadLetterStream := fmt.Sprintf("hedwig:%s:dlq", settings.QueueName)

	b := &Backend{
		settings:         settings,
		group:            group,
		mainStream:       mainStream,
		deadLetterStream: deadLetterStream,
		logger:           logger,
	}
	b.initDefaults()
	return b
}
