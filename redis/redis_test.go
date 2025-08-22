package redis_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	goredis "github.com/redis/go-redis/v9"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/internal/testutils"
	"github.com/cloudchacho/hedwig-go/redis"
)

type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type fakeValidator struct {
	mock.Mock
}

type fakeLog struct {
	level   string
	err     error
	message string
	fields  []any
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(_ context.Context, err error, message string, keyvals ...any) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, keyvals})
}

func (f *fakeLogger) Debug(_ context.Context, message string, keyvals ...any) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, keyvals})
}

func (f *fakeValidator) Serialize(message *hedwig.Message) ([]byte, map[string]string, error) {
	args := f.Called(message)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
}

func (f *fakeValidator) Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata any) (*hedwig.Message, error) {
	args := f.Called(messagePayload, attributes, providerMetadata)
	return args.Get(0).(*hedwig.Message), args.Error(1)
}

func (s *BackendTestSuite) publish(payload []byte, attributes map[string]string, topic string) (string, error) {
	values := maps.Clone(attributes)
	values["hedwig_payload"] = string(payload)
	return s.client.XAdd(context.Background(), &goredis.XAddArgs{Stream: topic, Values: values}).Result()
}

func (s *BackendTestSuite) TestReceive() {
	numMessages := uint32(10)
	visibilityTimeout := time.Duration(0)

	_, err := s.publish(s.payload, s.attributes, "hedwig:dev-user-created-v1")
	s.Require().NoError(err)

	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo": "bar",
	}
	_, err = s.publish(payload2, attributes2, "hedwig:dev-user-created-v1")
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()

	m := mock.Mock{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for receivedMessage := range s.messageCh {
			m.MethodCalled("MessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	m.On("MessageReceived", s.payload, s.attributes, mock.AnythingOfType("redis.Metadata")).
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(2))
			s.Require().NoError(err)
		}).
		Return().
		Once()
	m.On("MessageReceived", payload2, attributes2, mock.AnythingOfType("redis.Metadata")).
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(2))
			s.Require().NoError(err)
			cancel()
		}).
		Return().
		Once().
		After(time.Millisecond * 50)

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.messageCh)
		s.ErrorIs(err, context.Canceled)
		close(s.messageCh)
	})
	wg.Wait()

	if m.AssertExpectations(s.T()) {
		providerMetadata := m.Calls[0].Arguments.Get(2).(redis.Metadata)
		s.Equal(1, providerMetadata.DeliveryAttempt)
		s.Equal("hedwig:dev-user-created-v1", providerMetadata.Stream)
	}
}

func (s *BackendTestSuite) TestReceiveNoMessages() {
	numMessages := uint32(10)
	visibilityTimeout := time.Duration(0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.messageCh)
		s.ErrorIs(err, context.DeadlineExceeded)
	})

	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestReceiveWithExpiredVisibilityTimeout() {
	s.settings.MaxDeliveryAttempts = 3
	s.settings.VisibilityTimeout = time.Second

	numMessages := uint32(10)
	visibilityTimeout := time.Duration(0)

	messageID, err := s.publish(s.payload, s.attributes, "hedwig:dev-user-created-v1")
	s.Require().NoError(err)

	dropCtx, dropCancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer dropCancel()

	dropMessageCh := make(chan hedwig.ReceivedMessage)

	dm := mock.Mock{}
	dropWg := &sync.WaitGroup{}
	dropWg.Add(1)
	go func() {
		defer dropWg.Done()
		for receivedMessage := range dropMessageCh {
			dm.MethodCalled("DropMessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	dm.On("DropMessageReceived", s.payload, s.attributes, mock.AnythingOfType("redis.Metadata")).
		Run(func(args mock.Arguments) {
			// do nothing
		}).
		Return().
		Once()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(dropCtx, numMessages, visibilityTimeout, dropMessageCh)
		s.ErrorIs(err, context.Canceled)
		close(dropMessageCh)
	})
	dropWg.Wait()

	if dm.AssertExpectations(s.T()) {
		providerMetadata := dm.Calls[0].Arguments.Get(2).(redis.Metadata)
		s.Equal(1, providerMetadata.DeliveryAttempt)
		s.Equal("hedwig:dev-user-created-v1", providerMetadata.Stream)
		s.Equal(messageID, providerMetadata.MessageID)

	}

	time.Sleep(1500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()

	m := mock.Mock{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for receivedMessage := range s.messageCh {
			m.MethodCalled("MessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	m.On("MessageReceived", s.payload, s.attributes, mock.AnythingOfType("redis.Metadata")).
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(2))
			s.Require().NoError(err)
		}).
		Return().
		Once()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.messageCh)
		s.True(err == context.Canceled)
		close(s.messageCh)
	})
	wg.Wait()

	if m.AssertExpectations(s.T()) {
		providerMetadata := m.Calls[0].Arguments.Get(2).(redis.Metadata)
		s.Equal(2, providerMetadata.DeliveryAttempt)
		s.Equal("hedwig:dev-user-created-v1", providerMetadata.Stream)
		s.Equal(messageID, providerMetadata.MessageID)
	}
}

func (s *BackendTestSuite) TestReceiveAndMoveToDLQ() {

}

func (s *BackendTestSuite) TestRequeueDLQ() {
	numMessages := uint32(10)
	numConcurrency := uint32(10)
	visibilityTimeout := time.Second * 10

	_, err := s.publish(s.payload, s.attributes, "hedwig:dev:myapp:dlq")
	s.Require().NoError(err)

	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"baz": "quux",
	}
	_, err = s.publish(payload2, attributes2, "hedwig:dev:myapp:dlq")
	s.Require().NoError(err)

	testutils.RunAndWait(func() {
		err = s.backend.RequeueDLQ(context.Background(), numMessages, visibilityTimeout, numConcurrency)
		s.NoError(err)
	})

	results, err := s.client.XReadGroup(
		context.Background(),
		&goredis.XReadGroupArgs{
			Group:    "dev:myapp",
			Consumer: uuid.NewString(),
			Streams:  []string{"hedwig:dev:myapp", ">"},
		},
	).Result()
	s.Require().NoError(err)

	s.Equal(len(results), 1)

	received := 0
	received2 := 0

	for _, message := range results[0].Messages {
		messagePayload, ok := message.Values["hedwig_payload"].(string)
		s.True(ok)

		var messageData []byte
		if encoding, ok := message.Values["hedwig_encoding"]; ok && encoding == "base64" {
			messageData, err = base64.StdEncoding.DecodeString(messagePayload)
			s.Require().NoError(err)
		} else {
			messageData = []byte(messagePayload)
		}

		messageAttributes := make(map[string]string, len(message.Values)-1)
		for key, value := range message.Values {
			if key != "hedwig_payload" {
				messageAttributes[key] = value.(string)
			}
		}

		if bytes.Equal(messageData, s.payload) {
			s.Equal(messageAttributes, s.attributes)
			received++
		} else {
			s.Equal(messageData, payload2)
			s.Equal(messageAttributes, attributes2)
			received2++
		}

		err := s.client.XAck(context.Background(), "hedwig:dev:myapp", "dev:myapp", message.ID).Err()
		s.NoError(err)
	}

	s.Equal(received, 1)
	s.Equal(received2, 1)
}

func (s *BackendTestSuite) TestRequeueDLQNoMessages() {
	numMessages := uint32(10)
	numConcurrency := uint32(10)
	visibilityTimeout := time.Second * 10

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout, numConcurrency)
		s.ErrorIs(err, context.DeadlineExceeded)
	})
}

func (s *BackendTestSuite) TestProcessMessageSuccess() {

}

func (s *BackendTestSuite) TestProcessMessageFailure() {

}

func (s *BackendTestSuite) TestPublish() {
	msgTopic := "dev-user-created-v1"

	messageID, err := s.backend.Publish(context.Background(), s.message, s.payload, s.attributes, msgTopic)
	s.NoError(err)
	s.NotEmpty(messageID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	results, err := s.client.XReadGroup(
		ctx,
		&goredis.XReadGroupArgs{
			Group:    "dev:myapp",
			Consumer: uuid.NewString(),
			Streams:  []string{"hedwig:dev-user-created-v1", ">"},
		},
	).Result()
	s.Require().NoError(err)

	s.Equal(len(results), 1)

	received := 0

	for _, message := range results[0].Messages {
		messagePayload, ok := message.Values["hedwig_payload"].(string)
		s.True(ok)

		messageData := []byte(messagePayload)

		messageAttributes := make(map[string]string, len(message.Values)-1)
		for key, value := range message.Values {
			if key != "hedwig_payload" {
				messageAttributes[key] = value.(string)
			}
		}

		if bytes.Equal(messageData, s.payload) {
			s.Equal(messageAttributes, s.attributes)
			received++
		}

		err := s.client.XAck(ctx, "hedwig:dev-user-created-v1", "dev:myapp", message.ID).Err()
		s.NoError(err)
	}

	s.Equal(received, 1)
}

func (s *BackendTestSuite) TestNack() {
	// This is a no-op

	messageID := "foobar"
	err := s.backend.NackMessage(context.Background(), redis.Metadata{MessageID: messageID})
	s.NoError(err)
}

func (s *BackendTestSuite) TestNew() {
	assert.NotNil(s.T(), s.backend)
}

type BackendTestSuite struct {
	suite.Suite
	backend       *redis.Backend
	client        *goredis.Client
	clientOptions *goredis.Options
	settings      redis.Settings
	message       *hedwig.Message
	payload       []byte
	attributes    map[string]string
	validator     *fakeValidator
	messageCh     chan hedwig.ReceivedMessage
}

func (s *BackendTestSuite) SetupSuite() {
	s.clientOptions = &goredis.Options{
		Addr: "localhost:6379",
		DB:   10,
	}

	ctx := context.Background()
	if s.client == nil {
		s.client = goredis.NewClient(s.clientOptions)
	}

	err := s.client.Ping(ctx).Err()
	s.Require().NoError(err)
}

func (s *BackendTestSuite) SetupTest() {
	ctx := context.Background()

	logger := &fakeLogger{}

	settings := redis.Settings{
		QueueName:           "dev:myapp",
		Subscriptions:       []string{"dev-user-created-v1"},
		RedisClientOptions:  s.clientOptions,
		MaxDeliveryAttempts: 1,
		VisibilityTimeout:   time.Second * 10,
	}

	group := settings.QueueName
	mainStream := fmt.Sprintf("hedwig:%s", settings.QueueName)
	deadLetterStream := fmt.Sprintf("hedwig:%s:dlq", settings.QueueName)

	pipe := s.client.Pipeline()

	for _, subscription := range settings.Subscriptions {
		stream := fmt.Sprintf("hedwig:%s", subscription)
		pipe.XGroupCreateMkStream(ctx, stream, group, "$")
	}

	pipe.XGroupCreateMkStream(ctx, mainStream, group, "$")
	pipe.XGroupCreateMkStream(ctx, deadLetterStream, group, "$")

	_, err := pipe.Exec(ctx)
	require.NoError(s.T(), err)

	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &fakeHedwigDataField{}, "myapp")
	require.NoError(s.T(), err)

	validator := &fakeValidator{}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{"foo": "bar"}

	s.backend = redis.NewBackend(settings, logger)
	s.settings = settings
	s.message = message
	s.validator = validator
	s.payload = payload
	s.attributes = attributes
	s.messageCh = make(chan hedwig.ReceivedMessage)
}

func (s *BackendTestSuite) TearDownTest() {
	ctx := context.Background()

	group := s.settings.QueueName
	mainStream := fmt.Sprintf("hedwig:%s", s.settings.QueueName)
	deadLetterStream := fmt.Sprintf("hedwig:%s:dlq", s.settings.QueueName)

	pipe := s.client.Pipeline()

	for _, subscription := range s.settings.Subscriptions {
		stream := fmt.Sprintf("hedwig:%s", subscription)
		pipe.XGroupDestroy(ctx, stream, group)
		pipe.Del(ctx, stream)
	}

	pipe.XGroupDestroy(ctx, mainStream, group)
	pipe.Del(ctx, mainStream)
	pipe.XGroupDestroy(ctx, deadLetterStream, group)
	pipe.Del(ctx, deadLetterStream)

	_, err := pipe.Exec(ctx)
	require.NoError(s.T(), err)
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, &BackendTestSuite{})
}
