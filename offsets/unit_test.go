package offsets

import (
	"math"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestNoOlderThanNow(t *testing.T) {
	sa, ooor := NoOlderThan(0)
	cl := &fakeClient{t}

	o, err := sa("topic1", 0, 100, cl)
	t.Logf("StartingOffset(100) -> %d", o)
	if err != nil {
		t.Error(err)
	}
	if o != sarama.OffsetNewest {
		t.Error("unexpected", o)
	}

	o, err = ooor("topic1", 0, cl)
	t.Logf("OffsetOutOfRange() -> %d", o)
	if err != nil {
		t.Error(err)
	}
	if o != sarama.OffsetNewest {
		t.Error("unexpected", o)
	}
}

func TestNoOlderThan1Minute(t *testing.T) {
	sa, ooor := NoOlderThan(time.Minute)

	cl := &fakeClient{t}

	approx := time.Now().Add(-time.Minute).UnixNano()
	o, err := sa("topic1", 0, -1, cl)
	t.Logf("StartingOffset(-1) -> %d", o)
	if err != nil {
		t.Error(err)
	}
	if math.Abs(float64(o-approx)) > 1000000 {
		t.Error("unexpected", o)
	}

	approx = time.Now().Add(-time.Minute).UnixNano()
	o, err = sa("topic1", 0, 100, cl)
	t.Logf("StartingOffset(100) -> %d", o)
	if err != nil {
		t.Error(err)
	}
	if math.Abs(float64(o-approx)) > 1000000 {
		t.Error("unexpected", o)
	}

	approx = time.Now().UnixNano()
	o, err = sa("topic1", 0, approx, cl)
	t.Logf("StartingOffset(100) -> %d", o)
	if err != nil {
		t.Error(err)
	}
	if math.Abs(float64(o-approx)) > 1000000 {
		t.Error("unexpected", o)
	}

	approx = time.Now().Add(-time.Minute).UnixNano()
	o, err = ooor("topic1", 0, cl)
	t.Logf("OffsetOutOfRange() -> %d", o)
	if err != nil {
		t.Error(err)
	}
	if math.Abs(float64(o-approx)) > 1000000 {
		t.Error("unexpected", o)
	}
}

// a fake sarama.Client which maps time -> offsets using time
type fakeClient struct{ t *testing.T }

func (*fakeClient) Brokers() []*sarama.Broker                                        { return nil }
func (*fakeClient) Broker(int32) (*sarama.Broker, error)                             { return nil, sarama.ErrBrokerNotFound }
func (*fakeClient) Topics() ([]string, error)                                        { return nil, nil }
func (*fakeClient) Partitions(topic string) ([]int32, error)                         { return nil, nil }
func (*fakeClient) WritablePartitions(topic string) ([]int32, error)                 { return nil, nil }
func (*fakeClient) Leader(topic string, partitionID int32) (*sarama.Broker, error)   { return nil, nil }
func (*fakeClient) Replicas(topic string, partitionID int32) ([]int32, error)        { return nil, nil }
func (*fakeClient) RefreshMetadata(topics ...string) error                           { return nil }
func (*fakeClient) Coordinator(consumerGroup string) (*sarama.Broker, error)         { return nil, nil }
func (*fakeClient) RefreshCoordinator(consumerGroup string) error                    { return nil }
func (*fakeClient) Close() error                                                     { return nil }
func (*fakeClient) Closed() bool                                                     { return false }
func (*fakeClient) InSyncReplicas(string, int32) ([]int32, error)                    { return nil, nil }
func (*fakeClient) Controller() (*sarama.Broker, error)                              { return nil, nil }
func (*fakeClient) RefreshController() (*sarama.Broker, error)                       { return nil, nil }
func (*fakeClient) InitProducerID() (*sarama.InitProducerIDResponse, error)          { return nil, nil }
func (*fakeClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) { return nil, nil }
func (*fakeClient) RefreshBrokers(addrs []string) error                              { return nil }
func (*fakeClient) LeaderAndEpoch(topic string, partitionID int32) (*sarama.Broker, int32, error) {
	return nil, 0, nil
}
func (*fakeClient) LeastLoadedBroker() *sarama.Broker                        { return nil }
func (*fakeClient) RefreshTransactionCoordinator(transactionID string) error { return nil }
func (*fakeClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	return nil, nil
}

func (cl *fakeClient) GetOffset(topic string, partition int32, ts_msec int64) (int64, error) {
	offset := time.Unix(ts_msec/1000, (ts_msec%1000)*1000000).UnixNano()
	cl.t.Logf("GetOffset(%q,%d,%d) -> %d", topic, partition, ts_msec, offset)
	return offset, nil
}
func (*fakeClient) Config() *sarama.Config {
	cfg := sarama.NewConfig()
	return cfg
}
