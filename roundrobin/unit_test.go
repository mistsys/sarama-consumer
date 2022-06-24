/*
  A simple kafka consumer-group client

  Copyright 2016 MistSys
*/

package roundrobin_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
	consumer "github.com/mistsys/sarama-consumer"
	"github.com/mistsys/sarama-consumer/roundrobin"
)

func TestRoundRobin(t *testing.T) {
	var rr consumer.Partitioner = roundrobin.RoundRobin

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
		},
	}

	// pretend to have 3 members, all asking for two topics
	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		jreqs[i].MemberId = fmt.Sprintf("member%d", i)
		jreqs[i].ProtocolType = "consumer"
		rr.PrepareJoin(&jreqs[i], []string{"topic1", "topic2"}, nil)

		t.Logf("JoinGroupRequests[%d] = %v\n", i, jreqs[i])
	}

	var jresp = sarama.JoinGroupResponse{
		GenerationId:  1,
		GroupProtocol: string(roundrobin.RoundRobin),
		Members:       make(map[string][]byte),
	}
	for i := range jreqs {
		for _, gp := range jreqs[i].OrderedGroupProtocols {
			if gp.Name == string(roundrobin.RoundRobin) {
				jresp.Members[jreqs[i].MemberId] = gp.Metadata
			}
		}
	}
	t.Logf("JoinGroupResponse = %v\n", jresp)

	var sreq = sarama.SyncGroupRequest{
		GroupId:      "group",
		GenerationId: 1,
		MemberId:     "member0",
	}
	err := rr.Partition(&sreq, &jresp, &mock_client)
	t.Logf("SyncGroupRequest = %v\n", sreq)
	if err != nil {
		t.Fatal(err)
	}

	// check the results assigned to each of the 3 consumers
	// since RR depends on hash order, and that changes, we can't be sure which result each consumer gets,
	// but there are only 3 possible results
	var expected = map[int]map[string][]int32{
		0: map[string][]int32{"topic1": []int32{0, 4, 7}, "topic2": []int32{0}},
		1: map[string][]int32{"topic1": []int32{1, 5}, "topic2": []int32{1}},
		2: map[string][]int32{"topic1": []int32{3, 6}},
	}

	for i := range jreqs {
		var sresp = sarama.SyncGroupResponse{
			MemberAssignment: sreq.GroupAssignments[jreqs[i].MemberId],
		}

		act, err := rr.ParseSync(&sresp)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s assignment %v\n", jreqs[i].MemberId, act)

		for i, ex := range expected {
			if reflect.DeepEqual(ex, act) {
				delete(expected, i)
				goto matched
			}
		}
		t.Errorf("Unexpected assignment %v\n(Expected one of %v)\n", act, expected)
	matched:
	}
}

// mock sarama.Client which implements the metadata API sufficiently for our unit test purposes
type mockClient struct {
	config     *sarama.Config
	partitions map[string][]int32
}

func (mc *mockClient) Config() *sarama.Config {
	return mc.config
}

func (mc *mockClient) Brokers() []*sarama.Broker            { return nil }
func (mc *mockClient) Broker(int32) (*sarama.Broker, error) { return nil, sarama.ErrBrokerNotFound }

func (mc *mockClient) Topics() ([]string, error) {
	var topics = make([]string, 0, len(mc.partitions))
	for t := range mc.partitions {
		topics = append(topics, t)
	}
	return topics, nil
}

func (mc *mockClient) Partitions(topic string) ([]int32, error) {
	if p, ok := mc.partitions[topic]; ok {
		return p, nil
	}
	return nil, sarama.ErrUnknownTopicOrPartition
}

func (mc *mockClient) WritablePartitions(topic string) ([]int32, error) {
	return mc.Partitions(topic)
}

func (*mockClient) Leader(topic string, part int32) (*sarama.Broker, error) {
	return nil, sarama.ErrBrokerNotAvailable
}
func (*mockClient) Replicas(topic string, part int32) ([]int32, error) {
	return nil, sarama.ErrNotEnoughReplicas
}
func (*mockClient) RefreshMetadata(topics ...string) error                        { return nil }
func (*mockClient) GetOffset(topic string, part int32, time int64) (int64, error) { return 0, nil }
func (*mockClient) Coordinator(group string) (*sarama.Broker, error) {
	return nil, sarama.ErrBrokerNotAvailable
}
func (*mockClient) RefreshCoordinator(group string) error { return nil }
func (*mockClient) Close() error                          { return nil }
func (*mockClient) Closed() bool                          { return false }
func (*mockClient) InSyncReplicas(string, int32) ([]int32, error) {
	return nil, sarama.ErrNotEnoughReplicas
}
func (*mockClient) Controller() (*sarama.Broker, error)                              { return nil, nil }
func (*mockClient) RefreshController() (*sarama.Broker, error)                       { return nil, nil }
func (*mockClient) InitProducerID() (*sarama.InitProducerIDResponse, error)          { return nil, nil }
func (*mockClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) { return nil, nil }
func (*mockClient) RefreshBrokers(addrs []string) error                              { return nil }
