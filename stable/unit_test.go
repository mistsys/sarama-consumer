/*
  A simple kafka consumer-group client

  Copyright 2016 MistSys
*/

package stable_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/kr/pretty"
	consumer "github.com/mistsys/sarama-consumer"
	"github.com/mistsys/sarama-consumer/stable"
)

type assignments map[string]map[string][]int32 // map of member to topic to the assigned list of partitions

// compare two assignments for logical equality. returns bool and a reason why
func (a assignments) Equal(b assignments) (bool, string) {
	// both must have the same per-topic data, but nil and {} are equivalent,
	// and the order of the partitions doesn't matter

	for member, topics := range a {
		if b_topics, ok := b[member]; ok {
			// compare each topic's assignments

			// first divide the topics into those with an empty list of partitions and those with a non-empty list
			var nonempty, empty []string
			for t, p := range topics {
				if len(p) != 0 {
					nonempty = append(nonempty, t)
				} else {
					empty = append(empty, t)
				}
			}

			// for each empty topic, check that b_topics is also empty. Note that nil and int32{} and missing entirely are treated as equivalently empty
			for _, t := range empty {
				if p, ok := b_topics[t]; ok {
					if len(p) != 0 {
						return false, fmt.Sprintf("member %q, topic %q mismatch", member, t)
					}
				} // else missing == empty assignment list
			}

			// for each non-empty topic, check that the partitions assigned match after sorting
			for _, t := range nonempty {
				pa := topics[t]
				if pb, ok := b_topics[t]; ok {
					pla := make(partitionslist, len(pa))
					plb := make(partitionslist, len(pb))
					copy(pla, pa)
					copy(plb, pb)
					sort.Sort(pla)
					sort.Sort(plb)
					if !pla.Equal(plb) {
						return false, fmt.Sprintf("member %q, topic %q mismatch", member, t)
					}
				} else {
					return false, fmt.Sprintf("member %q, topic %q missing", member, t)
				}
			}

		} else {
			return false, fmt.Sprintf("member %q is missing", member)
		}

	}
	if len(a) != len(b) {
		return false, fmt.Sprintf("unequal number of members (%d != %d)", len(a), len(b))
	}

	return true, ""
}

// test behavior when clients have no preexisting state
func TestGreenfield(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with no current assignments, asking for all the topics
	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		jreqs[i].MemberId = fmt.Sprintf("member%d", i)
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, nil)
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("assignment = %v", pretty.Sprint(a))
}

// test behavior when clients have a preexisting assignment which should not change at all
func TestStable(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with some preexisting assignments, asking for all the topics
	var preexisting = assignments{
		"member0": map[string][]int32{
			"topic1": []int32{0, 4},
			"topic2": []int32{1},
			"topic3": []int32{4, 5},
			"topic4": []int32{1},
		},
		"member1": map[string][]int32{
			"topic1": []int32{1, 5, 7},
			"topic2": nil,
			"topic3": []int32{3, 2},
			"topic4": []int32{2},
		},
		"member2": map[string][]int32{
			"topic1": []int32{3, 6},
			"topic2": []int32{0},
			"topic3": []int32{1, 0},
			"topic4": []int32{0},
		},
	}

	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		id := fmt.Sprintf("member%d", i)
		jreqs[i].MemberId = id
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, preexisting[id])
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("preexisting = %v", pretty.Sprint(preexisting))
	t.Logf("assignment = %v", pretty.Sprint(a))

	if eq, why := preexisting.Equal(a); !eq {
		t.Error("partitioning altered stable assignment:", why)
	}
}

// test behavior when clients have a preexisting assignment but one client "restarts"
func TestRestart(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with some preexisting assignments, asking for all the topics
	var preexisting = assignments{
		"member0": map[string][]int32{
			"topic1": []int32{0, 4},
			"topic2": []int32{1},
			"topic3": []int32{4, 5},
			"topic4": []int32{1},
		},
		"member1": nil, // pretend member1 just restarted
		"member2": map[string][]int32{
			"topic1": []int32{3, 5, 6},
			"topic2": []int32{0},
			"topic3": []int32{1, 0},
			"topic4": []int32{0},
		},
	}

	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		id := fmt.Sprintf("member%d", i)
		jreqs[i].MemberId = id
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, preexisting[id])
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("preexisting = %v", pretty.Sprint(preexisting))
	t.Logf("assignment = %v", pretty.Sprint(a))

	// insert the correct answer for member1
	preexisting["member1"] = map[string][]int32{
		"topic1": []int32{1, 7},
		"topic2": nil,
		"topic3": []int32{3, 2},
		"topic4": []int32{2},
	}
	t.Logf("expected = %v", pretty.Sprint(preexisting))

	// and compare
	if eq, why := preexisting.Equal(a); !eq {
		t.Error("partitioning altered stable assignment after a restart:", why)
	}
}

// test behavior when clients have a preexisting assignment which should change
func TestUnstable(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with some preexisting assignments, asking for all the topics
	var preexisting = assignments{
		"member0": map[string][]int32{
			"topic1": []int32{0, 7, 3, 4}, // too many partitions of topic1
			"topic2": []int32{},
			"topic3": []int32{5}, // partitions 1,3 and 4 are consumed by no one
			"topic4": []int32{1},
		},
		"member1": map[string][]int32{
			"topic1": []int32{1, 5},
			"topic2": nil,
			"topic3": []int32{2},
			"topic4": []int32{2},
		},
		"member2": map[string][]int32{
			"topic1": []int32{6},
			"topic2": []int32{0, 1}, // must give up one partition
			"topic3": []int32{0},
			"topic4": []int32{0},
		},
	}

	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		id := fmt.Sprintf("member%d", i)
		jreqs[i].MemberId = id
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, preexisting[id])
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("preexisting = %v", pretty.Sprint(preexisting))
	t.Logf("assignment = %v", pretty.Sprint(a))
}

// test behavior when clients have a preexisting assignments which should change together
func TestConsistent(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(true)

	topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1, 2, 3},
			"topic2": []int32{0, 1, 2, 3},
			"topic3": []int32{0, 1, 2, 3},
			"topic4": []int32{0, 1, 2, 3},
		},
	}

	// pretend to have 3 members, with some preexisting assignments, asking for all the topics
	var preexisting = assignments{
		"member0": map[string][]int32{
			"topic1": []int32{0, 7, 3, 4}, // too many partitions of topic1
			"topic2": []int32{},
			"topic3": []int32{5}, // partitions 1,3 and 4 are consumed by no one
			"topic4": []int32{1},
		},
		"member1": map[string][]int32{
			"topic1": []int32{1, 5},
			"topic2": nil,
			"topic3": []int32{2},
			"topic4": []int32{2},
		},
		"member2": map[string][]int32{
			"topic1": []int32{6},
			"topic2": []int32{0, 1}, // must give up one partition
			"topic3": []int32{0},
			"topic4": []int32{0},
		},
	}

	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		id := fmt.Sprintf("member%d", i)
		jreqs[i].MemberId = id
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, preexisting[id])
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("preexisting = %v", pretty.Sprint(preexisting))
	t.Logf("assignment = %v", pretty.Sprint(a))

	// verify the assignments are consistent across topics
	for m, aa := range a {
		var prev *partitionslist
		for topic, p := range aa {
			pp := partitionslist(p)
			sort.Sort(pp)
			if prev == nil {
				prev = &pp
			} else if !pp.Equal(*prev) {
				t.Errorf("inconsistent partition assignments in topic %q for member %q: %v and %v", topic, m, pp, *prev)
			}
		}
	}
}

// test behavior when clients have a preexisting assignment which overlap
func TestFalseClaims(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with some preexisting assignments, asking for all the topics
	var preexisting = assignments{
		"member0": map[string][]int32{
			"topic1": []int32{0, 1, 2, 6, 7, 3, 4}, // claim non-existant partition 2, as well as 1 and 6 which other clients claim too
			"topic2": []int32{0, 1},                // everyone claims all the partitions
			"topic3": []int32{5, 99},               // partition 99 does not exist; partitions 1,3 and 4 are consumed by no one
			"topic4": []int32{1, 2},                // more overlapping claims:
		},
		"member1": map[string][]int32{
			"topic1": []int32{1, 5},
			"topic2": []int32{0, 1},
			"topic3": []int32{2},
			"topic4": []int32{2, 0},
		},
		"member2": map[string][]int32{
			"topic1": []int32{6},
			"topic2": []int32{0, 1}, // must give up one partition
			"topic3": []int32{0},
			"topic4": []int32{0, 1},
		},
	}

	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		id := fmt.Sprintf("member%d", i)
		jreqs[i].MemberId = id
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, preexisting[id])
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("preexisting = %v", pretty.Sprint(preexisting))
	t.Logf("assignment = %v", pretty.Sprint(a))
}

// test behavior when there are no partitions
func TestNoPartitions(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	topics := []string{"topic1", "topic2"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{}, // no partitions at all
			"topic2": []int32{},
		},
	}

	// pretend to have 3 members, with no current assignments, asking for all the topics
	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		jreqs[i].MemberId = fmt.Sprintf("member%d", i)
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], topics, nil)
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("assignment = %v", pretty.Sprint(a))
}

// test behavior when clients request no topics at all
func TestNoTopics(t *testing.T) {
	var partitioner consumer.Partitioner = stable.New(false)

	no_topics := []string{}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with no current assignments, asking for all four topics
	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		jreqs[i].MemberId = fmt.Sprintf("member%d", i)
		jreqs[i].ProtocolType = "consumer"
		partitioner.PrepareJoin(&jreqs[i], no_topics, nil)
	}

	a := join_and_sync(jreqs[:], partitioner, &mock_client, t)
	t.Logf("assignment = %v", pretty.Sprint(a))
}

//-------------------------------------------------------------------------------------------------------

// join and sync and sanity check and return the resulting assignment
func join_and_sync(jreqs []sarama.JoinGroupRequest, partitioner consumer.Partitioner, client sarama.Client, t *testing.T) assignments {
	var jresp = sarama.JoinGroupResponse{
		GenerationId:  1,
		GroupProtocol: partitioner.Name(),
	}
	for i := range jreqs {
		for _, gp := range jreqs[i].OrderedGroupProtocols {
			if gp.Name == partitioner.Name() {
				jresp.Members = append(jresp.Members, sarama.GroupMember{
					MemberId:        jreqs[i].MemberId,
					GroupInstanceId: jreqs[i].GroupInstanceId,
					Metadata:        gp.Metadata,
				})
			}
		}
	}
	//t.Logf("JoinGroupResponse = %v\n", jresp)

	var sreq = sarama.SyncGroupRequest{
		GroupId:      "group",
		GenerationId: 1,
		MemberId:     "member0",
	}
	err := partitioner.Partition(&sreq, &jresp, client)
	//t.Logf("SyncGroupRequest = %v\n", sreq)
	if err != nil {
		t.Fatal(err)
	}

	return sanity_check(&sreq, partitioner, jreqs[:], t)
}

// sanity check a set of assignments for basic correctness
func sanity_check(sreq *sarama.SyncGroupRequest, partitioner consumer.Partitioner, jreqs []sarama.JoinGroupRequest, t *testing.T) assignments {

	assignments := make(assignments)    // map of member to topic to the assigned list of partitions
	topics := make(map[string]struct{}) // set of all topics in the response

	for i := range jreqs {
		id := jreqs[i].MemberId
		var sresp sarama.SyncGroupResponse
		for j := range sreq.GroupAssignments {
			if sreq.GroupAssignments[j].MemberId == id {
				sresp.MemberAssignment = sreq.GroupAssignments[j].Assignment
			}
		}

		act, err := partitioner.ParseSync(&sresp)
		if err != nil {
			t.Errorf("failed to parse SyncGroupRespons data: %v", err)
		}

		t.Logf("%s assignment %v\n", id, act)

		assignments[id] = act

		for t := range act {
			topics[t] = struct{}{}
		}
	}

	for topic := range topics {
		low := 1 << 30
		high := 0
		used := make(map[int32]string)
		for id, act := range assignments {
			a := act[topic]

			// keep track of the high and low # of partitions that have been assigned
			n := len(a)
			if low > n {
				low = n
			}
			if high < n {
				high = n
			}

			// and make sure no partition is assigned twice
			for id2, p := range a {
				if _, ok := used[p]; ok {
					t.Errorf("Partition %d of topic %q is assigned to multiple consumers, including %v and %v", p, topic, id, id2)
				}
				used[p] = id
			}
		}
		t.Logf("topic %q assigned partitions as %v", topic, used)
		if low != high && low != high-1 {
			t.Errorf("Partition assignment of topic %q is uneven. Some have %d and some have %d", topic, low, high)
		}
	}

	return assignments
}

// mock sarama.Client which implements the metadata API sufficiently for our unit test purposes
type mockClient struct {
	config     *sarama.Config
	partitions map[string][]int32
}

func (mc *mockClient) Brokers() []*sarama.Broker            { return nil }
func (mc *mockClient) Broker(int32) (*sarama.Broker, error) { return nil, sarama.ErrBrokerNotFound }

func (mc *mockClient) Config() *sarama.Config {
	return mc.config
}

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
func (*mockClient) LeaderAndEpoch(topic string, partitionID int32) (*sarama.Broker, int32, error) {
	return nil, 0, nil
}
func (*mockClient) LeastLoadedBroker() *sarama.Broker                        { return nil }
func (*mockClient) RefreshTransactionCoordinator(transactionID string) error { return nil }
func (*mockClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	return nil, nil
}

// a sortable, comparible list of partition ids
type partitionslist []int32 // a list of the partition ids

// implement sort.Interface
func (pl partitionslist) Len() int           { return len(pl) }
func (pl partitionslist) Swap(i, j int)      { pl[i], pl[j] = pl[j], pl[i] }
func (pl partitionslist) Less(i, j int) bool { return pl[i] < pl[j] }

// and compare two sorted partitionslist for equality
func (pl partitionslist) Equal(pl2 partitionslist) bool {
	if len(pl) != len(pl2) {
		return false
	}
	for i, m := range pl {
		if m != pl2[i] {
			return false
		}
	}
	return true
}
