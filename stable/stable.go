/*
  A partitioner assigns partitions within a consumer group like a
  consistent hash would. When group membership changes the least
  number of partitions are reassigned.

  I don't actually use a consistent hash because such algos take
  either a lot of CPU (the max(N-hash-functions) algo) or a lot
  of ram (the points-on-the-circle algo), or aren't applicable
  when members come and go (jump-hash).

  Instead I take advantage of the fact that I can know the current
  assignments of the remaining group members when constructing
  the new assignments. That makes keep the partitioning stable
  relatively easy to do:
    1) calculate an ideal load for each consumer (I assume equal
	   weight since I don't need anything more complicated...yet)
	2) remove excess partitions from overloaded consumers
	3) add the abandonned and excess partitions to the least loaded
	   consumers.

  Note that the state of the partition at time T depends on
  the history of the consumer group memberships from the time when
  there was 1 consumer until time T.

  Optionally the partitioner can be consistent across matching topics,
  so that the same partition in the matching topics is assigned to the
  same consumer.

  NOTE: the determination of "matching topics" is done by the heuristic
  that any topic with the same number of partitions and same set of
  consumers is matched. This is going to match more topics than necessary.
  So far this extra matching hasn't hurt. If it does then the exactly
  set of matching topics needs to be provided by each clients, and
  the partitioner needs to merge each client's matches to determine
  the actual set of matched topics. (This is necessary so that during
  upgrades, when the set of matched topics is inconsistent across clients,
  the partitioning is correct for everyone)

  Copyright 2016 MistSys
*/

package stable

import (
	"fmt"
	"sort"

	"github.com/Shopify/sarama"
)

// a partitioner that assigns partitions to consumers such that as consumers come and go the least number of partitions are reassigned
type stablePartitioner struct {
	consistent bool
	name       string
}

// New constructs a new stable partitioner.
// If consistent is true then the partitioning is consistent for all topics which have the same consumers and the same # of partitions.
// Otherwise each topic is partitioned independantly.
func New(consistent bool) *stablePartitioner {
	name := "stable"
	if consistent {
		name = "stable&consistent"
	}

	return &stablePartitioner{
		consistent: consistent,
		name:       name,
	}
}

// print a debug message
func dbgf(format string, args ...interface{}) {
	//log.Printf(format, args...)
}

func (sp *stablePartitioner) Name() string { return sp.name }

func (sp *stablePartitioner) PrepareJoin(jreq *sarama.JoinGroupRequest, topics []string, current_assignments map[string][]int32) {
	//fmt.Printf("stable.PrepareJoin(%q)\n", topics)
	// encode the current assignments in a manner proprietary to this partitioner
	var data = data{
		version:     1,
		assignments: current_assignments,
	}

	jreq.AddGroupProtocolMetadata(sp.name,
		&sarama.ConsumerGroupMemberMetadata{
			Version:  1,
			Topics:   topics,
			UserData: data.marshal(),
		})
}

// for each topic in jresp, assign the topic's partitions to the members requesting the topic
func (sp *stablePartitioner) Partition(sreq *sarama.SyncGroupRequest, jresp *sarama.JoinGroupResponse, client sarama.Client) error {
	if jresp.GroupProtocol != sp.name {
		return fmt.Errorf("sarama.JoinGroupResponse.GroupProtocol %q unexpected; expected %q", jresp.GroupProtocol, sp.name)
	}
	by_member, err := jresp.GetMembers() // map of member to ConsumerGroupMemberMetadata
	dbgf("by_member = %v", by_member)
	if err != nil {
		return err
	}

	// invert the data, so we have the requests grouped by topic (they arrived grouped by member, since the kafka broker treats the data from each consumer as an opaque blob, so it couldn't do this step for us)
	by_topic := make(map[string]map[string][]int32) // map of topic to members and members to current partition assignment
	for member, request := range by_member {
		if request.Version != 1 {
			// skip unsupported versions. we'll only assign to clients we can understand. Since we are such a client
			// we won't block all consumers (at least for those topics we consume). If this ends up a bad idea, we
			// can always change this code to return an error.
			continue
		}
		var data data
		err := data.unmarshal(request.UserData)
		if err != nil {
			return err
		}

		for _, topic := range request.Topics {
			members, ok := by_topic[topic]
			if !ok {
				members = make(map[string][]int32, len(by_member)) // capacity is a guess bases on the observation that members typically consume all topics
				by_topic[topic] = members
			}
			members[member] = data.assignments[topic] // NOTE: might be nil, which is OK. It just means the member wants to consume the partition but isn't doing so currently
		}
	}
	dbgf("by_topic = %v", by_topic)

	// make sure we have fresh metadata for all these topics
	if len(by_topic) != 0 {
		topics := make([]string, 0, len(by_topic))
		for t := range by_topic {
			topics = append(topics, t)
		}
		err = client.RefreshMetadata(topics...)
		if err != nil {
			return err
		}
	} // else asking for RefreshMetadata() would refresh all known topics, which is expensive and unnecessary

	// lookup the partitions in each topic. since we are asking for all partitions, not just the online ones, the numbering
	// appears to always be 0...N-1. But in case I don't understand kafka and there is some corner case where the numbering
	// of partitions is different I keep careful track of the exact numbers I've received.
	var partitions_by_topic = make(map[string]partitionslist, len(by_topic))
	for topic := range by_topic {
		// note: calls to client.Partitions() hit the metadata cache in the sarama client, so we don't gain much by asking concurrently
		partitions, err := client.Partitions(topic)
		if err != nil {
			// what to do? we could maybe skip the topic, assigning it to no-one. But I/O errors are likely to happen again.
			// so let's stop partitioning and return the error.
			return err
		}
		pl := make(partitionslist, len(partitions)) // we must make a copy before we sort the list
		copy(pl, partitions)
		sort.Sort(pl)
		partitions_by_topic[topic] = pl
	}
	dbgf("partitions_by_topic = %v", partitions_by_topic)

	// and compute the sorted set of members of each topic
	var members_by_topic = make(map[string]memberslist, len(by_topic))
	for topic, members := range by_topic {
		ml := make(memberslist, 0, len(members))
		for m := range members {
			ml = append(ml, m)
		}
		sort.Sort(ml)
		members_by_topic[topic] = ml
	}
	dbgf("members_by_topic = %v", members_by_topic)

	if sp.consistent {
		// I want topics with the same # of partitions and the same consumer group membership to result in the same partition assignments.
		// That way messages published under identical partition keys in those topics will all end up consumed by the same member.
		// So organize topics into groups which will be partitioned identically
		var matched_topics = make(map[string]string, len(by_topic)) // map from each topic to the 'master' topic with the same # of partitions and group membership. Topics which are unique are their own master
	topic_match_loop:
		for topic, members := range members_by_topic {
			// see if a match exists
			num_partitions := len(partitions_by_topic[topic])
			for t := range matched_topics { // TODO if # of topics gets large enough this shows up in the profiler, change this to some sort of a map lookup, rather than this O(N^2) search
				if num_partitions == len(partitions_by_topic[t]) && members.Equal(members_by_topic[t]) {
					// match; have topic 'topic' be partitioned the same way as topic 't'
					matched_topics[topic] = matched_topics[t]
					continue topic_match_loop
				}
			}
			// no existing topic matches this one, so it is its own match
			matched_topics[topic] = topic
		}
		dbgf("matched_topics = %v", matched_topics)

		// adjust the partitioning of each master topic in by_topic
		for topic, match := range matched_topics {
			if topic == match {
				adjust_partitioning(by_topic[topic], partitions_by_topic[topic])
			} // else it is not a master topic. once the master has been partitioned we'll simply copy the result
		}

		// set matched topics to the same assignment as their master
		for topic, match := range matched_topics {
			if topic != match {
				by_topic[topic] = by_topic[match]
			}
		}
	} else {
		// partition each topic independantly
		for topic, members := range by_topic {
			adjust_partitioning(members, partitions_by_topic[topic])
		}
	}

	// invert by_topic into the equivalent organized by member, and then by topic
	assignments := make(map[string]map[string][]int32, len(by_member)) // map of member to topics, and topic to partitions
	for topic, members := range by_topic {
		for member, partitions := range members {
			topics, ok := assignments[member]
			if !ok {
				topics = make(map[string][]int32, len(by_topic))
				assignments[member] = topics
			}
			topics[topic] = partitions
		}
	}
	dbgf("assignments = %v", assignments)

	// and encode the assignments in the sync request
	for member, topics := range assignments {
		sreq.AddGroupAssignmentMember(member,
			&sarama.ConsumerGroupMemberAssignment{
				Version: 1,
				Topics:  topics,
			})
	}

	return nil
}

func (*stablePartitioner) ParseSync(sresp *sarama.SyncGroupResponse) (map[string][]int32, error) {
	if len(sresp.MemberAssignment) == 0 {
		// in the corner case that we ask for no topics, we get nothing back. However sarama fd498173ae2bf (head of master branch Nov 6th 2016) will return a useless error if we call sresp.GetMemberAssignment() in this case
		return nil, nil
	}
	ma, err := sresp.GetMemberAssignment()
	dbgf("MemberAssignment %v", ma)
	if err != nil {
		return nil, err
	}
	if ma.Version != 1 {
		return nil, fmt.Errorf("unsupported MemberAssignment version %d", ma.Version)
	}
	return ma.Topics, nil
}

// ----------------------------------

// adjust_partitioning does the main work. it adjusts the partition assignment map it is passed in-place
func adjust_partitioning(assignment map[string][]int32, partitions partitionslist) {
	dbgf("adjust_partitioning(assignment = %v, partitions = %v)", assignment, partitions)
	num_members := len(assignment)
	dbgf("num_members = %v", num_members)
	if num_members == 0 {
		// no one wants this topic; stop now before we /0
		return
	}
	num_partitions := len(partitions)
	dbgf("num_partitions = %v", num_partitions)
	low := num_partitions / num_members                      // the minimum # of partitions each member should receive
	high := (num_partitions + num_members - 1) / num_members // the maximum # of partitions each member should receive
	dbgf("low = %v, high = %v", low, high)

	unassigned := make(map[int32]struct{}, len(partitions)) // set of unassigned partition ids
	claimed := make(map[int32]struct{}, len(partitions))    // set of initially claimed partition ids

	// initially all partitions are unassigned
	for _, p := range partitions {
		unassigned[p] = struct{}{}
	}
	dbgf("unassigned = %v", unassigned)

	// remove from each member's claim any partitions which do not exist
	// and any partitions which are already claimed by another member.
	// (these will happen only in corner cases where kafka is reconfigured,
	// or clients are confused/out of sync, but it is always a good idea
	// to check this for sanity before proceeding further)
	for m, a := range assignment {
		for _, p := range a {
			_, ok := unassigned[p]
			_, ok2 := claimed[p]
			if !ok || ok2 {
				// partition p no longer exists, or already is claimed; we need to filter this member's assignment carefully
				if ok2 {
					dbgf("partition %d claimed by %q is already claimed ", p, m)
				} else {
					dbgf("partition %d claimed by %q no longer exists", p, m)
				}
				a2 := make([]int32, 0, len(a)-1)
				for _, p := range a {
					_, ok := unassigned[p]
					_, ok2 := claimed[p]
					if ok && !ok2 {
						a2 = append(a2, p)
						claimed[p] = struct{}{}
					}
				}
				assignment[m] = a2
				break
			}
			claimed[p] = struct{}{}
		}
	}
	dbgf("assignment = %v", assignment)

	// let each member keep up to 'high' of its current assignment
	for m, a := range assignment {
		if len(a) > high {
			a = a[:high]
			assignment[m] = a
		}
		for _, p := range a {
			delete(unassigned, p)
		}
	}
	dbgf("assignment = %v", assignment)
	dbgf("unassigned = %v", unassigned)

	// assign the unassigned partitions to any member with < low partitions
	for p := range unassigned {
	unassigned_loop:
		for m, a := range assignment {
			if len(a) < low {
				a = append(a, p)
				assignment[m] = a
				delete(unassigned, p)
				break unassigned_loop
			}
		}
	}
	dbgf("assignment = %v", assignment)
	dbgf("unassigned = %v", unassigned)

	// take partitions from any member with > low partitions to give
	// to any member with < low partitions
	for m, a := range assignment {
	stealing_from_the_numerous:
		for len(a) < low {
			for m2, a2 := range assignment {
				n := len(a2)
				if n > low {
					// take the last partition from m2 and give it to m
					assignment[m2] = a2[:n-1]
					a = append(a, a2[n-1])
					assignment[m] = a
					continue stealing_from_the_numerous
				}
			}
		}
	}

	// and finally assign any remaining unassigned partitions to any member with < high partitions
	for p := range unassigned {
	assignment_loop:
		for m, a := range assignment {
			if len(a) < high {
				a = append(a, p)
				assignment[m] = a
				delete(unassigned, p) // not really necessary, since we don't reuse unassigned afterwards
				break assignment_loop
			}
		}
	}
	dbgf("assignment = %v", assignment)
	dbgf("unassigned = %v", unassigned)

	// and we're done
	dbgf("assignment = %v", assignment)
}

// ----------------------------------

// a sortable, comparible list of members
type memberslist []string // a list of the member ids

// implement sort.Interface
func (ml memberslist) Len() int           { return len(ml) }
func (ml memberslist) Swap(i, j int)      { ml[i], ml[j] = ml[j], ml[i] }
func (ml memberslist) Less(i, j int) bool { return ml[i] < ml[j] }

// and compare two sorted memberslist for equality
func (ml memberslist) Equal(ml2 memberslist) bool {
	if len(ml) != len(ml2) {
		return false
	}
	for i, m := range ml {
		if m != ml2[i] {
			return false
		}
	}
	return true
}

// ----------------------------------

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
