package proxy

// ReplicaState is the state of a node in the replica.
type ReplicaState string

const (
	// ReplicaStatePrimary indicates the node is a primary.
	ReplicaStatePrimary = ReplicaState("PRIMARY")

	// ReplicaStateSecondary indicates the node is a secondary.
	ReplicaStateSecondary = ReplicaState("SECONDARY")

	// ReplicaStateArbiter indicates the node is an arbiter.
	ReplicaStateArbiter = ReplicaState("ARBITER")
)
