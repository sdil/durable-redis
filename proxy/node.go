package main

type Node struct {
	role NodeRole
}

type NodeRole string

const (
	NodeRolePrimary NodeRole = "primary"
	NodeRoleReplica NodeRole = "replica"
)
