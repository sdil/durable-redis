package main

type Node struct {
	Role NodeRole
	IP  string
}

type NodeRole string

const (
	NodeRolePrimary NodeRole = "primary"
	NodeRoleReplica NodeRole = "replica"
)
