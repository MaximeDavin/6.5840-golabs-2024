package kvsrv

type PutAppendArgs struct {
	Key   string
	Value string
	Id    int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
