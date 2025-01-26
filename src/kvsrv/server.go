package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	store   map[string]string
	history map[int64]int // We store the length of the value for every transaction
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if prevValueLength, ok := kv.history[args.Id]; ok {
		reply.Value = kv.store[args.Key][:prevValueLength]
	} else {
		reply.Value = kv.store[args.Key]
		kv.history[args.Id] = len(reply.Value)
		kv.store[args.Key] += args.Value

	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.history = make(map[int64]int)
	return kv
}
