package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GET OpType = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type OpResult struct {
	Err   Err
	Value string
}
type ResultChan struct {
	ch        chan OpResult
	requestId int64
}
type RequestReply struct {
	RequestId int64
	Reply     OpResult
}
type SnapshotStatus struct {
	LastApplied   int
	DB            map[string]string
	RequestReplys map[int64]RequestReply
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db            map[string]string
	resultChans   map[int64]*ResultChan
	requestReplys map[int64]RequestReply //客户端的唯一指令和响应的对应map
	// storeInterface    store                       //数据库接口
	// replyChMap  map[int]chan ApplyNotifyMsg //某index的响应的chan
	lastApplied int //上一条应用的log的index,防止快照导致回退
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// 已执行
	if requestReply, ok := kv.requestReplys[args.ClientId]; ok && requestReply.RequestId >= args.RequestId {
		reply.Err = requestReply.Reply.Err
		reply.Value = requestReply.Reply.Value
		kv.mu.Unlock()
		return
	}
	op := Op{
		OpType:    GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("client[%d]: 获取Key[%s]失败,当前节点不是leader\n", args.ClientId, args.Key)
		kv.mu.Unlock()
		return
	}
	resultCh, ok := kv.resultChans[op.ClientId]
	if !ok {
		resultCh = &ResultChan{make(chan OpResult, 1), 0}
		kv.resultChans[op.ClientId] = resultCh
	}
	resultCh.requestId = args.RequestId
	if len(resultCh.ch) > 0 {
		<-resultCh.ch
	}
	kv.mu.Unlock()
	select {
	case replyMsg := <-resultCh.ch:
		reply.Err = replyMsg.Err
		reply.Value = replyMsg.Value
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if requestReply, ok := kv.requestReplys[args.ClientId]; ok && requestReply.RequestId == args.RequestId {
		reply.Err = requestReply.Reply.Err
		kv.mu.Unlock()
		return
	}
	op := Op{
		OpType:    PUT,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if args.Op == "Put" {
		op.OpType = PUT
	} else {
		op.OpType = APPEND
	}
	_, _, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	resultCh, ok := kv.resultChans[op.ClientId]
	// 有数据
	if !ok {
		resultCh = &ResultChan{make(chan OpResult, 1), 0}
		kv.resultChans[op.ClientId] = resultCh
	}
	resultCh.requestId = args.RequestId
	// 之前结果
	if len(resultCh.ch) > 0 {
		<-resultCh.ch
	}
	kv.mu.Unlock()
	select {
	case replyMsg := <-resultCh.ch:
		reply.Err = replyMsg.Err
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) applyMsgHandler() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()
			//当为合法命令时
			if applyMsg.CommandValid {
				kv.applyCommand(applyMsg.Command.(Op), applyMsg.CommandIndex)
				kv.lastApplied++
				kv.snapshot()
			} else if applyMsg.SnapshotValid {
				//当为合法快照时
				kv.applySnapshot(applyMsg.Snapshot)
			} else {
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyCommand(op Op, index int) {
	//TODO
	// 判断是否执行过了
	DPrintf("requestReply.requestId %d", kv.requestReplys[op.ClientId].RequestId)
	if requestReply, ok := kv.requestReplys[op.ClientId]; ok && requestReply.RequestId >= op.RequestId {
		// if resultCh, ok := kv.resultChans[int64(index)]; ok {
		// 	resultCh <- requestReply.reply
		// }

		return
	}
	DPrintf("client[%d]: 收到RPC;args=[%v]; server %d", op.ClientId, op, kv.me)
	result := OpResult{}
	switch op.OpType {
	case GET:
		if value, ok := kv.db[op.Key]; ok {
			result = OpResult{Err: OK, Value: value}
		} else {
			result = OpResult{Err: ErrNoKey}
		}
	case PUT:
		kv.db[op.Key] = op.Value
		result = OpResult{Err: OK}
	case APPEND:
		if _, ok := kv.db[op.Key]; !ok {
			kv.db[op.Key] = op.Value
		} else {
			kv.db[op.Key] += op.Value
		}
		result = OpResult{Err: OK}
	}
	kv.requestReplys[op.ClientId] = RequestReply{op.RequestId, result}
	DPrintf("requestReply.requestId %d", kv.requestReplys[op.ClientId].RequestId)
	// if resultCh, ok := kv.resultChans[op.ClientId]; ok && !resultCh.isStop {
	// 	resultCh.ch <- result
	// 	resultCh.isStop = true
	// }
	if resultCh, ok := kv.resultChans[op.ClientId]; ok && resultCh.requestId == op.RequestId {
		resultCh.ch <- result
	}
}

func (kv *KVServer) applySnapshot(snapShot []byte) {
	snapshotStatus := &SnapshotStatus{}
	if err := labgob.NewDecoder(bytes.NewBuffer(snapShot)).Decode(snapshotStatus); err != nil {
		DPrintf("snapshot gob decode snapshotStatus err:%v", err)
		return
	}
	if snapshotStatus.LastApplied < kv.lastApplied {
		DPrintf("snapshotStatus.LastApplied %d < kv.lastApplied %d", snapshotStatus.LastApplied, kv.lastApplied)
		return
	}
	kv.db = snapshotStatus.DB
	kv.requestReplys = snapshotStatus.RequestReplys
	kv.lastApplied = snapshotStatus.LastApplied
}

func (kv *KVServer) snapshot() {
	if kv.maxraftstate == -1 {
		return
	}
	rate := float64(kv.rf.GetRaftStateSize()) / float64(kv.maxraftstate)
	if rate >= 0.9 {
		snapshotStatus := &SnapshotStatus{
			LastApplied:   kv.lastApplied,
			DB:            kv.db,
			RequestReplys: kv.requestReplys,
		}

		w := new(bytes.Buffer)
		if err := labgob.NewEncoder(w).Encode(snapshotStatus); err != nil {
			DPrintf("snapshot gob encode snapshotStatus err:%v", err)
			return
		}
		kv.rf.Snapshot(snapshotStatus.LastApplied, w.Bytes())
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.requestReplys = make(map[int64]RequestReply)
	// kv.resultChans = make(map[int64]*ResultChan)
	kv.resultChans = make(map[int64]*ResultChan)
	snapshot := kv.rf.GetSnapshot()
	kv.applySnapshot(snapshot)
	go kv.applyMsgHandler()
	return kv
}
