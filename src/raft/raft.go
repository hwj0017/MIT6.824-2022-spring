package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "crypto/rand"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"MIT6.824-2022-spring/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

var HeartBeatTimeout int = 50

type LogEntry struct {
	Term    int         // 创建该条目时的任期号
	Command interface{} // 要应用于状态机的命令
}
type Timer struct {
	timer *time.Ticker
}

func (t *Timer) reset() {
	randomTime := time.Duration(150+rand.Intn(200)) * time.Millisecond // 200~350ms
	t.timer.Reset(randomTime)                                          // 重置时间
}

func (t *Timer) resetHeartBeat() {
	t.timer.Reset(time.Duration(HeartBeatTimeout) * time.Millisecond)
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // 记录当前的任期
	votedFor    int        // 投给了谁
	log         []LogEntry // 日志条目数组

	// 易失性状态
	commitIndex int // 已被提交的日志索引
	lastApplied int // 已提交的日志索引

	// leader状态
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	// 快照状态

	// 自己的一些变量状态
	state     State // follower/candidate/leader
	timer     Timer
	voteCount int // 票数
	applyCh   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
		// fmt.Println("Leader是:", rf.me)
	}
	return rf.currentTerm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)    // 创建缓冲区，实际上是一个字节切片
	e := labgob.NewEncoder(w) // 创建一个新的labgob编码器，编码后的内容存放在w中
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()                // 将缓冲区的内容赋值给raftstate
	rf.persister.SaveRaftState(raftstate) // 在未实现快照之前，第二个参数设置为nil
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		votedFor    int
		log         []LogEntry
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("decode error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人最后的日志索引
	LastLogTerm  int // 候选人最后的日志任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票人的当前任期
	VoteGranted bool // true表示该节点把票投给了候选人
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	LastTerm      int
	LastTermIndex int // 用于返回与Leader.Term的匹配项,方便同步日志
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("RequestVote: %+v\n", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	neadPersisit := false
	reply.Term = max(rf.currentTerm, args.Term)
	reply.VoteGranted = false // 初始化
	// 1. 比自己还小直接返回不用看了
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		neadPersisit = true
	}
	// 此时已是follower
	// 比较Term、
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		{
			// fmt.Printf("CandidateId:%d, vote for %d\n", rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			neadPersisit = true
			rf.timer.reset()
		}
	}
	if neadPersisit {
		rf.persist()
	}
	// // Your code here (2A, 2B).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// reply.Term = rf.currentTerm
	// reply.VoteGranted = false // 初始化

	// // 1. 比自己还小直接返回不用看了
	// if args.Term < rf.currentTerm {
	// 	return
	// }

	// // 如果votedFor为空或候选人Id，且候选人的日志至少与接收方的日志一样新，则同意投票（§5.2，§5.4）
	// // if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
	// // 	(args.LastLogTerm > rf.log[len(rf.log)-1].term || (args.LastLogTerm == rf.log[len(rf.log)-1].term && args.LastLogIndex >= len(rf.log)) ) {

	// // 2. candidate的term比自己大，可以投票，转为follower
	// if args.Term > rf.currentTerm {
	// 	rf.state = Follower
	// 	rf.voteCount = 0
	// 	rf.votedFor = args.CandidateId
	// }

	// // 3. candidate的term和自己相等的情况下，如果没投过票，则投票
	// if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	// 	rf.votedFor = args.CandidateId
	// 	reply.VoteGranted = true // 投票
	// 	rf.timer.reset()
	// }
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		// fmt.Printf("%d sendRequestVote to %d failed, term: %d\n", rf.me, server, rf.currentTerm)
		return false
	}
	// fmt.Println(reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term || rf.state != Candidate {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return false
	}
	// 如果选择投票
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			// fmt.Printf("leaderId of %d:%d\n", len(rf.peers), rf.me)
			rf.state = Leader
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.timer.resetHeartBeat() // 改为设置心跳时间
			return true
		}
	}
	return false
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// // 收到过期的RPC回复,不处理
	// if args.Term < rf.currentTerm {
	// 	return false
	// }

	// // 如果选择投票
	// if reply.VoteGranted {
	// 	rf.voteCount++
	// 	if rf.voteCount > len(rf.peers)/2 {
	// 		// fmt.Println("新王登基，他的ID是:" ,rf.me)
	// 		rf.state = Leader
	// 		rf.timer.resetHeartBeat() // 改为设置心跳时间
	// 	}
	// } else {
	// 	// 拒绝投票
	// }

	// return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	needPersisit := false
	reply.Term = max(args.Term, rf.currentTerm)
	reply.Success = true
	reply.LastTerm = 0
	reply.LastTermIndex = 0
	// fmt.Println("收到心跳")
	// 收到rpc的term比自己的小 (§5.1)
	if args.Term < rf.currentTerm { // 并通知leader变为follower
		reply.Success = false
		return
	}
	rf.timer.reset()
	if args.Term > rf.currentTerm {
		// udate self
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		needPersisit = true
	}

	// log 缺失
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

	} else {
		// log 匹配
		reply.Success = true
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		rf.commitIndex = max(rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex > rf.lastApplied {
			go rf.applyLogs()
		}
		needPersisit = true
	}
	reply.LastTerm = rf.log[len(rf.log)-1].Term
	reply.LastTermIndex = len(rf.log) - 1
	if needPersisit {
		rf.persist()
	}
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// // fmt.Println("收到心跳")
	// // 收到rpc的term比自己的小
	// if args.Term < rf.currentTerm {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// } else {
	// 	rf.state = Follower
	// 	rf.votedFor = args.LeaderId
	// 	rf.currentTerm = args.Term
	// 	rf.timer.reset()

	// 	reply.Term = rf.currentTerm
	// 	reply.Success = true
	// }
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {

	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		// fmt.Printf("%d sendAppendEntries to %d failed, term: %d\n", rf.me, server, rf.currentTerm)
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(reply)
	// old leader
	if args.Term != rf.currentTerm || rf.state != Leader {
		return false
	}
	if rf.currentTerm < args.Term {
		// fmt.Println("old leader", rf.me, "becomes follower")
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return false
	}
	if !reply.Success {
		lastTermIndex := rf.nextIndex[server] - 1
		// TODO
		for lastTermIndex > 0 && rf.log[lastTermIndex].Term != reply.LastTerm {
			lastTermIndex--
		}
		rf.nextIndex[server] = min(lastTermIndex, reply.LastTermIndex) + 1
		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm: rf.log[rf.nextIndex[server]-1].Term, Entries: make([]LogEntry, len(rf.log[rf.nextIndex[server]:])), LeaderCommit: rf.commitIndex}
		copy(args.Entries, rf.log[rf.nextIndex[server]:])
		go rf.sendAppendEntries(server, args)
		return true
	}
	// success
	rf.nextIndex[server] = reply.LastTermIndex + 1 // CommitIndex为对端确定两边相同的index 加上1就是下一个需要发送的日志
	rf.matchIndex[server] = reply.LastTermIndex
	commitCount := 0
	rf.matchIndex[rf.me] = len(rf.log) - 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= reply.LastTermIndex {
			commitCount++
		}
	}
	if commitCount > len(rf.peers)/2 && rf.commitIndex < reply.LastTermIndex && rf.log[reply.LastTermIndex].Term == rf.currentTerm {
		rf.commitIndex = reply.LastTermIndex
		// fmt.Println("commitIndex:", rf.commitIndex)
		go rf.applyLogs()
	}
	return true

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// // 1. 收到过期的prc回复
	// if args.Term < rf.currentTerm {
	// 	return false
	// }

	// // 2. 心跳不允许
	// if !reply.Success {
	// 	rf.state = Follower
	// 	rf.currentTerm = reply.Term

	// 	rf.votedFor = -1
	// 	rf.voteCount = 0
	// 	rf.timer.reset()
	// }
	// return true
}

// 将日志写入管道
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > len(rf.log)-1 {
		fmt.Println("出现错误 : raft.go commitlogs()")
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
	rf.lastApplied = rf.commitIndex
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}
	// 添加新日志
	e := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, e)
	rf.persist()
	index = len(rf.log) - 1
	term = rf.currentTerm
	// fmt.Printf("leader %d, index %d, term %d\n", rf.me, index, term)
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.timer.C:
			rf.mu.Lock()
			switch rf.state {
			case Follower: // follower->candidate
				rf.state = Candidate
				// fmt.Println(rf.me, "进入candidate状态")
				fallthrough
			case Candidate: // 成为候选人，开始拉票
				rf.timer.reset()
				rf.currentTerm++
				// fmt.Printf("candidate %d start election, term %d\n", rf.me, rf.currentTerm)
				rf.voteCount = 1
				rf.votedFor = rf.me
				rf.persist()
				for i := 0; i < len(rf.peers); i++ {
					if rf.me == i { // 排除自己
						continue
					}
					args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}
					go rf.sendRequestVote(i, args)
				}
			case Leader:
				rf.timer.resetHeartBeat()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm: rf.log[rf.nextIndex[i]-1].Term, Entries: make([]LogEntry, len(rf.log[rf.nextIndex[i]:])), LeaderCommit: rf.commitIndex}
					copy(args.Entries, rf.log[rf.nextIndex[i]:]) // 显式拷贝
					go rf.sendAppendEntries(i, args)
				}

			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	// log[0]
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.timer = Timer{timer: time.NewTicker(time.Duration(150+rand.Intn(200)) * time.Millisecond)}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
