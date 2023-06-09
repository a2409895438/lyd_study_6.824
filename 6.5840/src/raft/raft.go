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

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

const (
	LEADER = iota + 1
	FOLLOWER
	CANDITATE
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state        int       //身份  1:LEADER 2:FOLLOWER 3:CANDICATE
	electTimeout time.Time //选举超时时间
	voteFor      int       //投票对象
	curTerm      int       //目前任期

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.curTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type Entry struct {
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if args.Term > rf.curTerm {

		rf.voteFor = args.CandidateId
		rf.curTerm = args.Term
		rf.electTimeout = SetElectTimeout()
		rf.state = FOLLOWER
		reply.VoteGranted = true
		return
	}
	if args.Term < rf.curTerm {
		reply.VoteGranted = false
		return
	}
	// 任期一样
	if rf.state == FOLLOWER && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		rf.voteFor = args.CandidateId
		rf.electTimeout = SetElectTimeout()
		reply.VoteGranted = true
	}
	return
}

// 心跳或者日志追加
type AppendEntriesArgs struct {
	Term     int //leader currentTerm
	LeaderId int
	//用于日志复制，确保前面日志能够匹配
	PrevLogTerm  int
	PrevLogIndex int

	Entries      []*Entry
	LeaderCommit int
}

// 心跳或者日志追加
type AppendEntriesReply struct {
	Success bool
	Term    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.curTerm > args.Term {
		reply.Success = false
		reply.Term = rf.curTerm
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.curTerm = args.Term
	rf.electTimeout = SetElectTimeout()
	rf.state = FOLLOWER

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(10) * time.Millisecond)

		// fmt.Printf("目前server:%d 的身份为:%d 任期为:%d ,超时时间为：%v \n", rf.me, rf.state, rf.curTerm, rf.electTimeout)
		// Your code here (2A)
		// Check if a leader election should be started.
		if time.Now().After(rf.electTimeout) {
			// 超时
			if rf.state == LEADER {
				continue
			}
			fmt.Printf(" %v: %v 选举超时 , 任期为 : %v\n", time.Now(), rf.me, rf.curTerm)
			rf.BeCandidate()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}

		// pause for a random amount of time between 50 and 150
		// milliseconds.

	}
}

func (rf *Raft) BeCandidate() {
	rf.mu.Lock()
	rf.state = CANDITATE
	rf.curTerm++
	rf.voteFor = rf.me
	rf.electTimeout = SetElectTimeout()
	rf.mu.Unlock()

	count := 1

	//发送请求
	waitFlag := sync.WaitGroup{}
	for i := range rf.peers {
		if rf.state != CANDITATE {
			return
		}

		if i != rf.me {
			server := i
			waitFlag.Add(1)
			go func() {
				args := &RequestVoteArgs{
					Term:         rf.curTerm,
					CandidateId:  rf.me,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}
				reply := &RequestVoteReply{}
				if ok := rf.sendRequestVote(server, args, reply); ok {
					if rf.state != CANDITATE {
						return
					}
					if reply.VoteGranted {
						count++
					}

					if count > len(rf.peers)/2 && rf.state == CANDITATE {
						rf.mu.Lock()
						fmt.Printf("%v 成了leader 任期为:%d\n", rf.me, rf.curTerm)
						rf.state = LEADER
						rf.mu.Unlock()
						rf.BeLeader()
					}
				}
				waitFlag.Done()
			}()
		}
	}
	waitFlag.Wait()
}

func (rf *Raft) BeLeader() {
	for {
		if rf.state != LEADER {
			return
		}
		for i := range rf.peers {
			if i != rf.me {
				server := i
				go func() {
					args := &AppendEntriesArgs{
						Term:     rf.curTerm,
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{}
					if ok := rf.sendAppendEntries(server, args, reply); ok {
						if !reply.Success && reply.Term > rf.curTerm {
							rf.state = FOLLOWER
						}
					}
				}()
			}
		}
		time.Sleep(10 * time.Millisecond)
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
	rand.Seed(int64(me))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.electTimeout = SetElectTimeout()
	rf.curTerm = 0
	rf.voteFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func SetElectTimeout() time.Time {
	return time.Now().Add(time.Duration(50+(rand.Int63()%150)) * time.Millisecond) // 随机超时时间150-600
}
