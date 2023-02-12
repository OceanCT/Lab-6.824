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
	"math/rand"
	//	"bytes"
    "strconv"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

func init() {
    rand.Seed(time.Now().Unix())
}


type Role int
const (
    Follower    Role = 0
    Candidate   Role = 1
    Leader      Role = 2
)
type Log struct {
    Command     string
    Term        int
}
const (
    ElectionTimeout = time.Millisecond * 1000
    IdleTimeout = time.Millisecond * 300
    // to avoid disconnected leader ask for resources too often
    DisConnectTimeOut = time.Millisecond * 500
    // to solve distributed deadlocks
    MaxRpcTime = time.Millisecond * 100
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LeaderOnlyInfo struct {
    nextIndex   []int
    matchIndex  []int
    disconnect  []bool
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    // change to rw mutex for better performance
	mu        sync.RWMutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    currentTerm int
    votedFor    int
    role        Role
    log         []Log
    commitIndex int
    lastApplied int
    electionTimeReset bool
    startElectionChannel    chan int
    stopElectionChannel     chan int
    stopLeaderChannel   chan int
    startLeaderChannel  chan int
    leaderOnly  *LeaderOnlyInfo

    // for debug 
    lockBelong  string
}

func (rf *Raft) getRoleName() string {
    if rf.role == Leader {
        return "Leader"
    } else if rf.role == Follower {
        return "Follower"
    } else if rf.role == Candidate {
        return "Candidate"
    } else {
        return ""
    }
}

func (rf *Raft) name() string {
    return "Server$" + strconv.FormatInt(int64(rf.me), 10) + "(Term: "+ strconv.FormatInt(int64(rf.currentTerm), 10) + 
    ", Role: "+ rf.getRoleName() +",)"
}

func (rf *Raft) lock(info string) {
    rf.mu.Lock()
    DPrintf(rf.name() + " Lock " + info)
}

func (rf *Raft) rLock(info string) {
    rf.mu.RLock()
    DPrintf(rf.name() + " Rlock " + info)
}

func (rf *Raft) unlock(info string) {
    name := rf.name()
    rf.mu.Unlock() 
    DPrintf(name + " Unlock " + info)
}
func (rf *Raft) rUnlock(info string) {
    name := rf.name()
    rf.mu.RUnlock()
    DPrintf(name + " RUnlcok " + info)
}

func (rf *Raft) dPrint(info string) {
    DPrintf(rf.name() + " " + info)
}


func (rf *Raft) randElectionTimeout() time.Duration {
    plus := time.Duration(rand.Int63()) % ElectionTimeout
    return plus + ElectionTimeout
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
    rf.mu.RLock()
    defer rf.mu.RUnlock()
    return rf.currentTerm , rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)

	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    // the Candidate's term
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term            int
    VoteGranted     bool
}

type AppendEntriesArgs struct {
    LeaderTerm      int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []Log
    LeaderCommit    int
}
type AppendEntriesReply struct {
    // currenct term of the received server
    Term            int
    Success         bool
}


func (rf *Raft) votedBefore() bool {
    return rf.votedFor != -1
}

func (rf *Raft) voteFor(server int) {
    rf.votedFor = server
} 

func (rf *Raft) changeCurrentTerm(term int) {
    rf.currentTerm = term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.lock("RequestVote")
    rf.lockBelong = "RequestVote"
    defer rf.unlock("RequestVote")
    rf.electionTimeReset = true
    reply.Term = rf.currentTerm
    if args.Term > rf.currentTerm {
        rf.convertRole(Follower)
    }
    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {
        if !rf.votedBefore() && (args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) ) {
            rf.voteFor(args.CandidateId)
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    }
    if args.Term > rf.currentTerm {
        rf.changeCurrentTerm(args.Term)  
    }
}



func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.lock("AppendEntries")
    rf.lockBelong = "AppendEntries"
    defer rf.unlock("AppendEntries")
    rf.electionTimeReset = true
    reply.Term = rf.currentTerm
    if args.LeaderTerm < rf.currentTerm {
        reply.Success = false
    } else if args.PrevLogIndex == -1 {
        reply.Success = true
        if rf.role == Candidate {
            rf.convertRole(Follower)
        }
    } else {
        if len(rf.log) < args.PrevLogIndex + 1 {
            reply.Success = false
        } else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
            reply.Success = false
        } else {
            reply.Success = true
            rf.log = append(rf.log[0: args.PrevLogIndex + 1], args.Entries...)
            if args.LeaderCommit > rf.commitIndex {
                if args.LeaderCommit > len(rf.log) - 1 {
                    rf.commitIndex = len(rf.log) - 1
                } else {
                    rf.commitIndex = args.LeaderCommit    
                }
            }
        }
    }
    if args.LeaderTerm > rf.currentTerm {
        rf.changeCurrentTerm(args.LeaderTerm)  
        rf.convertRole(Follower)
    }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ch := make(chan bool)
    go func() {
        ch <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
    }()
    select {
    case <- time.After(MaxRpcTime):
        DPrintf("AppendEntries timeout")
        return false
    case <- ch:
        return true
    }
//    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}


//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ch := make(chan bool)
    go func() {
        ch <- rf.peers[server].Call("Raft.RequestVote", args, reply)
    }()
    select {
    case <- time.After(MaxRpcTime):
        DPrintf("RequestVote timeout")
        return false
    case <- ch:
        return true
    }
//    return rf.peers[server].Call("Raft.RequestVote", args, reply)
}



// the initialization of server after changing its role
func (rf *Raft) convertRole(role Role) {
    var roleName string
    if role == Leader { roleName = "Leader" } else if role == Candidate { roleName = "Candidate" } else if role == Follower { roleName = "Follower" }
    rf.dPrint("convert to " + roleName)
    if role == Candidate {
        rf.currentTerm++
        rf.votedFor = rf.me
        rf.startElectionChannel <- 1
    } else if role == Leader {
        rf.leaderOnly = &LeaderOnlyInfo {
            nextIndex: make([]int, len(rf.peers), len(rf.peers)),
            matchIndex: make([]int, len(rf.peers), len(rf.peers)),
            disconnect: make([]bool, len(rf.peers), len(rf.peers)),
        }
        for index := range rf.leaderOnly.matchIndex {
            rf.leaderOnly.matchIndex[index] = -1
        }
        rf.startLeaderChannel <- 1
    } else if role == Follower {
        rf.votedFor = -1
    }
    rf.electionTimeReset = true
    rf.role = role
}






//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}


//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and maRequestVoteCPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
    rf.stopElectionChannel <- 1
}


func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}



func (rf *Raft) lastLogIndex() int {
    return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
    if len(rf.log) == 0 {
        return -1
    } else {
        return rf.log[rf.lastLogIndex()].Term
    }
}

func (rf *Raft) generateRequestVoteArgs() *RequestVoteArgs {
    args := &RequestVoteArgs {
        Term: rf.currentTerm,
        CandidateId: rf.me,
        LastLogIndex: rf.lastLogIndex(),
        LastLogTerm: rf.lastLogTerm(),
    }
    return args
}

func (rf *Raft) getRole() Role {
    rf.mu.RLock()
    defer rf.mu.RUnlock()
    return rf.role
}

// TODO::
func (rf *Raft) tryGetVote(serverIndex int) bool {
    rf.lock("tryGetVote from " + strconv.FormatInt(int64(serverIndex), 10))
    rf.lockBelong = "tryGetVote"
    defer rf.unlock("tryGetVote")
    reply := &RequestVoteReply {}
    rf.sendRequestVote(serverIndex, rf.generateRequestVoteArgs(), reply)
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.convertRole(Follower)
    }
    return reply.VoteGranted 
}


func (rf *Raft) elect() {
    voteNum := 1
    supporters := make([]int, 0, 0)
    for peerIndex := range rf.peers {
        if peerIndex == rf.me { continue }
        if rf.tryGetVote(peerIndex) {
            voteNum++
            supporters = append(supporters, peerIndex)
        } 
        if rf.getRole() != Candidate {
            return
        } else if voteNum >= (len(rf.peers) + 1) / 2 {
            // win the majority of votes, become leader
            rf.lock("Elect to become leader")
            DPrintf("Followers: %v", supporters)
            rf.convertRole(Leader)
            rf.unlock("Elect to become leader")
            return
        }
    }
}

// leaderOnly function
func (rf *Raft) checkCommitIndex() {
    for rf.getRole() == Leader {
        rf.lock("check commitIndex")
        newCommitIndex := rf.commitIndex + 1
        num := 1
        for index, match := range rf.leaderOnly.matchIndex {
            if index == rf.me { continue }
            if newCommitIndex <= match { num++ }
        }
        if num >= len(rf.peers) / 2 {
            rf.commitIndex = newCommitIndex
        }
        rf.unlock("check commitIndex")
    }    
}


func (rf *Raft) generateAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
    res := &AppendEntriesArgs {
        LeaderTerm: rf.currentTerm,
        LeaderId: rf.me,
        PrevLogIndex: prevLogIndex,
        // PrevLogTerm: rf.log[prevLogIndex].Term,
        Entries: rf.log[prevLogIndex + 1:],
        LeaderCommit: rf.commitIndex,
    }
    if prevLogIndex != -1 {
        res.PrevLogTerm = rf.log[prevLogIndex].Term
    } else {
        res.PrevLogTerm = -1
    }
    return res
}

// send idle appendEntries to establish leader position
func (rf *Raft) establishLeaderPosition() {
    rf.dPrint("try to establish leader position")
    for serverIndex := range rf.peers {
        if serverIndex == rf.me { continue }
        go func(server int) {
            for !rf.killed() && rf.getRole() == Leader {
                reply := &AppendEntriesReply{}
                disconnect := false
                rf.lock("Null AppendEntries To Server$" + strconv.FormatInt(int64(server), 10)) 
                rf.lockBelong = "NullAppendEntries"
                if rf.role == Leader {
                    args := rf.generateAppendEntriesArgs(-1)
                    disconnect = rf.sendAppendEntries(server, args, reply)
                    if reply.Term > rf.currentTerm {
                        rf.currentTerm = reply.Term
                        rf.convertRole(Follower)
                    }
                }
                rf.unlock("Null AppendEntries")
                if disconnect {
                    time.Sleep(DisConnectTimeOut)
                } else {
                    time.Sleep(IdleTimeout)
                }
            }
        }(serverIndex)
    }
}

// leader appends follower entries
func (rf *Raft) appendFollowerEntries() {
    for serverIndex := range rf.peers {
        go func(server int) {
            for rf.getRole() == Leader {
                rf.lock("appendFollowerEntries" + strconv.FormatInt(int64(server), 10))
                if len(rf.log) - 1 >= rf.leaderOnly.nextIndex[server] {       
                    args := rf.generateAppendEntriesArgs(rf.leaderOnly.nextIndex[server] - 1)
                    reply := &AppendEntriesReply{}
                    rf.sendAppendEntries(server, args, reply)
                    if reply.Success{
                        rf.leaderOnly.nextIndex[server] = len(rf.log)
                        rf.leaderOnly.matchIndex[server] = len(rf.log) - 1
                    }
                }
                rf.unlock("appendFollowerEntries")
            }
        }(serverIndex)
    }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft {}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    // rand.Seed(time.Now().UnixNano())
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.role = Follower
    rf.log = make([]Log, 0, 0)
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.electionTimeReset = false
    rf.startElectionChannel = make(chan int)
    rf.stopElectionChannel = make(chan int)
    rf.startLeaderChannel = make(chan int)
    rf.stopLeaderChannel = make(chan int)
    // initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go func() {
        for !rf.killed() {
            time.Sleep(rf.randElectionTimeout())
            if rf.electionTimeReset { 
                rf.electionTimeReset = false 
                continue
            }
            if rf.getRole() == Leader { continue }
            
            rf.lock("ElectionTimeOut")
            rf.lockBelong = "ElectionTimeOut"
            rf.convertRole(Candidate)
            rf.unlock("ElectionTimeout")
        }
    }()
    // candidate try to become leader
    go func() {
        for !rf.killed() {
            select {
            case <- rf.stopElectionChannel:
                return
            case <- rf.startElectionChannel:
                rf.elect()                
            }
        }       
    }()
    // waiting to become leader
    go func() {
        for !rf.killed() {
            select {
            case <- rf.stopLeaderChannel:
                return
            case <- rf.startLeaderChannel:
//                go rf.checkCommitIndex()
                go rf.establishLeaderPosition()
//                go rf.appendFollowerEntries()
            }
        }
    }()
    // if commitIndex > lastApplied apply log[lastApplied]
    /*
    go func() {
        for !rf.killed() {
            rf.mu.Lock()
            // rf.lock("check if logs can be applied")
            if rf.commitIndex > rf.lastApplied {
                // rf.apply(rf.log[rf.lastApplied])
                rf.lastApplied++
            } 
            rf.mu.Unlock()
            // rf.unlock("check if logs can be applied")
        }
    }()
    */
	return rf
}
