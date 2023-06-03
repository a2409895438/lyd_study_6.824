# lyd_study_6.824

# Lab1:MapReduce

包括两部分 coordinator 与 worker

在实际系统中，workers会运行在不同的机器中，在本次实验中只会运行在一台机器中并且通过RPC进行通信，每一个worker向coordinator请求任务。

mrsequential.go中是单线程的map-reduce

mrapps/wc.go 有map和reduce函数

## RPC通信

需要实现coordinator 与 worker的RPC通信

## 序列化存储

使用GO json库中的json.NewEncoder 与 json.NewDecoder

## 实现

代码实现在6.5840/src/mr 下

### 调度器 mr/coordinator.go

主要负责分发任务

```go
type Coordinator struct {
	// Your definitions here.
	nReduce   int
	nMap      int
	TaskQueue chan *Task
	TaskMap   map[int]*Task
	Type      uint
	mu        sync.Mutex
}

type Task struct {
	TaskId     int
	File       string
	TaskType   uint
	NReduce    int
	NMap       int
	CreateTime int
}
```

首先读取files字符串列表，建立Map任务并且分发任务到任务队列和任务列表

同时启动任务切换和超时检查的协程

任务切换：当任务列表TaskMap中没有任务的时候，切换任务状态  Map->Reduce->Exit   Exit状态目的是使程序退出os.Exit(0)

超时检查：主要针对crash test，遍历检查TaskMap中的任务，当存活时间超过10秒的时候，重新将其加入任务队列

### 任务器 mr/worker.go

两次RPC通信：首先通过RPC通信向调度器请求任务，完成任务后再次通过RPC告诉调度器任务已经完成

实现两个任务函数DoMapWork与DoReduceWork

## 可能存在的测试BUG

### 测试失败1

*** Starting early exit test.
test-mr.sh: line 259: wait: -n: invalid option
wait: usage: wait [id]
sort: cannot read: mr-out*: No such file or directory
cmp: EOF on mr-wc-all-initial
--- output changed after first worker exited
--- early exit test: FAIL

原因wait -n 不能被识别，需要upgrade bash

更新bash版本到5.0.0后成功
*** Starting early exit test.
--- early exit test: PASS

### 测试失败2

Starting crash test.

*** Starting crash test.
2023/04/10 17:47:33 dialing:dial unix /var/tmp/5840-mr-0: connect: connection refused
2023/04/10 17:47:33 dialing:dial unix /var/tmp/5840-mr-0: connect: connection refused
2023/04/10 17:47:33 dialing:dial unix /var/tmp/5840-mr-0: connect: connection refused
2023/04/10 17:47:33 dialing:dial unix /var/tmp/5840-mr-0: connect: connection refused
sort: cannot read: mr-out*: No such file or directory
cmp: EOF on mr-crash-all
--- crash output is not the same as mr-correct-crash.txt
--- crash test: FAIL
*** FAILED SOME TESTS

需要对超过10S的任务进行舍弃,并且重新放入任务队列

### 测试全部通过

*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS



# Lab2: RAFT

## 2A

