package queue

import "container/heap"

type Queue struct {
	pq *priorityQueue[int64]
}

func New() *Queue {
	queue := new(priorityQueue[int64])
	heap.Init(queue)
	return &Queue{
		pq: queue,
	}
}

func (q *Queue) Root() int64 {
	return q.pq.Root()
}

func (q *Queue) Push(value int64) {
	heap.Push(q.pq, value)
}

func (q *Queue) Pop() int64 {
	return heap.Pop(q.pq).(int64)
}
