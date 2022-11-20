package queue

type priorityQueue[T ~int | int64 | int32] []T

func (pq priorityQueue[T]) Root() T {
	if len(pq) > 0 {
		return pq[0]
	}
	return -1
}

func (pq priorityQueue[T]) Len() int { return len(pq) }

func (pq priorityQueue[T]) Less(i, j int) bool {
	return pq[i] < pq[j]
}

func (pq priorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue[T]) Push(x any) {
	value := x.(T)
	*pq = append(*pq, value)
}

func (pq *priorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	value := old[n-1]
	*pq = old[:n-1]
	return value
}
