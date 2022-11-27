##### ArrayDeque

```java
// stack
public void push(E e) {
    addFirst(e);
}
public E pop() {
    return removeFirst();
}

// queue
public boolean offer(E e) {
    return offerLast(e);
}
public E poll() {
    return pollFirst();
}
public E peek() {  // 不会删除
    return peekFirst();
} 
```

