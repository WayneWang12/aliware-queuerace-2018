package io.openmessaging;

public class Tuple<T, V> {
    final T first;
    final V second;

    public Tuple(T first, V second) {
        this.first = first;
        this.second = second;
    }
}
