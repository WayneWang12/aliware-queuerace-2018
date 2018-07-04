package io.openmessaging;

public class PutTest {

    public static void main(String[] args) {
        QueueStore store = new DefaultQueueStoreImpl();

        store.put("Queue-1", new byte[]{10, 12, 13});
    }

}
