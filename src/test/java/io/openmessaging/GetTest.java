package io.openmessaging;

import java.util.Collection;

public class GetTest {

    public static void main(String[] args) throws InterruptedException {
        QueueStore store = new DefaultQueueStoreImpl();

        for (int i = 0; i < 2000; i++) {
            store.put("Queue-1", generate1K((byte) 1));
            store.put("Queue-1", generate1K((byte) 2));
            store.put("Queue-1", generate1K((byte) 3));
            store.put("Queue-1", generate1K((byte) 4));
            store.put("Queue-1", generate1K((byte) 5));
        }

        System.out.println("put end");

        Thread.sleep(2000);

        print(store.get("Queue-1", 5000, 20));   //0,1,
        print(store.get("Queue-1", 1, 3));   //1,2,3
        print(store.get("Queue-1", 3, 3));   //3,4,5
        print(store.get("Queue-1", 6, 3));   //6,6
        print(store.get("Queue-1", 0, 3));   //0,1,2
        print(store.get("Queue-1", 1, 3));   //1,2,3
    }

    private static byte[] generate1K(byte head) {
        byte[] K = new byte[40];
        K[0] = head;
        return K;
    }

    private static void print(Collection<byte[]> bs) {
        for (byte[] b : bs) {
            if (b.length == 0) {
                System.out.print("null,");
                continue;
            }
            System.out.print(b[0] + ",");
        }
        System.out.println();
        System.out.println("-----------------------");
    }

}
