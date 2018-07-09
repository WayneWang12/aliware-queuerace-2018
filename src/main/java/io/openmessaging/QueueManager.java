package io.openmessaging;

import io.openmessaging.utils.DirectByteBufferPool;

import java.nio.ByteBuffer;

public class QueueManager {

    static public DirectByteBufferPool pool = new DirectByteBufferPool(2000000);

    private ByteBuffer queueBuffer = pool.acquire();

    final int queueId;

    static FileManager fileManager;

    static {
        fileManager = new FileManager();
    }

    public QueueManager(int queueId) {
        this.queueId = queueId;
    }


    void add(byte[] msg) {
        if(queueBuffer.remaining() < msg.length + 4) {
            queueBuffer.position(queueBuffer.capacity());
            queueBuffer.flip();
            fileManager.putMessage(queueId, queueBuffer);
            queueBuffer = pool.acquire();
        }
        queueBuffer.putInt(msg.length);
        queueBuffer.put(msg);
    }

    void getMessages(long offset, long num) {

    }
}
