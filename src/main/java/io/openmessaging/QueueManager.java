package io.openmessaging;

import java.nio.ByteBuffer;

public class QueueManager {

    static public DirectByteBufferPool pool = new DirectByteBufferPool();

    private ByteBuffer queueBuffer = pool.acquire();

    final int queueId;

    static ThreadLocal<FileManager> fileManager = ThreadLocal.withInitial(FileManager::new);

    public QueueManager(int queueId) {
        this.queueId = queueId;
    }


    void add(byte[] msg) {
        if(queueBuffer.remaining() < msg.length + 4) {
            queueBuffer.position(queueBuffer.capacity());
            queueBuffer.flip();
            fileManager.get().putMessage(queueId, queueBuffer);
            queueBuffer = pool.acquire();
        }
        queueBuffer.putInt(msg.length);
        queueBuffer.put(msg);
    }

    void getMessages(long offset, long num) {

    }
}
