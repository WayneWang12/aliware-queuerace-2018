package io.openmessaging;

import java.nio.ByteBuffer;

public class QueueManager {

    private ByteBuffer queueBuffer = ByteBuffer.allocateDirect(1500);
    private int msgCounter;

    static ThreadLocal<FileManager> fileManager = ThreadLocal.withInitial(FileManager::new);

    static final int msgBatch = 20;

    void add(byte[] msg) {
        queueBuffer.putInt(msg.length);
        queueBuffer.put(msg);
        msgCounter++;
        if(msgCounter % msgBatch == 0) {
            queueBuffer.position(queueBuffer.capacity());
            queueBuffer.flip();
            fileManager.get().writeQueueBuffer(queueBuffer);
            queueBuffer.clear();
        }
    }
}
