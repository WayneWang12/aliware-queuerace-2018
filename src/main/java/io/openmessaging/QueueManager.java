package io.openmessaging;

import java.nio.ByteBuffer;

public class QueueManager {

    private ByteBuffer queueBuffer = ByteBuffer.allocateDirect(1024);

    static ThreadLocal<FileManager> fileManager = ThreadLocal.withInitial(FileManager::new);
    static ThreadLocal<ReadManager> readManager = ThreadLocal.withInitial(ReadManager::new);

    void add(byte[] msg) {
        if(queueBuffer.remaining() < msg.length + 4) {
            queueBuffer.position(queueBuffer.capacity());
            queueBuffer.flip();
            fileManager.get().writeQueueBuffer(queueBuffer);
            queueBuffer.clear();
        }
        queueBuffer.putInt(msg.length);
        queueBuffer.put(msg);
    }

    void getMessages(long offset, long num) {

    }
}
