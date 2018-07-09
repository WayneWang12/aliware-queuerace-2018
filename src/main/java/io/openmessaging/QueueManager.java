package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueManager {

    private ByteBuffer queueBuffer = ByteBuffer.allocateDirect(1024);

    final int queueId;

    static ThreadLocal<FileManager> fileManager = ThreadLocal.withInitial(FileManager::new);
    static FileManager lastFileManager = fileManager.get();

    public QueueManager(int queueId) {
        this.queueId = queueId;
    }

    AtomicInteger msgCounter = new AtomicInteger();

    void add(byte[] msg) {
        queueBuffer.putInt(msg.length);
        queueBuffer.put(msg);
        if(msgCounter.incrementAndGet() % 10 == 0) {
            queueBuffer.position(queueBuffer.capacity());
            queueBuffer.flip();
            fileManager.get().putMessage(queueId, queueBuffer);
            queueBuffer.clear();
        }
    }

    boolean firstGet = true;

    ArrayList<byte[]> getMessages(long offset, long num) {
        if(firstGet) {
            firstGet = false;
            queueBuffer.position(queueBuffer.capacity());
            queueBuffer.flip();
            lastFileManager.lastPut(queueId, queueBuffer);
            queueBuffer.clear();
            queueBuffer = null;
        }
        return fileManager.get().getMessage(queueId, (int)offset, (int)num);
    }
}
