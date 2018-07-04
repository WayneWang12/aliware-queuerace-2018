package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.utils.Config.bufferSize;

public class QueueHolder {

    private final FileManager fileManager;
    private ReadBuffer readBuffer;

    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    private AtomicInteger msgOffset = new AtomicInteger();
    private AtomicInteger msgCount = new AtomicInteger();


    QueueHolder(FileManager fileManager) {
        this.fileManager = fileManager;
        this.readBuffer = new ReadBuffer(fileManager);
    }

    synchronized void add(byte[] msg) {
        if (writeBuffer.remaining() < msg.length + 4) {
            flush();
        }
        writeBuffer.putInt(msg.length);
        writeBuffer.put(msg);
        msgCount.getAndIncrement();
    }

    private boolean firstGet = true;

    List<byte[]> get(long offset, long num) {
        if (firstGet) {
            firstGet = false;
            flush();
            readBuffer.init(writeBuffer);
        }
        return readBuffer.getMessages(offset, (int) num);
    }

    private void flush() {
        writeBuffer.position(writeBuffer.capacity());
        writeBuffer.flip();
        int[] result = fileManager.put(writeBuffer);
        writeBuffer.clear();
        int msgEnd = msgOffset.get() + msgCount.get() - 1;
        readBuffer.addIndex(msgOffset.get(), msgEnd, result[0], result[1]);
        msgOffset.set(msgEnd + 1);
        msgCount.set(0);
    }

}
