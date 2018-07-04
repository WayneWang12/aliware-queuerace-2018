package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.utils.Config.bufferSize;

public class QueueHolder {

    final FileManager fileManager;
    ReadBuffer readBuffer;

    final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    AtomicInteger msgOffset = new AtomicInteger();
    AtomicInteger msgCount = new AtomicInteger();


    QueueHolder(FileManager fileManager) {
        this.fileManager = fileManager;
        this.readBuffer = new ReadBuffer(fileManager);
    }

    synchronized void add(byte[] msg) {
        if (writeBuffer.remaining() < msg.length + 4) {
            flush();
        }
        synchronized (writeBuffer) {
            writeBuffer.putInt(msg.length);
            writeBuffer.put(msg);
        }
        msgCount.getAndIncrement();
    }

    private boolean firstGet = true;

    List<byte[]> get(long offset, long num) {
        if (firstGet) {
            flush();
            readBuffer.init();
            firstGet = false;
        }
        return readBuffer.getMessages(offset, (int) num);
    }

    private void flush() {
        synchronized (fileManager) {
            writeBuffer.position(writeBuffer.capacity());
            writeBuffer.flip();
            int[] result = fileManager.put(writeBuffer);
            writeBuffer.clear();
            //todo 记录索引信息
            int msgEnd = msgOffset.get() + msgCount.get() - 1;
            readBuffer.addIndex(msgOffset.get(), msgEnd, result[0], result[1]);
            msgOffset.set(msgEnd + 1);
            msgCount.set(0);
        }
    }

}
