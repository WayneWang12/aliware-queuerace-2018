package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.utils.Config.bufferSize;

public class QueueHolder {

    private FileManager fileManager;
    private ReadBuffer readBuffer;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    private AtomicInteger msgOffset = new AtomicInteger();
    private AtomicInteger msgCount = new AtomicInteger();


    QueueHolder(FileManager fileManager) {
        this.fileManager = fileManager;
        this.readBuffer = new ReadBuffer(fileManager);
    }

    void add(byte[] msg) {
        if (writeBuffer.remaining() < msg.length + 4) {
            flush();
        }
        synchronized (writeBuffer) {
            if(writeBuffer.position() < 228 && writeBuffer.position() > 220) {
                System.out.println("神奇的数字");
            }
            writeBuffer.putInt(msg.length);
            writeBuffer.put(msg);
        }
        msgCount.getAndIncrement();
    }

    private boolean firstGet = true;

    List<byte[]> get(long offset, long num) {
        if(firstGet) {
            flush();
            readBuffer.init();
            firstGet = false;
        }
        return readBuffer.getMessages(offset, (int) num);
    }

    private void flush() {
        synchronized (this) {
            writeBuffer.position(writeBuffer.capacity());
            writeBuffer.flip();
            int[] result = fileManager.put(writeBuffer);
            writeBuffer.clear();
            //todo 记录索引信息
            int msgEnd = msgOffset.get() + msgCount.get();
            readBuffer.addIndex(msgOffset.get(), msgEnd, result[0], result[1]);
            msgOffset.set(msgEnd + 1);
            msgCount.set(0);
        }
    }

}
