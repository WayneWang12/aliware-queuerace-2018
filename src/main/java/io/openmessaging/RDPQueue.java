package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class RDPQueue {


    final int queueId;
    ByteBuffer byteBuffer;
    Block lastBlock;
    ArrayList<int[]> indexes = new ArrayList<>();

    static FileManager fileManager = new FileManager();
    static {
        fileManager.start();
    }

    static ThreadLocal<Block> blockThreadLocal = ThreadLocal.withInitial(fileManager::acquire);

    public RDPQueue(int queueId) {
        this.queueId = queueId;
        updateToNewByteBuffer();
    }

    AtomicInteger msgCount = new AtomicInteger();

    void updateToNewByteBuffer() {
        this.lastBlock = blockThreadLocal.get();
        this.byteBuffer = lastBlock.acquire();
        while (byteBuffer == null) {
            Block newBlock = fileManager.acquire();
            this.lastBlock = newBlock;
            this.byteBuffer = newBlock.acquire();
            blockThreadLocal.set(newBlock);
        }
        int[] index = new int[]{lastBlock.blockId, lastBlock.currentPosition - 1, msgCount.get()};
        indexes.add(index);
    }

    void add(byte[] msg) {
        if (byteBuffer.remaining() < msg.length + 4) {
            byteBuffer.position(byteBuffer.capacity());
            lastBlock.notifyFull();
            updateToNewByteBuffer();
        }
        byteBuffer.putInt(msg.length);
        byteBuffer.put(msg);
        msgCount.getAndIncrement();
    }

    ArrayList<byte[]> getMessages(long offset, long num) {
//       return new ArrayList<>();
        throw new NoSuchElementException("not implemented!");
    }

}
