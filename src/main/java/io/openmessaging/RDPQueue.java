package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class RDPQueue {

    ByteBuffer byteBuffer;
    Block lastBlock;

//    long[] indexes = new long[50];

    static FileManager fileManager = new FileManager();
    static {
        fileManager.start();
    }

    static ThreadLocal<Block> blockThreadLocal = ThreadLocal.withInitial(fileManager::acquire);

    public RDPQueue() {
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
//        updateIndexes();
    }

    int currentPosition = 0;

//    void updateIndexes() {
//        long position = lastBlock.blockId * Constants.blockSize  + (lastBlock.currentPosition - 1) * Constants.bufferSize;
//        indexes[currentPosition++] = position;
//    }

    void add(byte[] msg) {
        if (byteBuffer.remaining() < msg.length + 4) {
            byteBuffer.position(byteBuffer.capacity());
            lastBlock.notifyFull();
            updateToNewByteBuffer();
        }
        byteBuffer.put((byte) msg.length);
        byteBuffer.put(msg);
    }

    private boolean firstGet = true;

    static AtomicInteger got = new AtomicInteger();

    ArrayList<byte[]> getMessages(long offset, long num) {
        if(firstGet) {
            firstGet = false;
            got.getAndIncrement();
            byteBuffer.position(byteBuffer.capacity());
            lastBlock.notifyFull();
//            updateIndexes();
        }
        throw new NoSuchElementException("not implemented!");
    }

}
