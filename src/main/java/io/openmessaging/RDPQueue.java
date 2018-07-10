package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RDPQueue {

    int queueId;
    ByteBuffer byteBuffer;
    Block lastBlock;

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
        this.byteBuffer = lastBlock.acquire(queueId);
        while (byteBuffer == null) {
            Block newBlock = fileManager.acquire();
            this.lastBlock = newBlock;
            this.byteBuffer = newBlock.acquire(queueId);
            blockThreadLocal.set(newBlock);
        }
    }

    void add(byte[] msg) {
        if (byteBuffer.remaining() < msg.length + 4 || msgCount.incrementAndGet() % Constants.msgBatch == 0) {
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
        if (firstGet) {
            firstGet = false;
            fileManager.inReadStage.set(true);
            if (got.incrementAndGet() == DefaultQueueStoreImpl.queueMap.size()) {
                FileManager.needWrite.set(true);
            }
            byteBuffer.position(byteBuffer.capacity());
            lastBlock.notifyFull();
        }
        return findBlockByOffsetAndNumber((int) offset, (int) num);
    }

    ArrayList<byte[]> findBlockByOffsetAndNumber(int offset, int num) {
        int index = offset / Constants.msgBatch;
        long filePosition = Block.indexMap.get(queueId).get(index);
        int blockId = (int) (filePosition / Constants.blockSize);
        int bufferPosition = (int) (filePosition % Constants.blockSize / Constants.bufferSize);
        int offsetInBuffer = offset % Constants.msgBatch;
        if(bufferPosition < 0) {
            System.out.println("strange");
        }
        return fileManager.getBlockById(blockId).getMessages(bufferPosition, offsetInBuffer, num);
    }


}
