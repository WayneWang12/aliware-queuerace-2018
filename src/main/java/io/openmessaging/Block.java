package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Block {
    ByteBuffer blockBuffer;
    ByteBuffer[] bufferPoolFromBlock;
    int blockId = -1;

    static ConcurrentHashMap<Integer, ArrayList<Long>> indexMap = new ConcurrentHashMap<>();

    static final int bufferNumber = Constants.blockSize / Constants.bufferSize;

    int currentPosition = 0;

    public Block(ByteBuffer blockBuffer) {
        this.blockBuffer = blockBuffer;
        this.bufferPoolFromBlock = new ByteBuffer[bufferNumber];
        reloadDataFromBlock();
    }

    void reloadDataFromBlock() {
        blockBuffer.clear();
        for (int i = 0; i < bufferNumber; i++) {
            blockBuffer.position(i * Constants.bufferSize);
            blockBuffer.limit((i + 1) * Constants.bufferSize);
            bufferPoolFromBlock[i] = blockBuffer.slice();
        }
    }

    AtomicInteger fullBufferNumber = new AtomicInteger();

    void notifyFull() {
        fullBufferNumber.getAndIncrement();
    }

    boolean isFull() {
        return (blockId >= 0) && currentPosition !=0 && currentPosition == fullBufferNumber.get();
    }

    ByteBuffer acquire(int queueId) {
        if (currentPosition < bufferNumber) {
            long position = (long) currentPosition * Constants.bufferSize + Constants.blockSize * blockId;
            indexMap.putIfAbsent(queueId, new ArrayList<>());
            indexMap.get(queueId).add(position);
            return bufferPoolFromBlock[currentPosition++];
        } else {
            return null;
        }
    }

    public void resetState() {
        fullBufferNumber.set(0);
        currentPosition = 0;
        blockId = -1;
        for (int i = 0; i < bufferNumber; i++) {
            bufferPoolFromBlock[i].clear();
        }
    }

    ArrayList<byte[]> getMessages(int bufferPosition, int offset, int num) {
        ByteBuffer byteBuffer = bufferPoolFromBlock[bufferPosition].duplicate();
        ArrayList<byte[]> messages = new ArrayList<>();
        for (int i = 0; i < offset; i++) {
            int length = byteBuffer.get();
            byteBuffer.position(byteBuffer.position() + length);
        }
        for (int i = 0; i < num; i++) {
            int length = byteBuffer.get();
            if (length != 0) {
                byte[] msg = new byte[length];
                byteBuffer.get(msg);
                messages.add(msg);
            }
        }
        return messages;
    }

}
