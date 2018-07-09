package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class Block {
    ByteBuffer blockBuffer;
    ByteBuffer[] bufferPoolFromBlock;
    int blockId = -1;

    static final int bufferNumber = Constants.blockSize / Constants.bufferSize;

    int currentPosition = 0;

    public Block(ByteBuffer blockBuffer) {
        this.blockBuffer = blockBuffer;
        this.bufferPoolFromBlock = new ByteBuffer[bufferNumber];
        for(int i = 0; i < bufferNumber; i++) {
            blockBuffer.position(i * Constants.bufferSize);
            blockBuffer.limit((i + 1) * Constants.bufferSize);
            bufferPoolFromBlock[i] = blockBuffer.slice();
        }
    }

    AtomicLong fullBufferNumber = new AtomicLong();

    void notifyFull() {
        fullBufferNumber.getAndIncrement();
    }

    boolean isFull() {
        return currentPosition == bufferNumber && (int)fullBufferNumber.get() == bufferNumber;
    }

    ByteBuffer acquire() {
        if(currentPosition < bufferNumber) {
            return bufferPoolFromBlock[currentPosition++];
        } else {
            return null;
        }
    }

    public void resetState() {
        fullBufferNumber.set(0);
        currentPosition = 0;
        blockId = -1;
        for(int i = 0; i<bufferNumber; i++) {
            bufferPoolFromBlock[i].clear();
        }
    }
}
