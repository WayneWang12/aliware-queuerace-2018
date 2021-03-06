package io.openmessaging;

import java.nio.ByteBuffer;

public class RDPBlock {
    ByteBuffer rdpBuffer;
    ByteBuffer[] bufferForQueues;
    int bufferNumber;
    int currentPosition = 0;
    boolean full = false;
    long blockPositionInFile = -1;

    public RDPBlock(ByteBuffer rdpBuffer) {
        this.rdpBuffer = rdpBuffer;
        this.bufferForQueues = new ByteBuffer[(int) (Constants.blockSize / Constants.bufferSize)];
        int i ;
        for(i = 0; (i + 1) * Constants.bufferSize <= Constants.blockSize; i++) {
            rdpBuffer.position((int) (i * Constants.bufferSize));
            rdpBuffer.limit((int) ((i + 1) * Constants.bufferSize));
            bufferForQueues[i] = rdpBuffer.slice();
        }
        this.bufferNumber = i;
    }

    public boolean isFull() {
        if(!full && blockPositionInFile > 0) {
            for(ByteBuffer bb:bufferForQueues) {
                if(!(bb.position() == bb.limit())) {
                    return false;
                }
            }
            full = true;
        }
        return full;
    }

    ByteBuffer acquireQueueBuffer(RDPQueue queue) {
        if(currentPosition < bufferNumber) {
            long queueFilePosition = (long)currentPosition * Constants.bufferSize + blockPositionInFile;
            queue.setIndex(queueFilePosition);
            return bufferForQueues[currentPosition++];
        }
        return null;
    }

    public void resetState() {
        this.currentPosition = 0;
        this.full = false;
        this.blockPositionInFile = -1;
        for(ByteBuffer bb:bufferForQueues) {
            bb.clear();
        }
    }
}
