package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class RDPBlock {
    ByteBuffer rdpBuffer;
    ByteBuffer[] bufferForQueues;
    int bufferNumber;
    int currentPosition = 0;
    long blockPositionInFile = -1;
    int fullQueueNumber = 0;

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

    void notifyFull() {
        fullQueueNumber++;
    }

    public boolean isFull() {
        return fullQueueNumber == bufferNumber;
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
        this.fullQueueNumber = 0;
        this.blockPositionInFile = -1;
        for(ByteBuffer bb:bufferForQueues) {
            bb.clear();
        }
    }
}
