package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RDPQueue {

    FileManager fileManager;
    ByteBuffer byteBuffer;
    long[] indexes = new long[100];
    byte currentIndex = 0;
    short queueSize = 0;

    void setIndex(long position) {
        if (currentIndex < 100) {
            indexes[currentIndex++] = position;
        }
    }

    public RDPQueue(FileManager fileManager) {
        this.fileManager = fileManager;
        this.byteBuffer = fileManager.acquireQueueBuffer(this);
    }

    AtomicInteger msgCounter = new AtomicInteger();

    void add(byte[] msg) {
        byteBuffer.put((byte) msg.length);
        byteBuffer.put(msg);
        int msgCount = msgCounter.incrementAndGet();
        if (msgCount % 20 == 0) {
            flush();
        }
        queueSize++;
    }

    private void flush() {
        byteBuffer.position(byteBuffer.capacity());
        this.byteBuffer = fileManager.acquireQueueBuffer(this);
    }


    ArrayList<byte[]> getMessages(int offset, int num) {
        if(offset >= queueSize) {
            return Constants.EMPTY;
        }
        return findMessagesInBlockByOffsetAndNumber(offset, num);
    }

    ArrayList<byte[]> findMessagesInBlockByOffsetAndNumber(int offset, int num) {
        int start = offset / Constants.msgBatch; //在索引中的位置;
        int end = (offset + num) / Constants.msgBatch; //最后一条消息在索引中的位置；
        int offsetInBuffer = offset % Constants.msgBatch;
        if (start == end) {
            return getMessages(start, num, offsetInBuffer);
        } else {
            ArrayList<byte[]> messages = getMessages(start, end * Constants.msgBatch - offset, offsetInBuffer);
            messages.addAll(getMessages(end, offset + num - end * Constants.msgBatch - 1, 0));
            return messages;
        }
    }


    private ArrayList<byte[]> getMessages(int indexPosition, int num, int offsetInBuffer) {
        if (indexPosition < indexes.length) {
            long filePosition = indexes[indexPosition];
            long bufferPosition = (filePosition % Constants.blockSize);
            long blockId = (filePosition / Constants.blockSize);
            return fileManager.getMessagesInBlock(blockId, (int) bufferPosition, offsetInBuffer, num);
        } else {
            return Constants.EMPTY;
        }
    }


}
