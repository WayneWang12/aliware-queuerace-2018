package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RDPQueue {

    int queueId;
    FileManager fileManager;
    ByteBuffer byteBuffer;

    public RDPQueue(int queueId, FileManager fileManager) {
        this.queueId = queueId;
        this.fileManager = fileManager;
        this.byteBuffer = fileManager.acquireQueueBuffer(queueId);
    }

    AtomicInteger msgCounter = new AtomicInteger();

    void add(byte[] msg) {
        byteBuffer.put((byte) msg.length);
        byteBuffer.put(msg);
        int msgCount = msgCounter.incrementAndGet();
        if (msgCount % 20 == 0) {
            flush();
        }
    }

    private void flush() {
        byteBuffer.position(byteBuffer.capacity());
        byteBuffer = fileManager.acquireQueueBuffer(queueId);
    }

    boolean firstGet = true;

    ArrayList<byte[]> getMessages(int offset, int num) {
        if (firstGet) {
            firstGet = false;
            flush();
            fileManager.inReadStage.set(true);
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
            messages.addAll(getMessages(end, offset + num - end * Constants.msgBatch, 0));
            return messages;
        }
    }

    private ArrayList<byte[]> getMessages(int indexPosition, int num, int offsetInBuffer) {
        ArrayList<Long> indexList = RDPBlock.rdpQueueIndexes.get(queueId);
        if (indexPosition < indexList.size()) {
            long filePosition = indexList.get(indexPosition);
            long bufferPosition = (filePosition % Constants.blockSize);
            long blockId = (filePosition / Constants.blockSize);
            return fileManager.getMessagesInBlock(blockId, (int) bufferPosition, offsetInBuffer, num);
        } else {
            return Constants.EMPTY;
        }
    }


}
