package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
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


    void add(byte[] msg) {
        byteBuffer.put(msg);
        if (++queueSize % 20 == 0) {
            flush();
        }
    }

    private void flush() {
        this.byteBuffer = fileManager.acquireQueueBuffer(this);
    }

    static ThreadLocal<ResultCache> resultCacheThreadLocal = new ThreadLocal<>();

    Collection<byte[]> getMessages(int offset, int num) {
        if(offset >= queueSize) {
            return Constants.EMPTY;
        }
        ResultCache resultCache = resultCacheThreadLocal.get();
        if(resultCache == null) {
            resultCache = new ResultCache();
            resultCacheThreadLocal.set(resultCache);
        }
        resultCache.clear();
        fillResultWithMessages(resultCache, offset, num);
        return resultCache.results;
    }

    void fillResultWithMessages(ResultCache resultCache, int offset, int num) {
        if(offset >= queueSize) {
            return;
        }
        int start = offset / Constants.msgBatch; //在索引中的位置;
        int end = (offset + num) / Constants.msgBatch; //最后一条消息在索引中的位置；
        int offsetInBuffer = offset % Constants.msgBatch;
        if (start == end) {
            fillResultInABlock(resultCache, indexes[start], offsetInBuffer, num);
        } else {
            fillResultInABlock(resultCache, indexes[start], end * Constants.msgBatch - offset, num);
            fillResultInABlock(resultCache, indexes[end], 0, offset + num - end * Constants.msgSize);
        }
    }

    void fillResultInABlock(ResultCache resultCache, long positionInFile, int offset, int num) {
        try {
            fileManager.fileChannel.read(resultCache.fileReader, positionInFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        resultCache.fileReader.position(offset * Constants.msgSize);
        for(int i = 0; i < num; i ++) {
            resultCache.fileReader.get(resultCache.results.next());
        }
    }

//    ArrayList<byte[]> findMessagesInBlockByOffsetAndNumber(int offset, int num) {
//        int start = offset / Constants.msgBatch; //在索引中的位置;
//        int end = (offset + num) / Constants.msgBatch; //最后一条消息在索引中的位置；
//        int offsetInBuffer = offset % Constants.msgBatch;
//        if (start == end) {
//            return getMessages(start, num, offsetInBuffer);
//        } else {
//            ArrayList<byte[]> messages = getMessages(start, end * Constants.msgBatch - offset, offsetInBuffer);
//            messages.addAll(getMessages(end, offset + num - end * Constants.msgBatch, 0));
//            return messages;
//        }
//    }
//
//
//    private ArrayList<byte[]> getMessages(int indexPosition, int num, int offsetInBuffer) {
//        if (indexPosition < indexes.length) {
//            long filePosition = indexes[indexPosition];
//            long bufferPosition = (filePosition % Constants.blockSize);
//            long blockId = (filePosition / Constants.blockSize);
//            return fileManager.getMessagesInBlock(blockId, (int) bufferPosition, offsetInBuffer, num);
//        } else {
//            return Constants.EMPTY;
//        }
//    }


}
