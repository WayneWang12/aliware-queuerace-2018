package io.openmessaging;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class RDPQueue {

    FileManager fileManager;
    ByteBuffer byteBuffer;
    long[] indexes = new long[100];
    byte currentIndex = 0;
    short queueSize = 0;
    int queueId;

    void setIndex(long position) {
        if (currentIndex < 100) {
            indexes[currentIndex++] = position;
        }
    }

    static AtomicInteger currentQueueId = new AtomicInteger();

    public RDPQueue(FileManager fileManager) {
        this.queueId = currentQueueId.getAndIncrement();
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

    Collection<byte[]> getMessages(int offset, int num, boolean isSequentially) {
        if (offset >= queueSize) {
            return Constants.EMPTY;
        }
        ResultCache resultCache = resultCacheThreadLocal.get();
        if (resultCache == null) {
            resultCache = new ResultCache();
            resultCacheThreadLocal.set(resultCache);
        }
        resultCache.clear();
        if(isSequentially) {
            fillResultFromBlockCache(resultCache, offset, num);
        } else {
            fillResultWithMessages(resultCache, offset, num);
        }
        return resultCache.results;
    }

    void fillResultWithMessages(ResultCache resultCache, int offset, int num) {
        int start = offset / Constants.msgBatch; //在索引中的位置;
        int end = (offset + num) / Constants.msgBatch; //最后一条消息在索引中的位置；
        int offsetInBuffer = offset % Constants.msgBatch; //消息在buffer中的位置；
        int secondOffsetEnd = (offset + num) % Constants.msgBatch; //最后一条消息在索引中的位置；
        if (start == end) {
            fillResultDirectlyFromFile(resultCache, indexes[start], offsetInBuffer, num);
        } else {
            fillResultDirectlyFromFile(resultCache, indexes[start], offsetInBuffer, end * Constants.msgBatch - offset);
            if(end < currentIndex) {
                fillResultDirectlyFromFile(resultCache, indexes[end], 0, secondOffsetEnd);
            }
        }
    }

    void fillResultDirectlyFromFile(ResultCache resultCache, long positionInFile, int offset, int num) {
        try {
            fileManager.fileChannel.read(resultCache.fileReader, positionInFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        resultCache.fileReader.position(offset * Constants.msgSize);
        for (int i = 0; i < num; i++) {
            resultCache.fileReader.get(resultCache.results.next());
        }
        resultCache.fileReader.clear();
    }

    static Cache<Integer, RDPBlock> blockCache = Caffeine.newBuilder()
            .removalListener((Integer key, RDPBlock block, RemovalCause cause) -> {
                if(block != null) {
                    block.rdpBuffer.clear();
                    while (!FileManager.rdpBlocksPool.offer(block)){
                    }
                }
            })
            .maximumSize(46000) //49152
            .build();

    void fillResultFromBlockCache(ResultCache resultCache, int offset, int num)  {
        int index = offset / Constants.msgBatch; //在索引中的位置;
        int offsetInBuffer = offset % Constants.msgBatch; //消息在buffer中的位置；
        long filePosition = indexes[index];
        int blockId = (int) (filePosition / Constants.blockSize);
        RDPBlock block = getBlock(blockId);
        int positionInBuffer = (int) (filePosition % Constants.blockSize);
        ByteBuffer bb = block.rdpBuffer.duplicate();
        bb.position(positionInBuffer + offsetInBuffer * Constants.msgSize);
        for (int i = 0; i < num; i++) {
            bb.get(resultCache.results.next());
        }
    }

    private RDPBlock getBlock(int blockId) {
        RDPBlock block = blockCache.getIfPresent(blockId);
        if(block == null) {
            while ((block = FileManager.rdpBlocksPool.poll()) == null){
            }
            try {
                block.rdpBuffer.clear();
                fileManager.fileChannel.read(block.rdpBuffer, (long) blockId * Constants.blockSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            block.rdpBuffer.clear();
            blockCache.put(blockId, block);
        }
        return block;
    }


}
