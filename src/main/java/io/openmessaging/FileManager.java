package io.openmessaging;

import io.openmessaging.utils.LRUCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager {

    int blockSize = 64 * 1024;
    private FileChannel fileChannel;
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(blockSize);
    static ConcurrentMap<Integer, ArrayList<int[]>> queueIndex = new ConcurrentHashMap<>();
    static ConcurrentMap<Integer, FileChannel> fileChannelConcurrentMap = new ConcurrentHashMap<>();
    static AtomicInteger globalFileId = new AtomicInteger();

    private int fileId;

    FileManager() {
        this.fileId = globalFileId.getAndIncrement();
        try {
            this.fileChannel =
                    new RandomAccessFile("/alidata1/race2018/data/" + Thread.currentThread().getName(), "rw").getChannel();
            fileChannelConcurrentMap.putIfAbsent(fileId, fileChannel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private AtomicInteger currentBlock = new AtomicInteger();

    private AtomicInteger lastPutCounter = new AtomicInteger();

    void lastPut(int queueId, ByteBuffer msg) {
        putMessage(queueId, msg);
        int lastPutNumber = lastPutCounter.incrementAndGet();
        if (lastPutNumber == DefaultQueueStoreImpl.queueMap.size()) {
            writeBuffer.position(writeBuffer.capacity());
            writeBuffer.flip();
            try {
                fileChannel.write(writeBuffer, currentBlock.getAndIncrement() * blockSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            writeBuffer.clear();
            writeBuffer = null;
            currentBlock.getAndIncrement();
        }
    }

    void putMessage(int queueId, ByteBuffer msg) {
        if (writeBuffer.remaining() < msg.capacity()) {
            writeBuffer.position(writeBuffer.capacity());
            writeBuffer.flip();
            try {
                fileChannel.write(writeBuffer, (long)currentBlock.getAndIncrement() * blockSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            writeBuffer.clear();
            currentBlock.getAndIncrement();
        }
        int position = writeBuffer.position();
        int[] index = {fileId, currentBlock.get(), position};
        if (!queueIndex.containsKey(queueId)) {
            queueIndex.put(queueId, new ArrayList<>());
        }
        queueIndex.get(queueId).add(index);
        writeBuffer.put(msg);
    }


    ArrayList<byte[]> getMessage(int queueId, int offset, int num) {
        ArrayList<int[]> indexes = queueIndex.get(queueId);
        int start = offset / Constants.msgBatch;
        int end = (offset + num) / Constants.msgBatch;
        if (start == end) {
            return getMessagesByIndex(indexes.get(start), start, offset, num);
        } else {
            ArrayList<byte[]> messages = getMessagesByIndex(indexes.get(start), start, offset, (start + 1) * Constants.msgBatch - offset);
            messages.addAll(getMessagesByIndex(indexes.get(end), end, 0, offset + num - end * Constants.msgBatch));
            return messages;
        }
    }

    static LRUCache<FileKey, MappedByteBuffer> lruCache = new LRUCache<>(1000);

    ArrayList<byte[]> getMessagesByIndex(int[] ints, int index, int offset, int num) {
        FileKey fileKey = new FileKey(ints[0], ints[1]);
        return getMessagesBy(fileKey, ints[2], offset - index * Constants.msgBatch, num);
    }

    ArrayList<byte[]> getMessagesBy(FileKey key, int positionInBlock, int offset, int num) {
        if (!lruCache.containsKey(key)) {
            try {
                MappedByteBuffer mappedByteBuffer = fileChannelConcurrentMap.get(key.getFileId()).map(FileChannel.MapMode.READ_ONLY, (long)key.getBlockId() * blockSize, blockSize);
                lruCache.put(key, mappedByteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ByteBuffer byteBuffer = lruCache.get(key).duplicate();
        byteBuffer.position(positionInBlock);
        for (int i = 0; i < offset; i++) {
            int length = byteBuffer.getInt();
            byteBuffer.position(byteBuffer.position() + length);
        }
        ArrayList<byte[]> result = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            int length = byteBuffer.getInt();
            byte[] msg = new byte[length];
            byteBuffer.get(msg);
            result.add(msg);
        }
        return result;
    }

}

