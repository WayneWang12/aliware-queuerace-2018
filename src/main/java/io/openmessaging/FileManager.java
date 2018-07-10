package io.openmessaging;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FileManager {
    FileChannel fileChannel;
    ArrayList<RDPBlock> rdpBlockArrayList;
    static ConcurrentLinkedQueue<RDPBlock> rdpBlocksPool = new ConcurrentLinkedQueue<>();
    long blockNumber = Constants.MAX_DIRECT_BUFFER_SIZE / Constants.blockSize;

    FileManager() {
        this.rdpBlockArrayList = new ArrayList<>();
        try {
            this.fileChannel = new RandomAccessFile(Constants.filePath, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < blockNumber; i++) {
            RDPBlock rdpBlock = new RDPBlock(ByteBuffer.allocateDirect((int) Constants.blockSize));
            rdpBlockArrayList.add(rdpBlock);
            rdpBlocksPool.add(rdpBlock);
        }

        int step = 4;

        for (int i = 0; i < step; i++) {
            new Thread(new Flusher(i, step)).start();
        }
    }

    ThreadLocal<RDPBlock> rdpBlockThreadLocal = new ThreadLocal<>();

    AtomicLong currentFilePosition = new AtomicLong();

    AtomicBoolean inReadStage = new AtomicBoolean(false);

    ByteBuffer acquireQueueBuffer(RDPQueue queue) {
        RDPBlock rdpBlock = rdpBlockThreadLocal.get();
        ByteBuffer queueBuffer;
        if (rdpBlock == null || (queueBuffer = rdpBlock.acquireQueueBuffer(queue)) == null) {
            while ((rdpBlock = rdpBlocksPool.poll()) == null) {
            }
            rdpBlock.blockPositionInFile = currentFilePosition.getAndAdd(Constants.blockSize);
            queueBuffer = rdpBlock.acquireQueueBuffer(queue);
            rdpBlockThreadLocal.set(rdpBlock);
        }
        return queueBuffer;
    }

    Cache<Integer, RDPBlock> readCache = Caffeine.newBuilder()
            .removalListener((Integer key, RDPBlock block, RemovalCause cause) -> {
                block.rdpBuffer.clear();
                while (!rdpBlocksPool.offer(block)) ;
            })
            .expireAfterAccess(1, TimeUnit.SECONDS)
            .maximumSize(blockNumber / 2)
            .softValues()
            .build();

    ArrayList<byte[]> getMessagesInBlock(long blockId, int offsetInBlock, int msgOffsetInBlock, int num) {
        RDPBlock block = readCache.getIfPresent(blockId);
        if (block == null) {
            while ((block = rdpBlocksPool.poll()) == null) {
                System.out.println("run out of block.");
            }
            long filePosition = blockId * Constants.blockSize;
            try {
                block.rdpBuffer.clear();
                fileChannel.read(block.rdpBuffer, filePosition);
                readCache.put((int) blockId, block);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ByteBuffer bb = block.rdpBuffer.duplicate();
        bb.position(offsetInBlock);
        for (int i = 0; i < msgOffsetInBlock; i++) {
            int length = bb.get();
            bb.position(bb.position() + length);
        }
        ArrayList<byte[]> messages = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            int length = bb.get();
            if (length != 0) {
                byte[] msg = new byte[length];
                bb.get(msg);
                messages.add(msg);
            }
        }
        return messages;
    }

    volatile boolean WriteDone = false;

    class Flusher implements Runnable {

        private int id;
        private int step;

        public Flusher(int id, int step) {
            this.id = id;
            this.step = step;
        }

        void flushFullRdpBlocks() {
            for (int i = id; i < rdpBlockArrayList.size(); i += step) {
                RDPBlock rdpBlock = rdpBlockArrayList.get(i);
                if (rdpBlock.isFull()) {
                    rdpBlock.rdpBuffer.clear();
                    try {
                        fileChannel.write(rdpBlock.rdpBuffer, rdpBlock.blockPositionInFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    rdpBlock.resetState();
                    while (!rdpBlocksPool.offer(rdpBlock)) {
                    }
                }
            }
        }

        void flushDirtyRdpBlocks() {
            for (int i = id; i < rdpBlockArrayList.size(); i += step) {
                RDPBlock rdpBlock = rdpBlockArrayList.get(i);
                if (rdpBlock.blockPositionInFile != -1) {
                    rdpBlock.rdpBuffer.clear();
                    try {
                        fileChannel.write(rdpBlock.rdpBuffer, rdpBlock.blockPositionInFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    rdpBlock.resetState();
                    while (!rdpBlocksPool.offer(rdpBlock)) {
                    }
                }
            }
        }


        @Override
        public void run() {
            while (true) {
                if (inReadStage.get()) {
                    flushDirtyRdpBlocks();
                    WriteDone = true;
                    System.out.println("flush task " + id + " completed.");
                    return;
                } else {
                    flushFullRdpBlocks();
                }
            }
        }
    }
}
