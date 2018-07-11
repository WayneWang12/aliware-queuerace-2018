package io.openmessaging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
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

        int step = 1;

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
