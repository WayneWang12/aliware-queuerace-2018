package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager {

    static ArrayList<Block> blockArrayList = new ArrayList<>(Constants.blockNumber);
    public static ConcurrentLinkedQueue<Block> blocksPool = new ConcurrentLinkedQueue<>();

    ConcurrentHashMap<Integer, Block> readCache = new ConcurrentHashMap<>(Constants.blockNumber);

    FileChannel fileChannel;

    FileManager() {
        try {
            this.fileChannel = new RandomAccessFile(Constants.filePath, "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < Constants.blockNumber; i++) {
            Block block = new Block(ByteBuffer.allocateDirect(Constants.blockSize));
            blockArrayList.add(block);
            blocksPool.add(block);
        }
    }

    AtomicInteger currentBlockId = new AtomicInteger();

    Block acquire() {
        Block block;
        while ((block = blocksPool.poll()) == null) ;
        block.blockId = currentBlockId.getAndIncrement();
        return block;
    }

    AtomicBoolean inReadStage = new AtomicBoolean(false);
    AtomicInteger readBlock = new AtomicInteger(0);

    Block getBlockById(int blockId) {
        Block block = readCache.get(blockId);
        if (block == null) {
            while ((block = blocksPool.poll()) == null) ;
            try {
                block.blockBuffer.clear();
                fileChannel.read(block.blockBuffer, (long) blockId * Constants.blockSize);
                block.blockId = blockId;
                block.blockBuffer.clear();
                block.reloadDataFromBlock();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return block;
    }

    static  AtomicBoolean needWrite = new AtomicBoolean(true);

    class FlushTask implements Runnable {
        private int id;
        private int step;

        FlushTask(int id, int step) {
            this.id = id;
            this.step = step;
        }

        boolean firstRead = true;

        @Override
        public void run() {
            while (true) {
                if (inReadStage.get()) {
                    if(firstRead) {
                        firstRead = false;
                        findFullBlockAndWrite();
                    }
                    preReadBlock();
                } else {
                    findFullBlockAndWrite();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private void preReadBlock() {
            Block block;
            while ((block = blocksPool.poll()) == null) ;
            try {
                int blockId = readBlock.getAndIncrement();
                block.blockBuffer.clear();
                fileChannel.read(block.blockBuffer, (long) blockId * Constants.blockSize);
                block.reloadDataFromBlock();
                block.blockId = blockId;
                readCache.put(blockId, block);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        void findBlockAndWrite() {
             for (int i = id; i < blockArrayList.size(); i += step) {
                Block block = blockArrayList.get(i);
                if (block.currentPosition > 0) {
                    block.blockBuffer.clear();
                    try {
                        long position = (long) block.blockId * Constants.blockSize;
                        fileChannel.write(block.blockBuffer, position);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    block.resetState();
                    while (!blocksPool.offer(block)) ;
                }
            }
        }

        private void findFullBlockAndWrite() {
            for (int i = id; i < blockArrayList.size(); i += step) {
                Block block = blockArrayList.get(i);
                if (block.isFull()) {
                    block.blockBuffer.clear();
                    try {
                        long position = (long) block.blockId * Constants.blockSize;
                        fileChannel.write(block.blockBuffer, position);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    block.resetState();
                    while (!blocksPool.offer(block)) ;
                }
            }
        }
    }

    void start() {
        int step = 4;
        for (int n = 0; n < step; n++) {
            new Thread(new FlushTask(n, step)).start();
        }
    }

}
