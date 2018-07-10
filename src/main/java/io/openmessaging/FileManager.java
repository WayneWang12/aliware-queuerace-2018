package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager {

    ArrayList<Block> blockArrayList = new ArrayList<>(Constants.blockNumber);
    ConcurrentLinkedQueue<Block> blocksPool = new ConcurrentLinkedQueue<>();
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

    class FlushTask implements Runnable {
        private int id;
        private int step;

        FlushTask(int id, int step) {
            this.id = id;
            this.step = step;
        }

        private boolean firstRead = true;

        @Override
        public void run() {
            while (true) {
                if (inReadStage.get()) {
                    if(firstRead) {
                        firstRead = false;
                        findFullBlockAndWrite();
                        System.out.println("write is done!");
                    }
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

        private void findFullBlockAndWrite() {
            for (int i = id; i < blockArrayList.size(); i += step) {
                Block block = blockArrayList.get(i);
                if (block.isFull()) {
                    block.blockBuffer.clear();
                    try {
                        fileChannel.write(block.blockBuffer, (long) block.blockId * Constants.blockSize);
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
        for (int n = 0; n < 4; n++) {
            new Thread(new FlushTask(n, 4)).start();
        }
    }

}
