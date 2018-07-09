package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class FileManager implements Runnable {

    ArrayList<Block> blockArrayList = new ArrayList<>(Constants.blockNumber);
    ConcurrentLinkedQueue<Block> blocksPool = new ConcurrentLinkedQueue<>();
    FileChannel fileChannel;

    FileManager() {
        try {
            this.fileChannel = new RandomAccessFile(Constants.filePath, "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for(int i = 0; i< Constants.blockNumber; i++) {
            Block block = new Block(ByteBuffer.allocateDirect(Constants.blockSize));
            blockArrayList.add(block);
            blocksPool.add(block);
        }
    }

    AtomicInteger currentBlockId = new AtomicInteger();

    Block acquire() {
        Block block;
        while ((block = blocksPool.poll()) == null);
        block.blockId = currentBlockId.getAndIncrement();
        return block;
    }

    void start() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        while (true) {
            for(int i = 0; i < blockArrayList.size(); i ++) {
                Block block = blockArrayList.get(i);
                if(block.isFull()) {
                    block.blockBuffer.clear();
                    try {
                        fileChannel.write(block.blockBuffer, (long)block.blockId * Constants.blockSize);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    block.resetState();
                    while (!blocksPool.offer(block));
                }
            }
            try {
                Thread.sleep(1l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

