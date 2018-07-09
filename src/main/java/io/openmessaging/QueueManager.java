package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.NoSuchElementException;

public class QueueManager {


    final int queueId;
    ByteBuffer byteBuffer;
    Block lastBlock;

    static FileManager fileManager = new FileManager();
    static {
        fileManager.start();
    }

    static ThreadLocal<Block> blockThreadLocal = ThreadLocal.withInitial(fileManager::acquire);

    public QueueManager(int queueId) {
        this.queueId = queueId;
        updateToNewByteBuffer();
    }

    void updateToNewByteBuffer() {
        this.lastBlock = blockThreadLocal.get();
        this.byteBuffer = lastBlock.acquire();
        while (byteBuffer == null) {
            Block newBlock = fileManager.acquire();
            this.lastBlock = newBlock;
            this.byteBuffer = newBlock.acquire();
            blockThreadLocal.set(newBlock);
        }
    }

    void add(byte[] msg) {
        if (byteBuffer.remaining() < msg.length + 4) {
            byteBuffer.position(byteBuffer.capacity());
            lastBlock.notifyFull();
            updateToNewByteBuffer();
        }
        byteBuffer.putInt(msg.length);
        byteBuffer.put(msg);
    }

    ArrayList<byte[]> getMessages(long offset, long num) {
//       return new ArrayList<>();
        throw new NoSuchElementException("not implemented!");
    }

}
