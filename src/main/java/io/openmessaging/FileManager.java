package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.nio.file.StandardOpenOption.*;

public class FileManager {

    static class Task {
        private int queueId;
        private ByteBuffer msg;

        public Task(int queueId, ByteBuffer msg) {
            this.queueId = queueId;
            this.msg = msg;
        }

        public int getQueueId() {
            return queueId;
        }

        public ByteBuffer getMsg() {
            return msg;
        }
    }

    private FileChannel fileChannel;
    private BlockingQueue<Task> bufferQueue = new LinkedBlockingQueue<>();
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(64 * 1024);

    FileManager() {
        try {
            this.fileChannel =
                    FileChannel.open(Paths.get("/alidata1/race2018/data/" + Thread.currentThread().getName()), CREATE, READ, WRITE, DELETE_ON_CLOSE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            while (true) {
                try {
                    Task task = bufferQueue.take();
                    if(writeBuffer.remaining() < task.getMsg().capacity()) {
                        writeBuffer.position(writeBuffer.capacity());
                        writeBuffer.flip();
                        fileChannel.write(writeBuffer);
                        writeBuffer.clear();
                    }
                    writeBuffer.put(task.getMsg());
                    QueueManager.pool.release(task.getMsg());
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void putMessage(int queueId, ByteBuffer msg) {
        try {
            bufferQueue.put(new Task(queueId, msg));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
