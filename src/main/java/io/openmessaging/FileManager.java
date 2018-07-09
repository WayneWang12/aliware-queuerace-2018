package io.openmessaging;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscLinkedQueue7;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
    private MessagePassingQueue<Task> bufferQueue = new MpscLinkedQueue7<>();
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(64 * 1024);

    FileManager() {
        try {
            this.fileChannel =
                    new RandomAccessFile("/alidata1/race2018/data/" + Thread.currentThread().getName(), "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            while (true) {
                Task task = bufferQueue.poll();
                if (task == null) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (writeBuffer.remaining() < task.getMsg().capacity()) {
                        writeBuffer.position(writeBuffer.capacity());
                        writeBuffer.flip();
                        try {
                            fileChannel.write(writeBuffer);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        writeBuffer.clear();
                    }
                    writeBuffer.put(task.getMsg());
                    QueueManager.pool.release(task.getMsg());
                }
            }
        }).start();
    }

    void putMessage(int queueId, ByteBuffer msg) {
        bufferQueue.offer(new Task(queueId, msg));
    }

}
