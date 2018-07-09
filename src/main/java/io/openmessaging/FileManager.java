package io.openmessaging;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscLinkedQueue7;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileManager {

    private FileChannel fileChannel;
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(64 * 1024);

    FileManager() {
        try {
            this.fileChannel =
                    new RandomAccessFile("/alidata1/race2018/data/" + Thread.currentThread().getName(), "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void putMessage(int queueId, ByteBuffer msg) {
        if (writeBuffer.remaining() < msg.capacity()) {
            writeBuffer.position(writeBuffer.capacity());
            writeBuffer.flip();
            try {
                fileChannel.write(writeBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            writeBuffer.clear();
        }
        writeBuffer.put(msg);
    }

}
