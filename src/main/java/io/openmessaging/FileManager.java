package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileManager {

    private FileChannel fileChannel;
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(128 * 1024);

    FileManager() {
        try {
            this.fileChannel =
                    new RandomAccessFile("/alidata1/race2018/data/" + Thread.currentThread().getName(), "rw")
                            .getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    void writeQueueBuffer(ByteBuffer byteBuffer) {
        if(writeBuffer.remaining() < byteBuffer.capacity()) {
            writeBuffer.position(writeBuffer.capacity());
            writeBuffer.flip();
            try {
                fileChannel.write(writeBuffer);
                writeBuffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        writeBuffer.put(byteBuffer);
    }

}
