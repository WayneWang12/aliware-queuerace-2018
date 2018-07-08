package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.*;

public class FileManager {

    private FileChannel fileChannel;
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(32 * 1024);

    FileManager() {
        try {
            this.fileChannel =
                    FileChannel.open(Paths.get("/alidata1/race2018/data/" + Thread.currentThread().getName()), CREATE, APPEND, DELETE_ON_CLOSE);
        } catch (IOException e) {
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
