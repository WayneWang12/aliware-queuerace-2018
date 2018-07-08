package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.*;

public class FileManager {

    private final int bufferSize = 128;

    private FileChannel fileChannel;
    private ByteBuffer[] writeBuffer = new ByteBuffer[bufferSize];

    FileManager() {
        for(int n = 0; n < bufferSize; n ++) {
            writeBuffer[n] = ByteBuffer.allocateDirect(1024);
        }
        try {
            this.fileChannel =
                    FileChannel.open(Paths.get("/alidata1/race2018/data/" + Thread.currentThread().getName()), CREATE, READ, WRITE, DELETE_ON_CLOSE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    int currentPosition = 0;

    ByteBuffer putMessage(ByteBuffer msg) {
        if(currentPosition >= bufferSize) {
            try {
                fileChannel.write(writeBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            currentPosition = 0;
        }
        ByteBuffer replace = writeBuffer[currentPosition];
        msg.position(msg.capacity());
        msg.flip();
        writeBuffer[currentPosition++] = msg;
        replace.clear();
        return replace;
    }

}
