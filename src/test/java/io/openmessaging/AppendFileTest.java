package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AppendFileTest {

    public static void main(String[] args) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile( new File("appendFile.txt"),"rw");
        FileChannel fc = randomAccessFile.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(3);

        fc.write(buf);

        buf.clear();
        buf.put((byte) 100);
        buf.position(buf.capacity());   //put index to the end
        buf.flip();

        fc.write(buf);
    }

}
