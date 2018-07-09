package io.openmessaging.utils;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static io.openmessaging.Config.*;

public class DirectByteBufferPool {
    private int maxPoolEntries;
    private int buffersInPool = 0;
    private final ByteBuffer[] pool;

    public DirectByteBufferPool(int maxPoolEntries) {
        this.maxPoolEntries = maxPoolEntries;
        this.pool = new ByteBuffer[maxPoolEntries];
    }

    public ByteBuffer acquire() {
        return takeBufferFromPool();
    }

    public void release(ByteBuffer bb) {
        offerBufferToPool(bb);
    }

    private ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size);
    }

    private ByteBuffer takeBufferFromPool() {
        ByteBuffer buffer = null;
        synchronized (pool) {
            if (buffersInPool > 0) {
                buffersInPool--;
                buffer = pool[buffersInPool];
            }
        }
        if (buffer == null)
            return allocate(bufferSize);
        else {
            buffer.clear();
            return buffer;
        }

    }

    private void offerBufferToPool(ByteBuffer buf) {
        boolean clean = false;
        synchronized (pool) {
            if (buffersInPool < maxPoolEntries) {
                pool[buffersInPool] = buf;
                buffersInPool++;
            } else {
                clean = true;
            }
        }
        if (clean) tryCleanDirectByteBuffer(buf);
    }


    private void tryCleanDirectByteBuffer(ByteBuffer buf) {
        try {
            if (buf.isDirect()) {
                Method cleanerMethod = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");

                cleanerMethod.setAccessible(true);

                Method cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean");
                cleanMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(buf);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception exeption) {
            exeption.printStackTrace();
        }
    }
}
