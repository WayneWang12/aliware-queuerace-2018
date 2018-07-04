package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.utils.Config.bufferSize;

public class FileManager {

    private String threadName;

    private String filePath(int fileId) {
        return "/alidata1/race2018/data/" + threadName + "-file-" + fileId;
    }

    private Map<Integer, MappedByteBuffer> fileChannelMap = new ConcurrentHashMap<>();

    private AtomicInteger currentFileId = new AtomicInteger();
    private final int MAP_SIZE = Integer.MAX_VALUE;
    private MappedByteBuffer currentMappedByteBuffer;

    public FileManager(String threadName) {
        this.threadName = threadName;
        updateState();
    }

    private void updateState() {
        int id = currentFileId.incrementAndGet();
        this.currentMappedByteBuffer = mappedBufferById(id);
        fileChannelMap.put(id, currentMappedByteBuffer);
        positionCount.set(0);
    }

    private AtomicInteger positionCount = new AtomicInteger();
    public int[] put(ByteBuffer buffer) {
            int position = positionCount.getAndIncrement() * bufferSize;
            if (Integer.MAX_VALUE - position < buffer.capacity()) {
                updateState();
                position = positionCount.getAndIncrement() * bufferSize;
            }
            currentMappedByteBuffer.position(position);
            currentMappedByteBuffer.put(buffer);
            int fid = currentFileId.get();
            return new int[]{fid, position};
    }

    public void get(int fileId, int startPosition, byte[] dst) {
        MappedByteBuffer mmap = fileChannelMap.get(fileId);
        mmap.position(startPosition);
        mmap.get(dst);
    }

    private FileChannel mappedFileChannel(int id) throws IOException {
        return FileChannel.open(
                Paths.get(filePath(id)),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );
    }

    private MappedByteBuffer mappedBufferById(int id) {
        try {
            return mappedFileChannel(id).map(FileChannel.MapMode.READ_WRITE, 0, MAP_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
