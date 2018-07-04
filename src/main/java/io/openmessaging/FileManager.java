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

public class FileManager {

    private String filePath(int fileId) {
        return "/alidata1/race2018/data/file-" + fileId;
    }

    private Map<Integer, MappedByteBuffer> fileChannelMap = new ConcurrentHashMap<>();


    private AtomicInteger currentFileId = new AtomicInteger();
    private final long MAP_SIZE = Integer.MAX_VALUE;
    private MappedByteBuffer currentMappedByteBuffer;

    public FileManager() {
        updateState();
    }

    private void updateState() {
        int id = currentFileId.incrementAndGet();
        this.currentMappedByteBuffer = mappedBufferById(id);
        fileChannelMap.put(id, currentMappedByteBuffer);
    }

    public int[] put(ByteBuffer buffer) {
        synchronized (this) {
            if (currentMappedByteBuffer.remaining() < buffer.capacity()) {
                updateState();
            }
            int position = currentMappedByteBuffer.position();
            int fid = currentFileId.get();
            currentMappedByteBuffer.put(buffer);
            return new int[]{fid, position};
        }
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
