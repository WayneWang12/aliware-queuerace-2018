package io.openmessaging;

import io.openmessaging.utils.BinarySearch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.utils.Config.bufferSize;

public class ReadBuffer {
    private ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    private FileManager fileManager;
    private ArrayList<int[]> indexes = new ArrayList<>();

    public ReadBuffer(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public void addIndex(int msgOffset, int msgEnd, int fileId, int startPosition) {
        indexes.add(new int[]{msgOffset, msgEnd, fileId, startPosition});
    }

    int startMsgOffset = -1;
    int endMsgOffset = -1;
    int checkpoint = 0;

    public void init() {
        updateBufferByOffset(0);
    }

    public synchronized List<byte[]> getMessages(long msgOffset, int num) {
        if (msgOffset > endMsgOffset || msgOffset < startMsgOffset) { //如果msgOffset大于当前，则需要更新index
            updateBufferByOffset(msgOffset);
        }
        List<byte[]> result = new ArrayList<>();
        if (startMsgOffset <= msgOffset && msgOffset + num <= endMsgOffset) {
            getMessageFromBuffer(msgOffset, num, result);
        } else if (startMsgOffset <= msgOffset && msgOffset + num > endMsgOffset) {
            getMessageFromBuffer(msgOffset, endMsgOffset, result);
            updateBufferByOffset(endMsgOffset + 1);
            getMessageFromBuffer(endMsgOffset + 1, (int) (msgOffset + num - endMsgOffset), result);
        }
        return result;
    }

    private synchronized void updateBufferByOffset(long msgOffset) {
        int index = BinarySearch.binarySearch(indexes, msgOffset);
        if (index < indexes.size()) {
            int[] currentIndex = indexes.get(index);
            this.startMsgOffset = currentIndex[0];
            this.endMsgOffset = currentIndex[1];
            this.checkpoint = this.startMsgOffset;
            buffer.clear();
            fileManager.get(currentIndex[2], currentIndex[3], buffer.array());
        }
    }

    private void resetBuffer() {
        checkpoint = startMsgOffset;
        buffer.position(0);
    }

    private void getMessageFromBuffer(long msgOffset, int num, List<byte[]> result) {
        //如果当前消息读取已经越过msgOffset，则重置buffer
        if (msgOffset < endMsgOffset) {
            if (checkpoint > msgOffset) {
                resetBuffer();
            }
            while (checkpoint < msgOffset) {
                step();
                checkpoint++;
            }
            while (checkpoint < msgOffset + num && checkpoint < endMsgOffset) {
                result.add(getMessage());
                checkpoint++;
            }
        }
    }

    private byte[] getMessage() {
        int messageSize = buffer.getInt();
        byte[] compressedMessage = new byte[messageSize];
        buffer.get(compressedMessage);
        return compressedMessage;
    }


    private void step() {
        int messsageSize = buffer.getInt();
        buffer.position(buffer.position() + messsageSize);
    }
}
