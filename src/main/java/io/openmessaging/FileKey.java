package io.openmessaging;

import java.util.Objects;

public class FileKey {
    private final int fileId;
    private final int blockId;

    public FileKey(int fileId, int blockId) {
        this.fileId = fileId;
        this.blockId = blockId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileKey)) return false;
        FileKey fileKey = (FileKey) o;
        return fileId == fileKey.fileId &&
                blockId == fileKey.blockId;
    }

    @Override
    public int hashCode() {

        return Objects.hash(fileId, blockId);
    }

    public int getFileId() {
        return fileId;
    }

    public int getBlockId() {
        return blockId;
    }
}
