package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

public interface Constants {
    int msgBatch = 20;
    long bufferSize = 1024;
    long blockSize = 64 * 1024;
    long MAX_DIRECT_BUFFER_SIZE = 3 * 1024 * 1024 * 1024l;
    String filePath = "/alidata1/race2018/data/rdp";
    ArrayList<byte[]> EMPTY = new ArrayList<>();
}
