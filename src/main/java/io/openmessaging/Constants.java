package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

public interface Constants {
    int msgBatch = 20;
    int bufferSize = 1024;
    int blockSize = 64 * 1024;
    int blockNumber = Integer.MAX_VALUE / blockSize / 2 * 3;
    String filePath = "/alidata1/race2018/data/rdp";
    List<byte[]> EMPTY = new ArrayList<>();
}
