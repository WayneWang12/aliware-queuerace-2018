package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

public interface Constants {
    int msgBatch = 10;
    public static final int bufferSize = 1024;
    public static final List<byte[]> EMPTY = new ArrayList<>();
}
