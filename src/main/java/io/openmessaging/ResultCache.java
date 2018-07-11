package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

public class ResultCache {

    public HackCollection<byte[]> results;

    ResultCache() {
        this.results = new HackCollection<>(10);
        for(int i = 0; i < 10; i++) {
            results.add(new byte[58]);
        }
        results.clear();
    }

    ByteBuffer fileReader = ByteBuffer.allocate((int) Constants.bufferSize);

    void clear() {
        results.clear();
        fileReader.clear();
    }

}
