package io.openmessaging;

import java.nio.ByteBuffer;

public class ResultCache {

    public HackCollection<byte[]> results;

    ResultCache() {
        this.results = new HackCollection<>(10);
        for(int i = 0; i < 10; i++) {
            results.add(new byte[58]);
        }
        results.clear();
    }

    ByteBuffer fileReader = ByteBuffer.allocateDirect((int) Constants.bufferSize);

    void clear() {
        results.clear();
        fileReader.clear();
    }

}
