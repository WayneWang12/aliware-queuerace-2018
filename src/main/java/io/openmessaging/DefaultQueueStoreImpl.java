package io.openmessaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

    Map<String, QueueManager> queueMap = new ConcurrentHashMap<>();
    FileManager fileManager = new FileManager();

    AtomicInteger queueId = new AtomicInteger();

    public void put(String queueName, byte[] message) {
        if (!queueMap.containsKey(queueName)) {
            queueMap.put(queueName, new QueueManager(queueId.getAndIncrement(), fileManager));
        }
        queueMap.get(queueName).add(message);
    }
    public synchronized Collection<byte[]> get(String queueName, long offset, long num) {
        if (!queueMap.containsKey(queueName)) {
            return EMPTY;
        }
        QueueManager queue = queueMap.get(queueName);
        throw new NoSuchElementException("not implemented");
    }
}
