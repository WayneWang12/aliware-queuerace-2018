package io.openmessaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    private ThreadLocal<FileManager> fileManager = ThreadLocal.withInitial(() -> new FileManager(Thread.currentThread().getName()));//new FileManager();


    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

    Map<String, QueueHolder> queueMap = new ConcurrentHashMap<>();

    AtomicInteger queueId = new AtomicInteger();

    public void put(String queueName, byte[] message) {
        if (!queueMap.containsKey(queueName)) {
            queueMap.put(queueName, new QueueHolder(fileManager.get()));
        }
        queueMap.get(queueName).add(message);
    }
    public synchronized Collection<byte[]> get(String queueName, long offset, long num) {
        if (!queueMap.containsKey(queueName)) {
            return EMPTY;
        }
        QueueHolder queue = queueMap.get(queueName);
        return queue.get(offset, num);
    }
}
