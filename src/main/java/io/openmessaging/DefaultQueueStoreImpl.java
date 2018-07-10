package io.openmessaging;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constants.EMPTY;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {


    ConcurrentHashMap<String, RDPQueue> queueMap = new ConcurrentHashMap<>();

    FileManager fileManager = new FileManager();

    public void put(String queueName, byte[] message) {
        if (!queueMap.containsKey(queueName)) {
            queueMap.put(queueName, new RDPQueue(fileManager));
        }
        queueMap.get(queueName).add(message);
    }

    private static Object lock = new Object();

    public Collection<byte[]> get(String queueName, long offset, long num) {
        if(!fileManager.WriteDone) {
            synchronized (lock) {
                fileManager.inReadStage.set(true);
                while (!fileManager.WriteDone) {
                    try {
                        Thread.sleep(1l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        if (!queueMap.containsKey(queueName)) {
            return EMPTY;
        }
        return queueMap.get(queueName).getMessages((int)offset, (int) num);
    }
}
