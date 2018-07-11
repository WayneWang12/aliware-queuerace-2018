package io.openmessaging;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {


    RDPQueue[] queueMap = new RDPQueue[Constants.queueSize];
    private static final String PRE = "Queue-";
    FileManager fileManager = new FileManager();

    {
        for (int i = 0; i < queueMap.length; i++) {
            queueMap[i] = new RDPQueue(fileManager);
        }
    }

    int getIndex(String queueName) {
        return Integer.valueOf(queueName.substring(PRE.length()));
    }


    public void put(String queueName, byte[] message) {
        queueMap[getIndex(queueName)].add(message);
    }

    private static Object lock = new Object();

    AtomicInteger consumeCounter = new AtomicInteger();

    public Collection<byte[]> get(String queueName, long offset, long num) {

        if (!fileManager.WriteDone) {
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
        if(consumeCounter.get() <= Constants.indexCheckNumber) {
            consumeCounter.getAndIncrement();
            return queueMap[getIndex(queueName)].getMessages((int) offset, (int) num, false);
        } else {
            return queueMap[getIndex(queueName)].getMessages((int) offset, (int) num, true);
        }

    }
}
