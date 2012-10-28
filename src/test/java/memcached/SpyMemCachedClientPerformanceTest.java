package memcached;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.MemcachedClient;

import org.junit.BeforeClass;
import org.junit.Test;

public class SpyMemCachedClientPerformanceTest {
    private static MemcachedClient mcc;

    @Test
    public void testSingleThreadSet() {
        SpyMemSetThread memThread = new SpyMemSetThread(0, 1, null);
        memThread.run();
    }

    @Test
    public void test10ThreadsSet() throws InterruptedException {
        startSetThreads(10);
    }

    @Test
    public void test20ThreadsSet() throws InterruptedException {
        startSetThreads(20);
    }

    @Test
    public void test30ThreadsSet() throws InterruptedException {
        startSetThreads(30);
    }

    @Test
    public void test40ThreadsSet() throws InterruptedException {
        startSetThreads(40);
    }

    @Test
    public void test50ThreadsSet() throws InterruptedException {
        startSetThreads(50);
    }

    @Test
    public void test100ThreadsSet() throws InterruptedException {
        startSetThreads(100);
    }

    @Test
    public void test200ThreadsSet() throws InterruptedException {
        startSetThreads(200);
    }

    @Test
    public void test500ThreadsSet() throws InterruptedException {
        startSetThreads(500);
    }

    @Test
    public void test1000ThreadsSet() throws InterruptedException {
        startSetThreads(1000);
    }

    private void startSetThreads(int threadsNum) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(threadsNum);
        for (int j = 0, jj = threadsNum; j < jj; ++j)
            ClientUtils.threadPool.submit(new SpyMemSetThread(j, threadsNum, countDownLatch));

        countDownLatch.await(2, TimeUnit.SECONDS);
    }

    @BeforeClass
    public static void setup() {
        mcc = ClientUtils.getSpyClient();
    }

    public static class SpyMemSetThread implements Runnable {
        private int index;
        private int threadsNum;
        private CountDownLatch countDownLatch;

        public SpyMemSetThread(int index, int threadsNum, CountDownLatch countDownLatch) {
            this.index = index;
            this.threadsNum = threadsNum;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                for (int i = 0, ii = ClientUtils.MAX_OPERATION_NUM / threadsNum; i < ii; ++i) {
                    String key = index + ".s." + i;
                    ClientUtils.setSpyCache(mcc, key, 0, key);
                }
            } finally {
                if (countDownLatch != null) countDownLatch.countDown();
            }
        }
    }

    @Test
    public void testSingleThreadGet() {
        SpyMemGetThread memThread = new SpyMemGetThread(0, 1, null);
        memThread.run();
    }

    @Test
    public void test10ThreadsGet() throws InterruptedException {
        startGetThreads(10);
    }

    @Test
    public void test20ThreadsGet() throws InterruptedException {
        startGetThreads(20);
    }

    @Test
    public void test30ThreadsGet() throws InterruptedException {
        startGetThreads(30);
    }

    @Test
    public void test40ThreadsGet() throws InterruptedException {
        startGetThreads(40);
    }

    @Test
    public void test50ThreadsGet() throws InterruptedException {
        startGetThreads(50);
    }

    @Test
    public void test100ThreadsGet() throws InterruptedException {
        startGetThreads(100);
    }

    @Test
    public void test200ThreadsGet() throws InterruptedException {
        startGetThreads(200);
    }

    @Test
    public void test500ThreadsGet() throws InterruptedException {
        startGetThreads(500);
    }

    @Test
    public void test1000ThreadsGet() throws InterruptedException {
        startGetThreads(1000);
    }

    private void startGetThreads(int threadsNum) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(threadsNum);
        for (int j = 0, jj = threadsNum; j < jj; ++j) {
            ClientUtils.threadPool.submit(new SpyMemGetThread(j, threadsNum, countDownLatch));
        }

        countDownLatch.await(2, TimeUnit.SECONDS);
    }

    public static class SpyMemGetThread implements Runnable {
        private int index;
        private int threadsNum;
        private CountDownLatch countDownLatch;

        public SpyMemGetThread(int index, int threadsNum, CountDownLatch countDownLatch) {
            this.index = index;
            this.threadsNum = threadsNum;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                for (int i = 0, ii = ClientUtils.MAX_OPERATION_NUM / threadsNum; i < ii; ++i) {
                    String key = index + ".s." + i;
                    String str = (String) mcc.get(key);

                    if (!key.equals(str)) System.err.println(key + " != " + str);
                }
            } finally {
                if (countDownLatch != null) countDownLatch.countDown();
            }
        }
    }

}
