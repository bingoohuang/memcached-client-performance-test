package memcached;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.spy.memcached.internal.OperationFuture;

import com.whalin.MemCached.SockIOPool;

public class ClientUtils {
    private static final String MEMCACHED_SERVER_ADDR = "127.0.0.1:11211";
    public static final int MAX_OPERATION_NUM = 60000;
    public static ExecutorService threadPool = Executors.newCachedThreadPool();

    public static net.rubyeye.xmemcached.MemcachedClient getXClient() {
        MemcachedClientBuilder builder = new XMemcachedClientBuilder(
                net.rubyeye.xmemcached.utils.AddrUtil.getAddresses(MEMCACHED_SERVER_ADDR));
        try {
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static net.spy.memcached.MemcachedClient getSpyClient() {
        // MemcachedClient c = new MemcachedClient(AddrUtil.getAddresses("server1:11211 server2:11211"));
        try {
            return new net.spy.memcached.MemcachedClient(
                    net.spy.memcached.AddrUtil.getAddresses(MEMCACHED_SERVER_ADDR));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setupMemCachedPool() {
        String[] servers = { MEMCACHED_SERVER_ADDR };
        SockIOPool pool = SockIOPool.getInstance();
        pool.setServers(servers);
        pool.setFailover(true);
        pool.setInitConn(10);
        pool.setMinConn(1);
        pool.setMaxConn(20);
        // pool.setMaintSleep( 30 );
        pool.setNagle(false);
        pool.setSocketTO(3000);
        pool.setAliveCheck(true);
        pool.initialize();

    }

    public static Object getXCache(net.rubyeye.xmemcached.MemcachedClient mcc, String key) {
        try {
            return mcc.get(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean setXCache(net.rubyeye.xmemcached.MemcachedClient mcc, String key, int exp, Object value) {
        try {
            return mcc.set(key, exp, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setSpyCache(net.spy.memcached.MemcachedClient mcc, String key, int exp, Object value) {
        OperationFuture<Boolean> set = mcc.set(key, 0, key);
        try {
            if (!set.get(10, TimeUnit.SECONDS)) System.err.println(key + " set not ok");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static OperationFuture<Boolean> setSpyAsynCache(net.spy.memcached.MemcachedClient mcc, String key, int exp,
            Object value) {
        return mcc.set(key, 0, key);
    }
}
