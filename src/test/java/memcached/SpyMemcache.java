package memcached;

/*
 * Implementation of the {...@link mtvi.util.cache.distributed.Memcache} interface
* which makes use of the spy.memcached Memcache client library. ion of
* spymemcache which include three connection factory and default one is Ketama.
*
* Here CacheOperationTimeOut:Is the number of seconds that can pass before a
* memcache operation (get, set, delete, etc.) tmes out.A default value between
* 2 and 5 (sec) is appropriate for this setting. defaultExpirationTimeSeconds -
* Is the number of seconds an object will remain in memcache before it expires
* and gets removed from the cache. Once an object is put into memcache, it
* should stay there for a long time.A default value between 3600 and 7200
* (1 to   * 2 hours) is appropriate for this setting.
*

**/
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.commons.lang3.StringUtils;


public class SpyMemcache {
    private MemcachedClient client;
    private String[] servers;
    private int cacheOperationTimeoutSeconds;
    private long defaultExpirationTimeSeconds;
    private String connectionType;

    public SpyMemcache() {
        try {
            if (getConnectionType().equalsIgnoreCase("default")) {
                client = new MemcachedClient(AddrUtil.getAddresses(StringUtils.join(getServers(),
                        ' ')));
            } else if (getConnectionType().equalsIgnoreCase("binary")) {
                client = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil
                        .getAddresses(StringUtils.join(getServers(), ' ')));
            } else {
                client = new MemcachedClient(new KetamaConnectionFactory(), AddrUtil
                        .getAddresses(StringUtils.join(getServers(), ' ')));
            }
        } catch (IOException e) {
        }
    }

    /**
     * Method returns the Object against the key or a null value if key value is
     * not available.
     *
     * @param key value in the cache
     * @return the Object
     */
    public Object get(String key) {
        Object myObj = null;
        Future<Object> future = client.asyncGet(key);
        try {
            myObj = future.get(getCacheOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) { // java.util.concurrent.TimeoutException
        } catch (Exception e) {
        }
        return myObj;
    }

    /**
     * Add an object to the cache if it does not exist.
     *
     * @param key value in the cache
     * @param the Object of key value pair
     * @param expiration the time in seconds value will be there in cache
     * @return boolean for sucess/failure to add in cache
     */
    public boolean add(String key, Object value, int expiration) {
        Future<Boolean> future = client.add(key, expiration, value);
        try {
            return future.get(getCacheOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) { // java.util.concurrent.TimeoutException
        } catch (Exception e) {
        }
        return false;
    }

    /**
     * Delete the given key from the cache.
     *
     * @param key value in the cache that needs to delete
     * @return boolean for sucess/failure to delete in cache
     */
    public boolean delete(String key) {
        Future<Boolean> future = client.delete(key);
        try {
            return future.get(getCacheOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) { // java.util.concurrent.TimeoutException
        } catch (Exception e) {
        }
        return false;
    }

    /**
     * Set an object in the cache regardless of any existing value.
     *
     * @param key value in the cache
     * @param the Object of key value pair
     * @param cacheTimeMillis
     * @return boolean for sucess/failure to set in cache
     */
    public boolean set(String key, Object value, long cacheTimeMillis) {
        int expiration = (int) (cacheTimeMillis / 1000);
        Future<Boolean> future = client.set(key, expiration, value);
        try {
            return future.get(getCacheOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) { // java.util.concurrent.TimeoutException
        } catch (Exception e) {
        }
        return false;
    }

    /**
     * Set an object in the cache regardless of any existing value.
     *
     * @param key value in the cache
     * @param the Object of key value pair
     *
     * @return boolean for sucess/failure to set in cache
     */
    public boolean set(String key, Object o) {
        return set(key, o, getDefaultExpirationTimeSeconds() * 1000);
    }

    /**
     * Replace an object with the given value if there is already a value for
     * the given key.
     *
     * @param key value in the cache
     * @param the Object of key value pair
     * @param expiration the time in seconds value will be there in cache
     * @return boolean for sucess/failure to replace value in cache
     */
    public boolean replace(String key, Object value, int expiration) {
        Future<Boolean> future = client.replace(key, expiration, value);
        try {
            return future.get(getCacheOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) { // java.util.concurrent.TimeoutException
        } catch (Exception e) {
        }
        return false;
    }

    /**
     * Array of distributed MemCached Server e.g "server1.mydomain.com:11211",
     * "server2.mydomain.com:11211", "server3.mydomain.com:11211"
     */
    public String[] getServers() {
        return servers;
    }

    public void setServers(String[] servers) {
        this.servers = servers;
    }

    public String getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(String connectionType) {
        this.connectionType = connectionType;
    }

    public int getCacheOperationTimeoutSeconds() {
        return cacheOperationTimeoutSeconds;
    }

    public void setCacheOperationTimeoutSeconds(int cacheOperationTimeoutSeconds) {
        this.cacheOperationTimeoutSeconds = cacheOperationTimeoutSeconds;
    }

    public long getDefaultExpirationTimeSeconds() {
        return defaultExpirationTimeSeconds;
    }

    public void setDefaultExpirationTimeSeconds(long defaultExpirationTimeSeconds) {
        this.defaultExpirationTimeSeconds = defaultExpirationTimeSeconds;
    }


    /**
     * Clear the spymemcache client
     */
    public void clear() {
        client.flush();
    }

    /**
     * shutdown the spymemcache client
     */
    public void shutdown() {
        client.shutdown();
    }

}
