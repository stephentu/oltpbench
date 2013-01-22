package com.oltpbenchmark.memcached;

import java.util.concurrent.TimeUnit;

/**
 * A necessary evil to unify all these stupid client impls
 * 
 * @author stephentu
 *
 */
public interface MemcachedClientIface {
    public void set(String key, int exp, Object o);
    public Object get(String key);
    public void delete(String key);
    public void shutdown();
    public void waitForQueues(long timeout, TimeUnit u);
}
