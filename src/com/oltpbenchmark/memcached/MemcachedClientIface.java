package com.oltpbenchmark.memcached;

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
}
