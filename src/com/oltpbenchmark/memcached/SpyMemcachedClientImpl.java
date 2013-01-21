package com.oltpbenchmark.memcached;

import net.spy.memcached.MemcachedClient;

public class SpyMemcachedClientImpl implements MemcachedClientIface {

    private final MemcachedClient client;
    
    public SpyMemcachedClientImpl(MemcachedClient client) {
        this.client = client;
    }

    @Override
    public void set(String key, int exp, Object o) {
        client.set(key, exp, o);
    }

    @Override
    public Object get(String key) {
        return client.get(key);
    }

    @Override
    public void shutdown() {
        client.shutdown();
    }

    @Override
    public void delete(String key) {
        client.delete(key);
    }
}
