package com.oltpbenchmark.memcached;

import java.util.concurrent.TimeUnit;

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

    @Override
    public void waitForQueues(long timeout, TimeUnit u) {
        client.waitForQueues(timeout, u);
    }
}
