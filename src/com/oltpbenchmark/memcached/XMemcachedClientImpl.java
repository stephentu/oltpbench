package com.oltpbenchmark.memcached;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

public class XMemcachedClientImpl implements MemcachedClientIface {

    private final MemcachedClient client;
    
    private static final Logger LOG = Logger.getLogger(XMemcachedClientImpl.class);
    
    public XMemcachedClientImpl(MemcachedClient client) {
        this.client = client;
    }
    
    @Override
    public void set(String key, int exp, Object o) {
        try {
            client.set(key, exp, o);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (MemcachedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Object get(String key) {
        try {
            return client.get(key);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (MemcachedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void shutdown() {
        try {
            client.shutdown();
        } catch (IOException e) {
            LOG.warn(e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            client.delete(key);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (MemcachedException e) {
            throw new IllegalStateException(e);
        }
    }

}
