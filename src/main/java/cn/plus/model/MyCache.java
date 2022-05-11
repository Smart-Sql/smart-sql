package cn.plus.model;

import org.apache.ignite.IgniteCache;

import java.io.Serializable;

/**
 * 自定义的 cache model
 * */
public class MyCache implements Serializable {

    private static final long serialVersionUID = 1344496584459267596L;

    private IgniteCache cache;

    private Object key;

    private Object value;

    public MyCache(IgniteCache cache, Object key, Object value)
    {
        this.cache = cache;
        this.key = key;
        this.value = value;
    }

    public MyCache()
    {}

    public IgniteCache getCache() {
        return cache;
    }

    public void setCache(IgniteCache cache) {
        this.cache = cache;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyCache{" +
                "cache=" + cache +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
