package org.tools;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.ArrayList;
import java.util.List;

public class KvSql {
    /**
     * 输入 cache 名字，获取 cache 的 key type
     * 必须在 ddl 中指定 cache_name ，同时不指定 key_type
     * */
    public static String getKeyType(Ignite ignite, String cacheName) {
        IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
        List<QueryEntity> list = (ArrayList) cache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        if (list.size() == 1) {
            return list.get(0).findKeyType();
        } else {
            throw new RuntimeException(String.format("cache[%s] not find key type", cacheName));
        }
    }

    /**
     * 输入 cache 名字，获取 cache 的 value type
     * 必须在 ddl 中指定 cache_name ，同时不指定 value_type
     * 用法：
     *    在 insert 和 update 中需要事务的时候，使用这个要结合
     * */
    public static String getValueType(Ignite ignite, String cacheName) {
        IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
        List<QueryEntity> list = (ArrayList) cache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        if (list.size() == 1) {
            return list.get(0).findValueType();
        } else {
            throw new RuntimeException(String.format("cache[%s] not find value type", cacheName));
        }
    }

    /**
     * 获取 Key Builder
     * */
    public static BinaryObjectBuilder getKeyBuilder(Ignite ignite, String cacheName)
    {
        return ignite.binary().builder(KvSql.getKeyType(ignite, cacheName));
    }

    /**
     * 获取 Value Builder
     * */
    public static BinaryObjectBuilder getValueBuilder(Ignite ignite, String cacheName)
    {
        return ignite.binary().builder(KvSql.getValueType(ignite, cacheName));
    }
}
