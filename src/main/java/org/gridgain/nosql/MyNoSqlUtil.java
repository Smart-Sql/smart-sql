package org.gridgain.nosql;

import cn.plus.model.nosql.MyCacheGroup;
import cn.plus.model.nosql.MyCacheValue;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * No sql
 * */
public class MyNoSqlUtil {

    public static CacheConfiguration getCfg(final String cacheName)
    {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);

        return cfg;
    }

    public static CacheConfiguration getCfg(final String cacheName, final String dataRegin)
    {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setDataRegionName(dataRegin);
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);

        return cfg;
    }

    public static void defineCache(final Ignite ignite, final Long group_id, final String cacheName, final String dataRegin, final String line)
    {
        ignite.getOrCreateCache(getCfg(cacheName, dataRegin));
        IgniteCache<MyCacheGroup, MyCacheValue> cache = ignite.cache("my_cache");
        cache.put(new MyCacheGroup(cacheName, group_id), new MyCacheValue(line, dataRegin));
    }

    public static Boolean hasCache(final Ignite ignite, final String cacheName, final Long group_id)
    {
        Boolean rs = ignite.cache("my_cache").containsKey(new MyCacheGroup(cacheName, group_id));
        if (rs)
        {
            return true;
        }
        return ignite.cache("my_cache").containsKey(new MyCacheGroup(cacheName, 0L));
    }

    public static void destroyCache(final Ignite ignite, final String cacheName, final Long group_id)
    {
        if (hasCache(ignite, cacheName, group_id)) {
            ignite.cache("my_cache").remove(new MyCacheGroup(cacheName, group_id));
            ignite.cache(cacheName).destroy();
        }
    }
}
