package org.gridgain.nosql;

import cn.plus.model.CacheDllType;
import cn.plus.model.MyCacheEx;
import cn.plus.model.MyNoSqlCache;
import cn.plus.model.MySmartCache;
import cn.smart.service.IMyLogTransaction;
import com.google.common.base.Strings;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.smart.service.MyLogService;
import org.apache.ignite.transactions.Transaction;
import org.gridgain.dml.util.MyCacheExUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * No sql
 * */
public class MyNoSqlUtil {

    private static IMyLogTransaction myLog = MyLogService.getInstance().getMyLog();

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

    public static CacheConfiguration getNearCfg(final String cacheName, final String mode, final int maxSize)
    {
        NearCacheConfiguration<Object, Object> nearCfg = new NearCacheConfiguration<>();

        // Use LRU eviction policy to automatically evict entries
        // from near-cache whenever it reaches 100_00 entries
        nearCfg.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(maxSize));

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        if (!Strings.isNullOrEmpty(mode) && mode.toLowerCase().equals("replicated")) {
            cfg.setCacheMode(CacheMode.REPLICATED);
        }
        else
        {
            cfg.setCacheMode(CacheMode.PARTITIONED);
        }
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setDataRegionName("Near_Caches_Region_Eviction");
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);
        cfg.setNearConfiguration(nearCfg);

        return cfg;
    }

    public static CacheConfiguration getCacheCfg(final String cacheName, final String mode)
    {

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        if (!Strings.isNullOrEmpty(mode) && mode.toLowerCase().equals("replicated")) {
            cfg.setCacheMode(CacheMode.REPLICATED);
        }
        else
        {
            cfg.setCacheMode(CacheMode.PARTITIONED);
        }
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setName(cacheName);
        cfg.setReadFromBackup(true);

        return cfg;
    }

    public static void createCache(final Ignite ignite, final String cacheName, final Boolean is_cache, final String mode, final int maxSize)
    {
        CacheConfiguration configuration;
        if (is_cache == true)
        {
            configuration = getNearCfg(cacheName, mode, maxSize);
        }
        else
        {
            configuration = getCacheCfg(cacheName, mode);
        }

        if (myLog != null)
        {
            IgniteTransactions transactions = ignite.transactions();
            Transaction tx = null;
            try {
                tx = transactions.txStart();

                ignite.getOrCreateCache(configuration);

                MySmartCache mySmartCache = new MySmartCache();
                mySmartCache.setCacheDllType(CacheDllType.CREATE);
                mySmartCache.setIs_cache(is_cache);
                mySmartCache.setMode(mode);
                mySmartCache.setMaxSize(maxSize);
                mySmartCache.setTable_name(cacheName);

                if(myLog.saveTo(MyCacheExUtil.objToBytes(mySmartCache)) == false)
                {
                    throw new Exception("log 保存失败！");
                }

                tx.commit();
            } catch (Exception ex) {
                if (tx != null) {
                    ignite.destroyCache(cacheName);
                    tx.rollback();
                }
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
        else
        {
            ignite.getOrCreateCache(configuration);
        }
    }

    public static void dropCache(final Ignite ignite, final String cacheName) throws Exception {
        if (myLog != null)
        {
            MySmartCache mySmartCache = new MySmartCache();
            mySmartCache.setCacheDllType(CacheDllType.DROP);
            mySmartCache.setTable_name(cacheName);

            if(myLog.saveTo(MyCacheExUtil.objToBytes(mySmartCache)) == false)
            {
                throw new Exception("log 保存失败！");
            }
            else
            {
                ignite.destroyCache(cacheName);
            }
        }
        else
        {
            ignite.destroyCache(cacheName);
        }
    }

    public static void runCache(final Ignite ignite, final MyNoSqlCache myNoSqlCache)
    {
        List<MyNoSqlCache> lst = new ArrayList<>();
        lst.add(myNoSqlCache);
        MyCacheExUtil.transLogCache(ignite, lst);
    }

//    public static void defineCache(final Ignite ignite, final Long group_id, final String cacheName, final String dataRegin, final String line)
//    {
//        ignite.getOrCreateCache(getCfg(cacheName, dataRegin));
//        IgniteCache<MyCacheGroup, MyCacheValue> cache = ignite.cache("my_cache");
//        cache.put(new MyCacheGroup(cacheName, group_id), new MyCacheValue(line, dataRegin));
//    }
//
//    public static Boolean hasCache(final Ignite ignite, final String cacheName, final Long group_id)
//    {
//        Boolean rs = ignite.cache("my_cache").containsKey(new MyCacheGroup(cacheName, group_id));
//        if (rs)
//        {
//            return true;
//        }
//        return ignite.cache("my_cache").containsKey(new MyCacheGroup(cacheName, 0L));
//    }
//
//    public static void destroyCache(final Ignite ignite, final String cacheName, final Long group_id)
//    {
//        if (hasCache(ignite, cacheName, group_id)) {
//            ignite.cache("my_cache").remove(new MyCacheGroup(cacheName, group_id));
//            ignite.cache(cacheName).destroy();
//        }
//    }
}
