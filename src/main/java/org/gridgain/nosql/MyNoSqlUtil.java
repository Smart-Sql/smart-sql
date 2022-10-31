package org.gridgain.nosql;

import cn.plus.model.CacheDllType;
import cn.plus.model.MyCacheEx;
import cn.plus.model.MyNoSqlCache;
import cn.plus.model.MySmartCache;
import cn.plus.model.ddl.MyCachePK;
import cn.plus.model.ddl.MyCaches;
import cn.smart.service.IMyLogTrans;
import com.google.common.base.Strings;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.smart.service.MyLogService;
import org.apache.ignite.transactions.Transaction;
import org.gridgain.dml.util.MyCacheExUtil;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * No sql
 * */
public class MyNoSqlUtil {

    private static IMyLogTrans myLog = MyLogService.getInstance().getMyLog();

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

    public static void createCache(final Ignite ignite, final String ds_name, final String cacheName, final Boolean is_cache, final String mode, final int maxSize)
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
            configuration.setSqlSchema(ds_name);
            ignite.getOrCreateCache(configuration);

            IgniteTransactions transactions = ignite.transactions();
            Transaction tx = null;
            String transSession = UUID.randomUUID().toString();
            try {
                tx = transactions.txStart();
                myLog.createSession(transSession);

                MySmartCache mySmartCache = new MySmartCache();
                mySmartCache.setCacheDllType(CacheDllType.CREATE);
                mySmartCache.setIs_cache(is_cache);
                mySmartCache.setMode(mode);
                mySmartCache.setMaxSize(maxSize);
                mySmartCache.setTable_name(cacheName);

                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));

                myLog.commit(transSession);
                tx.commit();
            } catch (Exception ex) {
                if (tx != null) {
                    myLog.rollback(transSession);
                    tx.rollback();
                    ignite.destroyCache(cacheName);
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

    public static void createCacheSave(final Ignite ignite, final String schema_name, final String table_name, final Boolean is_cache, final String mode, final int maxSize)
    {
        String cacheName = "c_" + schema_name.toLowerCase() + "_" + table_name.toLowerCase();
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
            configuration.setSqlSchema(schema_name);
            ignite.getOrCreateCache(configuration);

            IgniteTransactions transactions = ignite.transactions();
            Transaction tx = null;
            String transSession = UUID.randomUUID().toString();
            try {
                tx = transactions.txStart();
                myLog.createSession(transSession);

                MySmartCache mySmartCache = new MySmartCache();
                mySmartCache.setCacheDllType(CacheDllType.CREATE);
                mySmartCache.setIs_cache(is_cache);
                mySmartCache.setMode(mode);
                mySmartCache.setMaxSize(maxSize);
                mySmartCache.setTable_name(cacheName);

                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));

                IgniteCache<MyCachePK, MyCaches> my_caches = ignite.cache("my_caches");
                my_caches.put(new MyCachePK(schema_name, table_name), new MyCaches(schema_name, table_name, is_cache, mode, maxSize));

                myLog.commit(transSession);
                tx.commit();
            } catch (Exception ex) {
                if (tx != null) {
                    myLog.rollback(transSession);
                    tx.rollback();
                    ignite.destroyCache(cacheName);
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

            IgniteCache<MyCachePK, MyCaches> my_caches = ignite.cache("my_caches");
            my_caches.put(new MyCachePK(schema_name, table_name), new MyCaches(schema_name, table_name, is_cache, mode, maxSize));
        }
    }

    public static void dropCache(final Ignite ignite, final String cacheName) throws Exception {
        if (myLog != null)
        {
            MySmartCache mySmartCache = new MySmartCache();
            mySmartCache.setCacheDllType(CacheDllType.DROP);
            mySmartCache.setTable_name(cacheName);

            String transSession = UUID.randomUUID().toString();
            try
            {
                myLog.createSession(transSession);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));
                ignite.destroyCache(cacheName);
                myLog.commit(transSession);

            }
            catch (Exception ex)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.destroyCache(cacheName);
        }
    }

    public static void dropCacheSave(final Ignite ignite, final String schema_name, final String table_name) throws Exception {
        String cacheName = "c_" + schema_name.toLowerCase() + "_" + table_name.toLowerCase();
        if (myLog != null)
        {
            MySmartCache mySmartCache = new MySmartCache();
            mySmartCache.setCacheDllType(CacheDllType.DROP);
            mySmartCache.setTable_name(cacheName);

            String transSession = UUID.randomUUID().toString();
            try
            {
                myLog.createSession(transSession);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartCache));
                ignite.destroyCache(cacheName);
                myLog.commit(transSession);

            }
            catch (Exception ex)
            {
                myLog.rollback(transSession);
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

    public static void initCaches(final Ignite ignite)
    {
        CacheConfiguration<MyMModelKey, MyMlModel> cfg = new CacheConfiguration<>();
        cfg.setName("my_ml_model");
        cfg.setCacheMode(CacheMode.PARTITIONED);

        ignite.getOrCreateCache(cfg);

        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("select m.schema_name, m.table_name, m.is_cache, m.mode, m.maxSize from MY_META.my_caches m");
        sqlFieldsQuery.setLazy(true);
        Iterator<List<?>> iterator = ignite.cache("my_caches").query(sqlFieldsQuery).iterator();
        while (iterator.hasNext())
        {
            List<?> row = iterator.next();
            String cache_name = "c_" + row.get(0).toString().toLowerCase() + "_" + row.get(1).toString().toLowerCase();
            MyNoSqlUtil.createCache(ignite, row.get(0).toString(), cache_name, MyConvertUtil.ConvertToBoolean(row.get(2)), row.get(3).toString(), MyConvertUtil.ConvertToInt(row.get(4)));
            System.out.println(cache_name + " 初始化成功！");
        }
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
