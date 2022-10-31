package org.gridgain.smart.ml;

import clojure.lang.PersistentVector;
import cn.plus.model.ddl.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.tools.MyConvertUtil;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MyTrianDataUtil implements Serializable {
    private static final long serialVersionUID = 5546137149960479638L;

    public static Double[] vsToDouble(final PersistentVector vs)
    {
        Double[] rs = new Double[vs.count()];
        for (int i = 0; i < vs.count(); i++)
        {
            rs[i] = MyConvertUtil.ConvertToDouble(vs.nth(i));
        }
        return rs;
    }

    public static CacheConfiguration<Long, Vector> trainCfg(final String cacheName)
    {
        CacheConfiguration<Long, Vector> trainingSetCfg = new CacheConfiguration<>();
        trainingSetCfg.setName(cacheName);
        trainingSetCfg.setCacheMode(CacheMode.PARTITIONED);
        trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

        return trainingSetCfg;
    }

    /**
     * 是否存在这个训练数据集
     * */
    public static Boolean hasTrainMatrix(final Ignite ignite, final MyMlCaches mlCaches) {
        String cacheName = getCacheName(mlCaches);
        IgniteCache cache = ignite.cache(cacheName);
        if (cache != null)
        {
            return true;
        }
        //return ignite.cache(cacheName) == null ? false : true;
        return false;
    }

    /**
     * 删除训练数据集
     * */
    public static void dropTrainMatrix(final Ignite ignite, final MyMlCaches mlCaches) {
        String cacheName = getCacheName(mlCaches);
        if (hasTrainMatrix(ignite, mlCaches))
        {
            ignite.cache("ml_train_data").remove(new MyCachePK(mlCaches.getSchema_name(), mlCaches.getTable_name()));
            ignite.destroyCache(cacheName);
        }
    }

    public static void loadTrainMatrix(final Ignite ignite, final String schema_name, final String table_name, final PersistentVector vs)
    {
        String cacheName = "sm_ml_" + schema_name + "_" + table_name;
        Long key = Ignition.ignite().atomicSequence(cacheName, 0, true).incrementAndGet();
        Vector vts = VectorUtils.of(vsToDouble(vs));
        ignite.cache(cacheName).put(key, vts);
    }

    public static String getCacheName(final MyMlCaches mlCaches)
    {
        return "sm_ml_" + mlCaches.getSchema_name() + "_" + mlCaches.getTable_name();
    }

    public static String getCacheName(final MyTransData mlCaches)
    {
        return "sm_ml_" + mlCaches.getSchema_name() + "_" + mlCaches.getTable_name();
    }

    public static String getCacheName(final Object o)
    {
        if (o instanceof MyMlCaches)
        {
            MyMlCaches mlCaches = (MyMlCaches) o;
            return "sm_ml_" + mlCaches.getSchema_name() + "_" + mlCaches.getTable_name();
        }
        else if (o instanceof MyMlShowData)
        {
            MyMlShowData mlCaches = (MyMlShowData) o;
            return "sm_ml_" + mlCaches.getSchema_name() + "_" + mlCaches.getTable_name();
        }
        else if (o instanceof MyTransDataLoad)
        {
            MyTransDataLoad mlCaches = (MyTransDataLoad) o;
            return "sm_ml_" + mlCaches.getSchema_name() + "_" + mlCaches.getTable_name();
        }
        else
        {
            MyTransData mlCaches = (MyTransData) o;
            return "sm_ml_" + mlCaches.getSchema_name() + "_" + mlCaches.getTable_name();
        }
    }

    public static void train_matrix_single(final Ignite ignite, String schema_name, String table_name, String value)
    {
        String cacheName = "sm_ml_" + schema_name + "_" + table_name;
        String[] lst = value.split(",");
        Double[] rs = new Double[lst.length];
        for (int i = 0; i < lst.length; i++)
        {
            rs[i] = MyConvertUtil.ConvertToDouble(lst[i]);
        }

        ignite.cache(cacheName).put(ignite.atomicSequence(cacheName, 0, true).incrementAndGet(), VectorUtils.of(rs));
    }

    public static List showTrainData(final Ignite ignite, String cacheName, Integer item_size)
    {
        IgniteCache cache = ignite.cache(cacheName);

        Iterable<Cache.Entry<Long, Vector>> locEntries = cache.localEntries(CachePeekMode.OFFHEAP);

        List lst = new ArrayList();
        int index = 0;
        for (Cache.Entry<Long, Vector> entry : locEntries) {

            if (index <= item_size)
            {
                lst.add(entry.getValue().asArray());
                index++;
            }
            else
            {
                break;
            }

//            assertTrue(locKeys.remove(entry.getKey()));
//            assertEquals(data[entry.getKey()], entry.getValue());
        }
        return lst;
    }
}






















































