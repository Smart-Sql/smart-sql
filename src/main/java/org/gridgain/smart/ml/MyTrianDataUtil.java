package org.gridgain.smart.ml;

import clojure.lang.Cons;
import clojure.lang.ISeq;
import clojure.lang.PersistentVector;
import cn.plus.model.ddl.MyMlCaches;
import cn.plus.model.ddl.MyMlShowData;
import cn.plus.model.ddl.MyTransData;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.gridgain.internal.h2.tools.SimpleResultSet;
import org.tools.MyConvertUtil;

import javax.cache.Cache;
import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Hashtable;

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
        return ignite.cache(cacheName) == null? false: true;
    }

    /**
     * 删除训练数据集
     * */
    public static void dropTrainMatrix(final Ignite ignite, final MyMlCaches mlCaches) {
        String cacheName = getCacheName(mlCaches);
        if (hasTrainMatrix(ignite, mlCaches))
        {
            ignite.destroyCache(cacheName);
        }
    }

    public static void loadTrainMatrix(final Ignite ignite, final String data_set_name, final String table_name, final PersistentVector vs)
    {
        String cacheName = "sm_ml_" + data_set_name + "_" + table_name;
        Long key = Ignition.ignite().atomicSequence(cacheName, 0, true).incrementAndGet();
        Vector vts = VectorUtils.of(vsToDouble(vs));
        ignite.cache(cacheName).put(key, vts);
    }

    public static String getCacheName(final MyMlCaches mlCaches)
    {
        return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
    }

    public static String getCacheName(final MyTransData mlCaches)
    {
        return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
    }

    public static String getCacheName(final Object o)
    {
        if (o instanceof MyMlCaches)
        {
            MyMlCaches mlCaches = (MyMlCaches) o;
            return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
        }
        else if (o instanceof MyMlShowData)
        {
            MyMlShowData mlCaches = (MyMlShowData) o;
            return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
        }
        else
        {
            MyTransData mlCaches = (MyTransData) o;
            return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
        }
    }

    public static ResultSet showTrainData(final Ignite ignite, final MyMlShowData m)
    {
        IgniteCache cache = ignite.cache(getCacheName(m));

        Iterable<Cache.Entry<Long, Vector>> locEntries = cache.localEntries(CachePeekMode.OFFHEAP);

        for (Cache.Entry<Long, Vector> entry : locEntries) {
//            assertTrue(locKeys.remove(entry.getKey()));
//            assertEquals(data[entry.getKey()], entry.getValue());
        }

        SimpleResultSet rs = new SimpleResultSet();

        return rs;
    }
}






















































