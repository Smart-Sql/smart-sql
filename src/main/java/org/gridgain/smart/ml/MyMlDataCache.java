package org.gridgain.smart.ml;

import clojure.lang.PersistentVector;
import cn.plus.model.ddl.MyCachePK;
import cn.plus.model.ddl.MyMlCaches;
import cn.plus.model.ddl.MyTransData;
import com.google.common.base.Strings;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.tools.MyConvertUtil;

import java.util.Hashtable;
import java.util.List;

public class MyMlDataCache {

    public static MyMlCaches hashtableToCache(final Hashtable hashtable) throws Exception {
        MyMlCaches myMlCaches = new MyMlCaches();
        if (hashtable.containsKey("dataset_name") && hashtable.get("dataset_name") != null && !Strings.isNullOrEmpty(hashtable.get("dataset_name").toString()))
        {
            myMlCaches.setDataset_name(hashtable.get("dataset_name").toString());
        }
        else
        {
            throw new Exception("必须有 data_name 的属性！");
        }

        if (hashtable.containsKey("table_name") && hashtable.get("table_name") != null && !Strings.isNullOrEmpty(hashtable.get("table_name").toString()))
        {
            myMlCaches.setTable_name(hashtable.get("table_name").toString());
        }
        else
        {
            throw new Exception("必须有 table_name 的属性！");
        }

        if (hashtable.containsKey("describe") && hashtable.get("describe") != null && !Strings.isNullOrEmpty(hashtable.get("describe").toString()))
        {
            myMlCaches.setDescribe(hashtable.get("describe").toString());
        }
        return myMlCaches;
    }

    public static MyTransData hashtableToTransData(final Hashtable hashtable) throws Exception {
        MyTransData transData = new MyTransData();

        if (hashtable.containsKey("dataset_name") && hashtable.get("dataset_name") != null && !Strings.isNullOrEmpty(hashtable.get("dataset_name").toString()))
        {
            transData.setDataset_name(hashtable.get("dataset_name").toString());
        }
        else
        {
            throw new Exception("必须有 data_name 的属性！");
        }

        if (hashtable.containsKey("table_name") && hashtable.get("table_name") != null && !Strings.isNullOrEmpty(hashtable.get("table_name").toString()))
        {
            transData.setTable_name(hashtable.get("table_name").toString());
        }
        else
        {
            throw new Exception("必须有 table_name 的属性！");
        }

        if (hashtable.containsKey("value") && hashtable.get("value") != null && !Strings.isNullOrEmpty(hashtable.get("value").toString()))
        {
            List<Double> lst = (List<Double>) hashtable.get("value");
            transData.setValue(lst.toArray(new Double[0]));
        }
        else
        {
            throw new Exception("必须有 value 的属性！");
        }

        if (hashtable.containsKey("label") && hashtable.get("label") != null && !Strings.isNullOrEmpty(hashtable.get("label").toString()))
        {
            transData.setLabel(MyConvertUtil.ConvertToDouble(hashtable.get("label")));
        }
        else
        {
            throw new Exception("必须有 label 的属性！");
        }
        return transData;
    }

    public static String getCacheName(final MyMlCaches mlCaches)
    {
        return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
    }

    public static String getCacheName(final MyTransData mlCaches)
    {
        return "sm_ml_" + mlCaches.getDataset_name() + "_" + mlCaches.getTable_name();
    }

    /**
     * 是否存在这个训练数据集
     * */
    public static Boolean hasTrainMatrix(final Ignite ignite, final Hashtable hashtable) throws Exception {
        MyMlCaches mlCaches = hashtableToCache(hashtable);
        String cacheName = getCacheName(mlCaches);

        return ignite.cache(cacheName) == null? false: true;
    }

    /**
     * 删除训练数据集
     * */
    public static void dropTrainMatrix(final Ignite ignite, final Hashtable hashtable) throws Exception {
        MyMlCaches mlCaches = hashtableToCache(hashtable);
        String cacheName = getCacheName(mlCaches);
        if (hasTrainMatrix(ignite, hashtable))
        {
            ignite.destroyCache(cacheName);
        }
    }

    public static Double[] getVs(Double label, Double[] data)
    {
        Double[] vs = new Double[data.length + 1];
        vs[0] = label;
        for (int i = 1; i <= data.length; i++)
        {
            vs[i] = data[i - 1];
        }
        return vs;
    }

    /**
     * 将数据添加到训练集
     * */
    public static void trainMatrix(final Ignite ignite, final Hashtable hashtable) throws Exception {
        MyTransData myTransData = hashtableToTransData(hashtable);
        String cacheName = getCacheName(myTransData);
        Long key = Ignition.ignite().atomicSequence(cacheName, 0, true).incrementAndGet();
        Vector vs = VectorUtils.of(getVs(myTransData.getLabel(), myTransData.getValue()));
        ignite.cache(cacheName).put(key, vs);
    }

    /**
     * 返回 IgniteCache<Long, Vector>
     * */
    public static IgniteCache<Long, Vector> getMlDataCache(final Ignite ignite, final Hashtable hashtable) throws Exception {
        MyMlCaches mlCaches = hashtableToCache(hashtable);
        String cacheName = getCacheName(mlCaches);

        IgniteCache cache = ignite.cache(cacheName);

        System.out.println(cacheName);
        System.out.println(mlCaches);
        System.out.println(hashtable.toString());

        if (cache == null) {
            CacheConfiguration<Long, Vector> trainingSetCfg = new CacheConfiguration<>();
            trainingSetCfg.setName(cacheName);
            trainingSetCfg.setCacheMode(CacheMode.PARTITIONED);
            trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

            ignite.cache("ml_train_data").put(new MyCachePK(mlCaches.getDataset_name(), mlCaches.getTable_name()), mlCaches);

            return ignite.getOrCreateCache(trainingSetCfg);
        }
        return cache;
    }

    public static Double[] vsToDouble(final PersistentVector vs)
    {
        Double[] rs = new Double[vs.count()];
        for (int i = 0; i < vs.count(); i++)
        {
            rs[i] = MyConvertUtil.ConvertToDouble(vs.nth(i));
        }
        return rs;
    }

    public static void loadTrainMatrix(final Ignite ignite, final String data_set_name, final String table_name, final PersistentVector vs)
    {
        String cacheName = "sm_ml_" + data_set_name + "_" + table_name;
        Long key = Ignition.ignite().atomicSequence(cacheName, 0, true).incrementAndGet();
        Vector vts = VectorUtils.of(vsToDouble(vs));
        ignite.cache(cacheName).put(key, vts);
    }
}

