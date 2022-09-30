package org.gridgain.smart.ml.clustering;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.structures.LabeledVector;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MyKMeansUtil {

    public static void getMdlToCache(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        long seed = 0L;
        if (funcPs != null && funcPs.containsKey("seed"))
        {
            seed = MyConvertUtil.ConvertToLong(funcPs.get("seed"));
        }
        KMeansTrainer trainer = new KMeansTrainer()
                .withDistance(new EuclideanDistance())
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(seed));

        if (funcPs != null)
        {
            if (funcPs.containsKey("maxIterations"))
            {
                trainer.withMaxIterations(MyConvertUtil.ConvertToInt(funcPs.get("maxIterations")));
            }

            if (funcPs.containsKey("k"))
            {
                trainer.withAmountOfClusters(MyConvertUtil.ConvertToInt(funcPs.get("k")));
            }
        }

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);

        //LogisticRegressionModel mdl = trainer.fit(ignite, dataCache, vectorizer);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        KMeansModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );


        if (model != null)
        {
            double entropy = computeMeanEntropy(ignite.cache(cacheName), split.getTestFilter(), vectorizer, model);
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.KMeans), new MyMlModel(model, "KMeans 聚类中的平均熵：" + String.valueOf(entropy)));
        }
    }

    /**
     * Computes mean entropy in clusters.
     *
     * @param cache Dataset cache.
     * @param filter Test dataset filter.
     * @param vectorizer Upstream vectorizer.
     * @param model KMeans model.
     * @return Score.
     */
    public static double computeMeanEntropy(IgniteCache<Long, Vector> cache,
                                             IgniteBiPredicate<Long, Vector> filter,
                                             Vectorizer<Long, Vector, Integer, Double> vectorizer,
                                             KMeansModel model) {

        Map<Long, Map<Long, AtomicLong>> clusterUniqueLabelCounts = new HashMap<>();
        try (QueryCursor<Cache.Entry<Long, Vector>> cursor = cache.query(new ScanQuery<>(filter))) {
            for (Cache.Entry<Long, Vector> ent : cursor) {
                LabeledVector<Double> vec = vectorizer.apply(ent.getKey(), ent.getValue());
                long cluster = MyConvertUtil.ConvertToLong(model.predict(vec.features()));
                long channel = MyConvertUtil.ConvertToLong(vec.label().intValue());

                if (!clusterUniqueLabelCounts.containsKey(cluster)) {
                    clusterUniqueLabelCounts.put(MyConvertUtil.ConvertToLong(cluster), new HashMap<>());
                }

                if (!clusterUniqueLabelCounts.get(cluster).containsKey(channel)) {
                    clusterUniqueLabelCounts.get(cluster).put(MyConvertUtil.ConvertToLong(channel), new AtomicLong());
                }

                clusterUniqueLabelCounts.get(cluster).get(channel).incrementAndGet();
            }
        }

        double sumOfClusterEntropies = 0.0;
        for (Long cluster : clusterUniqueLabelCounts.keySet()) {
            Map<Long, AtomicLong> labelCounters = clusterUniqueLabelCounts.get(cluster);
            long sizeOfCluster = labelCounters.values().stream().mapToLong(AtomicLong::get).sum();
            double entropyInCluster = labelCounters.values().stream()
                    .mapToDouble(AtomicLong::get)
                    .map(lblsCount -> lblsCount / sizeOfCluster)
                    .map(lblProb -> -lblProb * Math.log(lblProb))
                    .sum();

            sumOfClusterEntropies += entropyInCluster;
        }

        return sumOfClusterEntropies / clusterUniqueLabelCounts.size();
    }
}
