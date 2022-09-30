package org.gridgain.smart.ml.tree.randomforest;

import org.apache.commons.math3.util.Precision;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.tree.randomforest.RandomForestRegressionTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import javax.cache.Cache;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MyRandomForestRegressionUtil {
    /**
     * 获取 model
     * */
    public static void getMdlToCache(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        IgniteCache<Long, Vector> dataCache = ignite.cache(cacheName);
        AtomicInteger idx = new AtomicInteger(0);
        RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(
                IntStream.range(0, dataCache.get(1L).size() - 1).mapToObj(
                        x -> new FeatureMeta("", idx.getAndIncrement(), false)).collect(Collectors.toList())
        ).withFeaturesCountSelectionStrgy(FeaturesCountSelectionStrategies.ONE_THIRD);

        if (funcPs != null)
        {
            if (funcPs.containsKey("amountOfTrees"))
            {
                trainer.withAmountOfTrees(MyConvertUtil.ConvertToInt(funcPs.get("amountOfTrees")));
            }

            if (funcPs.containsKey("maxDepth"))
            {
                trainer.withMaxDepth(MyConvertUtil.ConvertToInt(funcPs.get("maxDepth")));
            }

            if (funcPs.containsKey("minImpurityDelta"))
            {
                trainer.withMinImpurityDelta(MyConvertUtil.ConvertToInt(funcPs.get("minImpurityDelta")));
            }

            if (funcPs.containsKey("subSampleSize"))
            {
                trainer.withSubSampleSize(MyConvertUtil.ConvertToDouble(funcPs.get("subSampleSize")));
            }

            if (funcPs.containsKey("seed"))
            {
                trainer.withSeed(MyConvertUtil.ConvertToInt(funcPs.get("seed")));
            }
        }

        trainer.withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder()
                .withParallelismStrategyTypeDependency(ParallelismStrategy.ON_DEFAULT_POOL)
                .withLoggingFactoryDependency(ConsoleLogger.Factory.LOW));

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        //TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        ModelsComposition model = trainer.fit(ignite, dataCache, vectorizer);

        double score = 0D;
        int correctPredictions = 0;
        int count = 0;
        try (QueryCursor<Cache.Entry<Long, Vector>> observations = ignite.cache(cacheName).query(new ScanQuery<>()))
        {
            for (Cache.Entry<Long, Vector> observation : observations) {
                Vector val = observation.getValue();
                Vector inputs = val.copyOfRange(1, val.size());
                double label = val.copyOfRange(0, 1).get(0);
                double prediction = model.predict(inputs);

                if (Precision.equals(prediction, label, Precision.EPSILON)) {
                    correctPredictions++;
                }
                count++;
            }
            score = correctPredictions / count;
        }

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.RandomForestRegression), new MyMlModel(model, "RandomForestRegression 的准确率" + String.valueOf(score)));
        }
    }
}
