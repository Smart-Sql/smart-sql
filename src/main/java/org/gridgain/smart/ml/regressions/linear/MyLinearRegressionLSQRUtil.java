package org.gridgain.smart.ml.regressions.linear;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;

import java.util.List;

/**
 * 线性回归 LSQR
 * 最小二乘法
 * */
public class MyLinearRegressionLSQRUtil {

    private DatasetTrainer<LinearRegressionModel, Double> trainer;
    private Vectorizer<Long, Vector, Integer, Double> vectorizer;
    private TrainTestSplit<Long, Vector> split;
    private MinMaxScalerTrainer<Long, Vector> minMaxScalerTrainer;

    public MyLinearRegressionLSQRUtil()
    {
        this.trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        this.vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        this.split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        this.minMaxScalerTrainer = new MinMaxScalerTrainer<>();
    }

    /**
     * 获取 model
     * */
    public LinearRegressionModel getMdl(final Ignite ignite, final String cacheName)
    {
        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQR), model);
        }

        return model;
    }

    public void getMdlToCache(final Ignite ignite, final String cacheName)
    {
        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQR), model);
        }
    }

    /**
     * 获取 model
     * */
    public LinearRegressionModel getMinMaxScalerMdl(final Ignite ignite, final String cacheName)
    {
        Preprocessor<Long, Vector> preprocessor = minMaxScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        LinearRegressionModel model = trainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), preprocessor);
        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler), model);
        }

        return model;
    }

    public void getMinMaxScalerMdlToCache(final Ignite ignite, final String cacheName)
    {
        Preprocessor<Long, Vector> preprocessor = minMaxScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        LinearRegressionModel model = trainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), preprocessor);
        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler), model);
        }
    }

    /**
     * 获取评分
     * */
    public Double getScore(final Ignite ignite, final String cacheName)
    {
        MyMModelKey modelKey = new MyMModelKey();
        modelKey.setCacheName(cacheName);
        modelKey.setMethodName(MyMLMethodName.LinearRegressionLSQR);

        LinearRegressionModel mdl = (LinearRegressionModel) ignite.cache("my_ml_model").get(modelKey);
        return Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                mdl,
                vectorizer,
                new RegressionMetrics().withMetric(RegressionMetricValues::r2)
        );
    }

    /**
     * 获取评分
     * */
    public Double getMinMaxScalerScore(final Ignite ignite, final String cacheName)
    {
        Preprocessor<Long, Vector> preprocessor = minMaxScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        MyMModelKey modelKey = new MyMModelKey();
        modelKey.setCacheName(cacheName);
        modelKey.setMethodName(MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler);

        LinearRegressionModel mdl = (LinearRegressionModel) ignite.cache("my_ml_model").get(modelKey);
        return Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                mdl,
                preprocessor,
                new RegressionMetrics().withMetric(RegressionMetricValues::r2)
        );
    }
}












































