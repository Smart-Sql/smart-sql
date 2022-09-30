package org.gridgain.smart.ml.regressions.linear;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.preprocessing.standardscaling.StandardScalerTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import java.util.Hashtable;

public class MyLinearRegressionUtil {

    public static void getMdl(final Ignite ignite, final String cacheName, final String ml_func_name, final String preprocessor, final Hashtable<String, Object> funcPs)
    {
        if (ml_func_name.toLowerCase().equals("LinearRegressionLSQR".toLowerCase()) || ml_func_name.toLowerCase().equals("LinearRegression".toLowerCase()))
        {
            if (preprocessor == null)
            {
                getStandardMdlLSQR(ignite, cacheName);
            }
            else if (preprocessor.toLowerCase().equals("StandardScaler".toLowerCase()))
            {
                getStandardMdlLSQR(ignite, cacheName);
            }
            else
            {
                getNormalizationMdlLSQR(ignite, cacheName);
            }
        }
        else if (ml_func_name.toLowerCase().equals("LinearRegressionSGD".toLowerCase()))
        {
            if (preprocessor == null)
            {
                getStandardMdl(ignite, cacheName, funcPs);
            }
            else if (preprocessor.toLowerCase().equals("StandardScaler".toLowerCase()))
            {
                getStandardMdl(ignite, cacheName, funcPs);
            }
            else
            {
                getNormalizationMdl(ignite, cacheName, funcPs);
            }
        }
    }

    /**
     * 获取 model
     * */
    public static void getStandardMdl(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        LinearRegressionSGDTrainer<?> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
        ));

        if (funcPs != null)
        {
            if (funcPs.containsKey("maxIterations"))
            {
                trainer.withMaxIterations(MyConvertUtil.ConvertToInt(funcPs.get("maxIterations")));
            }

            if (funcPs.containsKey("batchSize"))
            {
                trainer.withBatchSize(MyConvertUtil.ConvertToInt(funcPs.get("batchSize")));
            }

            if (funcPs.containsKey("locIterations"))
            {
                trainer.withLocIterations(MyConvertUtil.ConvertToInt(funcPs.get("locIterations")));
            }

            if (funcPs.containsKey("seed"))
            {
                trainer.withSeed(MyConvertUtil.ConvertToLong(funcPs.get("seed")));
            }
        }

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        Preprocessor<Long, Vector> preprocessor = new StandardScalerTrainer<Long, Vector>()
                .fit(ignite, ignite.cache(cacheName), vectorizer);

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                preprocessor
        );

        double rmse = Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                model,
                preprocessor,
                new RegressionMetrics()
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionSGD), new MyMlModel(model, "LinearRegressionSGD 的均方根误差：" + String.valueOf(rmse)));
        }
    }

    /**
     * 获取 model
     * */
    public static void getNormalizationMdl(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        LinearRegressionSGDTrainer<?> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
        ));

        if (funcPs != null)
        {
            if (funcPs.containsKey("maxIterations"))
            {
                trainer.withMaxIterations(MyConvertUtil.ConvertToInt(funcPs.get("maxIterations")));
            }

            if (funcPs.containsKey("batchSize"))
            {
                trainer.withBatchSize(MyConvertUtil.ConvertToInt(funcPs.get("batchSize")));
            }

            if (funcPs.containsKey("locIterations"))
            {
                trainer.withLocIterations(MyConvertUtil.ConvertToInt(funcPs.get("locIterations")));
            }

            if (funcPs.containsKey("seed"))
            {
                trainer.withSeed(MyConvertUtil.ConvertToLong(funcPs.get("seed")));
            }
        }

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        Preprocessor<Long, Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Long, Vector>()
                .fit(
                        ignite,
                        ignite.cache(cacheName),
                        vectorizer
                );

        Preprocessor<Long, Vector> normalizationPreprocessor = new NormalizationTrainer<Long, Vector>()
                .fit(
                        ignite,
                        ignite.cache(cacheName),
                        minMaxScalerPreprocessor
                );

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                normalizationPreprocessor
        );

        double rmse = Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                model,
                normalizationPreprocessor,
                new RegressionMetrics()
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionSGD), new MyMlModel(model, "LinearRegressionSGD 的均方根误差：" + String.valueOf(rmse)));
        }
    }

    /**
     * 获取 model
     * */
    public static void getStandardMdlLSQR(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        Preprocessor<Long, Vector> preprocessor = new StandardScalerTrainer<Long, Vector>()
                .fit(ignite, ignite.cache(cacheName), vectorizer);

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                preprocessor
        );

        double rmse = Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                model,
                preprocessor,
                new RegressionMetrics()
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQR), new MyMlModel(model, "LinearRegressionLSQR 的均方根误差：" + String.valueOf(rmse)));
        }
    }

    /**
     * 获取 model
     * */
    public static void getNormalizationMdlLSQR(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        Preprocessor<Long, Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Long, Vector>()
                .fit(
                        ignite,
                        ignite.cache(cacheName),
                        vectorizer
                );

        Preprocessor<Long, Vector> normalizationPreprocessor = new NormalizationTrainer<Long, Vector>()
                .fit(
                        ignite,
                        ignite.cache(cacheName),
                        minMaxScalerPreprocessor
                );

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                normalizationPreprocessor
        );

        double rmse = Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                model,
                normalizationPreprocessor,
                new RegressionMetrics()
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQR), new MyMlModel(model, "LinearRegressionLSQR 的均方根误差：" + String.valueOf(rmse)));
        }
    }
}
