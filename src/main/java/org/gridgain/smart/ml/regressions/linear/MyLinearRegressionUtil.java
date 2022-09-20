package org.gridgain.smart.ml.regressions.linear;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerPreprocessor;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.standardscaling.StandardScalerPreprocessor;
import org.apache.ignite.ml.preprocessing.standardscaling.StandardScalerTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainer;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;

public class MyLinearRegressionUtil {
    /**
     * 获取 model
     * */
    public static void getMdlToCache(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQR), new MyMlModel(model));
        }
    }

    public static void getMinMaxMdlToCache(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        MinMaxScalerTrainer<Long, Vector> minMaxScalerTrainer = new MinMaxScalerTrainer<>();

        Preprocessor<Long, Vector> preprocessor = minMaxScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        LinearRegressionModel model = trainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), preprocessor);
        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler), new MyMlModel(model, preprocessor));
        }
    }

    public static void getStandardMdlToCache(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        StandardScalerTrainer<Long, Vector> standardScalerTrainer = new StandardScalerTrainer<>();

        Preprocessor<Long, Vector> preprocessor = standardScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        LinearRegressionModel model = trainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), preprocessor);
        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler), new MyMlModel(model, preprocessor));
        }
    }

    public static Double predict(final Ignite ignite, final String cacheName, final Double[] vs)
    {
        Double rs = null;
        MyMlModel model = (MyMlModel) ignite.cache("my_ml_model").get(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler));
        if (model.getPreprocessor() == null)
        {
            LinearRegressionModel igniteModel = (LinearRegressionModel) model.getModel();
            rs = igniteModel.predict(VectorUtils.of(vs));
        }
        else
        {
            if (model.getPreprocessor() instanceof StandardScalerPreprocessor)
            {
                StandardScalerPreprocessor standardScalerPreprocessor = (StandardScalerPreprocessor) model.getPreprocessor();
                double[] dv = standardScalerPreprocessor.apply(0L, VectorUtils.of(vs)).features().asArray();
                Vector vector = VectorUtils.of(dv);

                LinearRegressionModel igniteModel = (LinearRegressionModel) model.getModel();
                rs = igniteModel.predict(vector);
            }
            else
            {
                MinMaxScalerPreprocessor maxScalerPreprocessor = (MinMaxScalerPreprocessor) model.getPreprocessor();
                double[] dv = maxScalerPreprocessor.apply(0L, VectorUtils.of(vs)).features().asArray();
                Vector vector = VectorUtils.of(dv);

                LinearRegressionModel igniteModel = (LinearRegressionModel) model.getModel();
                rs = igniteModel.predict(vector);
            }

        }
        return rs;
    }

    /**
     * 获取 model
     * */
    public static void getMdlToCacheSGD(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
        ));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQR), new MyMlModel(model));
        }
    }

    public static void getMinMaxMdlToCacheSGDT(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
        ));

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        MinMaxScalerTrainer<Long, Vector> minMaxScalerTrainer = new MinMaxScalerTrainer<>();

        Preprocessor<Long, Vector> preprocessor = minMaxScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        LinearRegressionModel model = trainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), preprocessor);
        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler), new MyMlModel(model, preprocessor));
        }
    }

    public static void getStandardMdlToCacheSGDT(final Ignite ignite, final String cacheName)
    {
        DatasetTrainer<LinearRegressionModel, Double> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
        ));
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        StandardScalerTrainer<Long, Vector> standardScalerTrainer = new StandardScalerTrainer<>();

        Preprocessor<Long, Vector> preprocessor = standardScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        LinearRegressionModel model = trainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), preprocessor);
        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionLSQRWithMinMaxScaler), new MyMlModel(model, preprocessor));
        }
    }

}























































