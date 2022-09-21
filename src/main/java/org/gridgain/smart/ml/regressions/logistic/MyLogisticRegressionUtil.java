package org.gridgain.smart.ml.regressions.logistic;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.composition.bagging.BaggedModel;
import org.apache.ignite.ml.composition.bagging.BaggedTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.TrainerTransformers;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMPs;
import org.gridgain.smart.ml.model.MyMlModel;

/**
 * 逻辑回归
 * */
public class MyLogisticRegressionUtil {

    public static void getMdlToCache(final Ignite ignite, final String cacheName, final MyMPs ps)
    {
        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                .withUpdatesStgy(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(0.2),
                        SimpleGDParameterUpdate.SUM_LOCAL,
                        SimpleGDParameterUpdate.AVG
                ));

        if (ps != null)
        {
            if (ps.getMaxIterations() != null)
            {
                trainer = trainer.withMaxIterations(ps.getMaxIterations());
            }

            if (ps.getBatchSize() != null)
            {
                trainer = trainer.withBatchSize(ps.getBatchSize());
            }

            if (ps.getLocIterations() != null)
            {
                trainer = trainer.withLocIterations(ps.getLocIterations());
            }

            if (ps.getSeed() != null)
            {
                trainer = trainer.withSeed(ps.getSeed());
            }
        }

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);

        //LogisticRegressionModel mdl = trainer.fit(ignite, dataCache, vectorizer);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        LogisticRegressionModel model = trainer.fit(
                ignite,
                ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        double accuracy = Evaluator.evaluate(
                ignite.cache(cacheName),
                model,
                vectorizer
        ).accuracy();

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LogisticRegression), new MyMlModel(model, accuracy));
        }
    }

    public static void getBaggedMdlToCache(final Ignite ignite, final String cacheName, final MyMPs ps)
    {
        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                .withUpdatesStgy(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(0.2),
                        SimpleGDParameterUpdate.SUM_LOCAL,
                        SimpleGDParameterUpdate.AVG
                ));

        if (ps != null)
        {
            if (ps.getMaxIterations() != null)
            {
                trainer = trainer.withMaxIterations(ps.getMaxIterations());
            }

            if (ps.getBatchSize() != null)
            {
                trainer = trainer.withBatchSize(ps.getBatchSize());
            }

            if (ps.getLocIterations() != null)
            {
                trainer = trainer.withLocIterations(ps.getLocIterations());
            }

            if (ps.getSeed() != null)
            {
                trainer = trainer.withSeed(ps.getSeed());
            }
        }

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);

        BaggedTrainer<Double> baggedTrainer = TrainerTransformers.makeBagged(
                trainer,
                10,
                0.6,
                4,
                3,
                new OnMajorityPredictionsAggregator())
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1));

        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        double accuracy = Evaluator.evaluate(
                ignite.cache(cacheName),
                baggedTrainer.fit(ignite, ignite.cache(cacheName), vectorizer),
                vectorizer
        ).accuracy();

        BaggedModel model = baggedTrainer.fit(ignite, ignite.cache(cacheName), split.getTrainFilter(), vectorizer);

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LogisticRegression), new MyMlModel(model, accuracy));
        }
    }
}
