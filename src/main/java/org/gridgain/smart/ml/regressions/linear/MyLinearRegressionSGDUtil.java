package org.gridgain.smart.ml.regressions.linear;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.preprocessing.standardscaling.StandardScalerPreprocessor;
import org.apache.ignite.ml.preprocessing.standardscaling.StandardScalerTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMPs;

public class MyLinearRegressionSGDUtil {

    private LinearRegressionSGDTrainer<?> trainer;
    private LinearRegressionModel mdl;
    private Vectorizer<Long, Vector, Integer, Double> vectorizer;
    private TrainTestSplit<Long, Vector> split;
    private StandardScalerTrainer<Long, Vector> standardScalerTrainer;

    public MyLinearRegressionSGDUtil()
    {
        this.vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        this.split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        this.standardScalerTrainer = new StandardScalerTrainer<>();
    }

    /**
     * 获取 model
     * */
    public LinearRegressionModel getMdl(final Ignite ignite, final String cacheName, final MyMPs ps)
    {
        trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
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

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionSGD), model);
        }

        return model;
    }

    /**
     * 获取 model
     * */
    public LinearRegressionModel getStandardMdl(final Ignite ignite, final String cacheName, final MyMPs ps)
    {
        StandardScalerPreprocessor<Long, Vector> preprocessor = (StandardScalerPreprocessor<Long, Vector>)standardScalerTrainer.fit(
                ignite,
                ignite.cache(cacheName),
                vectorizer
        );

        trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM_LOCAL,
                RPropParameterUpdate.AVG
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

        LinearRegressionModel model = trainer.fit(
                ignite, ignite.cache(cacheName),
                split.getTrainFilter(),
                preprocessor
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionSGDStandard), model);
        }
        return model;
    }

    /**
     * 获取评分
     * */
    public Double getScore(final Ignite ignite, final String cacheName)
    {
        LinearRegressionModel mdl = (LinearRegressionModel) ignite.cache("my_ml_model").get(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionSGD));
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
    public Double getStandardScore(final Ignite ignite, final String cacheName)
    {
        LinearRegressionModel mdl = (LinearRegressionModel) ignite.cache("my_ml_model").get(new MyMModelKey(cacheName, MyMLMethodName.LinearRegressionSGDStandard));
        return Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                mdl,
                vectorizer,
                new RegressionMetrics().withMetric(RegressionMetricValues::r2)
        );
    }
}
