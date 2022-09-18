package org.gridgain.smart.ml.regressions.logistic;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.gridgain.smart.ml.model.MyMPs;

/**
 * 逻辑回归
 * */
public class LogisticRegressionSGDUtil {
    private Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);

    /**
     * 获取 model
     * */
    public LogisticRegressionModel getMdl(final Ignite ignite, final String cacheName)
    {
        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                .withUpdatesStgy(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(0.2),
                        SimpleGDParameterUpdate.SUM_LOCAL,
                        SimpleGDParameterUpdate.AVG
                ));

        return trainer.fit(
                ignite, ignite.cache(cacheName),
                vectorizer
        );
    }

    /**
     * 获取 model
     * */
    public LogisticRegressionModel getMdl(final Ignite ignite, final String cacheName, final MyMPs ps)
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

        return trainer.fit(
                ignite, ignite.cache(cacheName),
                vectorizer
        );
    }

    /**
     * 获取评分
     * */
    public Double getMinMaxScalerScore(final Ignite ignite, final String cacheName)
    {
//        double accuracy = Evaluator.evaluate(
//                ignite.cache(cacheName),
//                mdl,
//                vectorizer
//        ).accuracy();
        return 0D;
    }
}
