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
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import java.util.Hashtable;

/**
 * 逻辑回归，二元分类
 * */
public class MyLogisticRegressionUtil {

    public static void getMdlToCache(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        double learningRate = 0.2;
        if (funcPs != null && funcPs.containsKey("learningRate"))
        {
            learningRate = MyConvertUtil.ConvertToDouble(funcPs.get("learningRate"));
        }
        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                .withUpdatesStgy(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(learningRate),
                        SimpleGDParameterUpdate.SUM_LOCAL,
                        SimpleGDParameterUpdate.AVG
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

        //LogisticRegressionModel mdl = trainer.fit(ignite, dataCache, vectorizer);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        LogisticRegressionModel model = trainer.fit(
                ignite,
                ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

//        double accuracy = Evaluator.evaluate(
//                ignite.cache(cacheName),
//                model,
//                vectorizer
//        ).accuracy();

        double rmse = Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                model,
                vectorizer,
                new RegressionMetrics()
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.LogisticRegression), new MyMlModel(model, "LogisticRegression 的均方根误差：" + String.valueOf(rmse)));
        }
    }

}
