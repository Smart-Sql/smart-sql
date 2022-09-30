package org.gridgain.smart.ml.knn;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.knn.regression.KNNRegressionModel;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndexType;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import java.util.Hashtable;

public class MyKNNRegressionUtil {

    public static void getMdlToCache(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        KNNRegressionTrainer trainer = new KNNRegressionTrainer()
                .withDistanceMeasure(new ManhattanDistance())
                .withIdxType(SpatialIndexType.BALL_TREE)
                .withWeighted(true);

        if (funcPs != null)
        {
            if (funcPs.containsKey("k"))
            {
                trainer.withK(MyConvertUtil.ConvertToInt(funcPs.get("k")));
            }
        }

        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);

        //LogisticRegressionModel mdl = trainer.fit(ignite, dataCache, vectorizer);
        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);

        KNNRegressionModel model = trainer.fit(
                ignite,
                ignite.cache(cacheName),
                split.getTrainFilter(),
                vectorizer
        );

        double rmse = Evaluator.evaluate(
                ignite.cache(cacheName),
                model,
                vectorizer,
                new RegressionMetrics()
        );

        if (model != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.KNNRegression), new MyMlModel(model, "KNNRegression 的均方根误差：" + String.valueOf(rmse)));
        }
    }
}
