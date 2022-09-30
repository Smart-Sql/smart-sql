package org.gridgain.smart.ml.knn;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import java.util.Hashtable;

/**
 * KNN 二分类，label 只能是 0 或者 1
 * */
public class MyKNNClassificationUtil {

    public static void getMdlToCache(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        KNNClassificationTrainer trainer = new KNNClassificationTrainer()
                .withDistanceMeasure(new EuclideanDistance())
                .withWeighted(true);
                //.withDataTtl(600);

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

        KNNClassificationModel model = trainer.fit(
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
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.KNNClassification), new MyMlModel(model, "KNNClassification 的精确度：" + String.valueOf(accuracy)));
        }
    }
}
