package org.gridgain.smart.ml.svm;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.svm.SVMLinearClassificationModel;
import org.apache.ignite.ml.svm.SVMLinearClassificationTrainer;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;

/**
 * SVM 线性分类
 * */
public class MySVMLinearClassificationUtil {
    public static void getMdlToCache(final Ignite ignite, final String cacheName)
    {
        Vectorizer<Long, Vector, Integer, Double> vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);

        TrainTestSplit<Long, Vector> split = new TrainTestDatasetSplitter<Long, Vector>()
                .split(0.8);

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

        SVMLinearClassificationTrainer trainer = new SVMLinearClassificationTrainer();

        SVMLinearClassificationModel mdl = trainer.fit(ignite, ignite.cache(cacheName), normalizationPreprocessor);

        double accuracy = Evaluator.evaluate(
                ignite.cache(cacheName),
                split.getTestFilter(),
                mdl,
                normalizationPreprocessor,
                new Accuracy<>()
        );

        if (mdl != null)
        {
            ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.SVM), new MyMlModel(mdl, "SVM 的准确率：" + String.valueOf(accuracy)));
        }
    }
}
