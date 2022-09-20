package org.gridgain.smart.ml.regressions.linear;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * 线性回归 LSQR
 * 最小二乘法
 * 归一化
 * */
public class MyLinearRegressionLSQRMinMaxScalerUtil {
    private DatasetTrainer<LinearRegressionModel, Double> trainer;
    private Vectorizer<Long, Vector, Integer, Double> vectorizer;
    private TrainTestSplit<Long, Vector> split;
    private MinMaxScalerTrainer<Long, Vector> minMaxScalerTrainer;

    public MyLinearRegressionLSQRMinMaxScalerUtil()
    {
        this.trainer = new LinearRegressionLSQRTrainer()
                .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0));
        this.vectorizer = new DummyVectorizer<Long>()
                .labeled(Vectorizer.LabelCoordinate.FIRST);
        this.split = new TrainTestDatasetSplitter<Long, Vector>().split(0.8);
        this.minMaxScalerTrainer = new MinMaxScalerTrainer<>();
    }

}


















































