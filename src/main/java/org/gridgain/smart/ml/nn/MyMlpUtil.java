package org.gridgain.smart.ml.nn;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.structures.LabeledVector;
import org.gridgain.smart.ml.model.MyMLMethodName;
import org.gridgain.smart.ml.model.MyMModelKey;
import org.gridgain.smart.ml.model.MyMlModel;
import org.tools.MyConvertUtil;

import javax.cache.Cache;
import java.util.Hashtable;
import java.util.List;

/**
 * 神经网络，多层感知器
 * */
public class MyMlpUtil {

    public static void getMdlToCache(final Ignite ignite, final String cacheName, final Hashtable<String, Object> funcPs)
    {
        String newCacheName = cacheName + "_mlp";
        if (ignite.cache(newCacheName) != null)
        {
            ignite.destroyCache(newCacheName);
        }

        CacheConfiguration<Long, LabeledVector<double[]>> trainingSetCfg = new CacheConfiguration<>();
        trainingSetCfg.setName(newCacheName);
        trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

        IgniteCache<Long, LabeledVector<double[]>> dataCache = ignite.createCache(trainingSetCfg);
        try
        {
            QueryCursor<Cache.Entry<Long, Vector>> ds = ignite.cache(cacheName).query(new ScanQuery<>());
            int inputSize = 0;
            Long index = 1L;
            for (Cache.Entry<Long, Vector> observation : ds) {
                Vector val = observation.getValue();
                Vector inputs = val.copyOfRange(1, val.size());
                double groundTruth = val.get(0);
                if (inputSize == 0)
                {
                    inputSize = inputs.size();
                }

                dataCache.put(index, new LabeledVector<>(inputs, new double[] {MyConvertUtil.ConvertToDouble(groundTruth)}));
                index++;
            }

            Long totalCnt = index;

            // Define a layered architecture.
//            MLPArchitecture arch = new MLPArchitecture(inputSize).
//                    withAddedLayer(10, true, Activators.RELU).
//                    withAddedLayer(1, false, Activators.SIGMOID);

            MLPArchitecture arch = new MLPArchitecture(inputSize);

            int maxIterations = 100;
            int batchSize = 100;
            int locIterations = 100;
            long seed = 1234L;

            if (funcPs != null)
            {
                if (funcPs.containsKey("Layer"))
                {
                    List<Hashtable<String, Object>> layers = (List<Hashtable<String, Object>>) funcPs.get("Layer");
                    for (Hashtable<String, Object> ht : layers)
                    {
                        IgniteDifferentiableDoubleToDoubleFunction afun = null;
                        if (ht.get("activationFunction").toString().toLowerCase().equals("RELU"))
                        {
                            afun = Activators.RELU;
                        }
                        else if (ht.get("activationFunction").toString().toLowerCase().equals("SIGMOID"))
                        {
                            afun = Activators.SIGMOID;
                        }
                        else if (ht.get("activationFunction").toString().toLowerCase().equals("LINEAR"))
                        {
                            afun = Activators.LINEAR;
                        }
                        arch.withAddedLayer(MyConvertUtil.ConvertToInt(ht.get("neuronsCnt")), MyConvertUtil.ConvertToBoolean(ht.get("hasBias")), afun);
                    }
                }

                if (funcPs.containsKey("maxIterations"))
                {
                    maxIterations = MyConvertUtil.ConvertToInt(funcPs.get("maxIterations"));
                }

                if (funcPs.containsKey("batchSize"))
                {
                    batchSize = MyConvertUtil.ConvertToInt(funcPs.get("batchSize"));
                }

                if (funcPs.containsKey("locIterations"))
                {
                    locIterations = MyConvertUtil.ConvertToInt(funcPs.get("locIterations"));
                }

                if (funcPs.containsKey("seed"))
                {
                    seed = MyConvertUtil.ConvertToLong(funcPs.get("seed"));
                }
            }

            // Define a neural network trainer.
            MLPTrainer<SimpleGDParameterUpdate> trainer = new MLPTrainer<>(
                    arch,
                    LossFunctions.MSE,
                    new UpdatesStrategy<>(
                            new SimpleGDUpdateCalculator(0.1),
                            SimpleGDParameterUpdate.SUM_LOCAL,
                            SimpleGDParameterUpdate.AVG
                    ),
                    maxIterations,
                    batchSize,
                    locIterations,
                    seed
            );

            // Train neural network and get multilayer perceptron model.
            MultilayerPerceptron mlp = trainer.fit(ignite, dataCache, new LabeledDummyVectorizer<>());

            long failCnt = 0L;
            // Calculate score.
            for (long i = 1; i <= totalCnt; i++) {
                LabeledVector<double[]> pnt = dataCache.get(i);
                //Matrix predicted = mlp.predict(new DenseMatrix(new double[][] {{pnt.features().get(0), pnt.features().get(1), pnt.features().get(2)}}));
                Matrix predicted = mlp.predict(new DenseMatrix(new double[][] {pnt.features().asArray()}));

                double predictedVal = predicted.get(0, 0);
                double lbl = pnt.label()[0];
                System.out.printf(">>> key: %d\t\t predicted: %.4f\t\tlabel: %.4f\n", i, predictedVal, lbl);
                failCnt += Math.abs(predictedVal - lbl) < 0.5 ? 0 : 1;
            }

            double failRatio = (double)failCnt / totalCnt;

            if (mlp != null)
            {
                ignite.cache("my_ml_model").put(new MyMModelKey(cacheName, MyMLMethodName.NeuralNetwork), new MyMlModel(mlp, "NeuralNetwork 的错误率：" + String.valueOf(failRatio)));
            }

            System.out.println("\n>>> Fail percentage: " + (failRatio * 100) + "%.");
            System.out.println("\n>>> Distributed multilayer perceptron example completed.");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            if (dataCache != null)
            {
                dataCache.destroy();
            }
        }
    }
}
