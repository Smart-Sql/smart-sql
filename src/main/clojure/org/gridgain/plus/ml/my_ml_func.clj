(ns org.gridgain.plus.ml.my-ml-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar MyLetLayer)
             (com.google.common.base Strings)
             (cn.plus.model MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.gridgain.myservice MyLoadSmartSqlService)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache.affinity.rendezvous RendezvousAffinityFunction)
             (java.util List ArrayList Hashtable Date Iterator)
             (org.apache.ignite.ml.math.primitives.vector VectorUtils)
             (org.gridgain.smart.ml MyTrianDataUtil)
             (cn.plus.model.ddl MyCachePK MyMlCaches MyTransData MyMlShowData MyTransDataLoad)
             (org.gridgain.smart.ml.model MyMLMethodName MyMModelKey MyMPs)
             (org.gridgain.smart.ml.regressions.linear MyLinearRegressionUtil)
             (org.gridgain.smart.ml.regressions.logistic MyLogisticRegressionUtil)
             (org.gridgain.smart.ml.svm MySVMLinearClassificationUtil)
             (org.gridgain.smart.ml.tree MyDecisionTreeClassificationUtil MyDecisionTreeRegressionUtil)
             (org.gridgain.smart.ml.tree.randomforest MyRandomForestClassifierUtil MyRandomForestRegressionUtil)
             (org.gridgain.smart.ml.knn MyKNNClassificationUtil MyKNNRegressionUtil)
             (org.gridgain.smart.ml.clustering MyKMeansUtil MyGmmUtil)
             (org.gridgain.smart.ml.nn MyMlpUtil)
             (org.tools MyConvertUtil))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.ml.MyMlFunc
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [myGetCacheName [String String] String]
                  ^:static [myFit [org.apache.ignite.Ignite Object java.util.Hashtable] Object]
                  ^:static [myPredict [org.apache.ignite.Ignite Object java.util.Hashtable] Object]]
        ))

(defn get-cache-name [schema_name table_name]
    (format "sm_ml_%s_%s" (str/lower-case schema_name) (str/lower-case table_name)))

(defn -myGetCacheName [^String schema_name ^String table_name]
    (get-cache-name schema_name table_name))

(defn fit-model [^Ignite ignite ^String cache-name ^String func-name ^String preprocessor ^Hashtable func-ps]
    (cond (or (my-lexical/is-eq? func-name "LinearRegression") (my-lexical/is-eq? func-name "LinearRegressionLSQR") (my-lexical/is-eq? func-name "LinearRegressionLSQR")) (MyLinearRegressionUtil/getMdl ignite cache-name func-name preprocessor func-ps)
          (my-lexical/is-eq? func-name "LogisticRegression") (MyLogisticRegressionUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "svm") (MySVMLinearClassificationUtil/getMdlToCache ignite cache-name)
          (my-lexical/is-eq? func-name "DecisionTreeClassification") (MyDecisionTreeClassificationUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "DecisionTreeRegression") (MyDecisionTreeRegressionUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "RandomForestClassification") (MyRandomForestClassifierUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "RandomForestRegression") (MyRandomForestRegressionUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "KNNClassification") (MyKNNClassificationUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "KNNRegression") (MyKNNRegressionUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "KMeans") (MyKMeansUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "GMM") (MyGmmUtil/getMdlToCache ignite cache-name func-ps)
          (my-lexical/is-eq? func-name "NeuralNetwork") (MyMlpUtil/getMdlToCache ignite cache-name func-ps)
          ))

; 训练模型
(defn ml-fit [^Ignite ignite group_id ^Hashtable ht]
    (let [{table_name "table_name" schema_name "schema_name" ml_func_name "ml_func_name" preprocessor "preprocessor" ml_func_params "ml_func_params"} ht]
        (cond (my-lexical/is-eq? schema_name "public") (fit-model ignite (get-cache-name schema_name table_name) ml_func_name preprocessor ml_func_params)
              (my-lexical/is-str-not-empty? schema_name) (fit-model ignite (get-cache-name schema_name table_name) ml_func_name preprocessor ml_func_params)
              (my-lexical/is-eq? schema_name (second group_id)) (throw (Exception. "没有权限训练 %s 中的数据！" schema_name))
              )))

(defn to-ml-double [params]
    (cond (my-lexical/is-seq? params) (VectorUtils/of (double-array (map to-ml-double params)))
          (instance? MyVar params) (to-ml-double (.getVar params))
          :else
          (MyConvertUtil/ConvertToDouble params)
          ))

; 预测数据
(defn predict-model [^Ignite ignite ^String cache-name ^String func-name params]
    (let [cache (.cache ignite "my_ml_model")]
        (cond (my-lexical/is-eq? func-name "LinearRegression") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/LinearRegressionSGD))) (to-ml-double params))
              (my-lexical/is-eq? func-name "LinearRegressionSGD") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/LinearRegressionSGD))) (to-ml-double params))
              (my-lexical/is-eq? func-name "LinearRegressionLSQR") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/LinearRegressionLSQR))) (to-ml-double params))
              (my-lexical/is-eq? func-name "LogisticRegression") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/LogisticRegression))) (to-ml-double params))
              (my-lexical/is-eq? func-name "BaggedLogisticRegressionSGD") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/BaggedLogisticRegressionSGD))) (to-ml-double params))
              (my-lexical/is-eq? func-name "DecisionTreeClassification") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/DecisionTreeClassification))) (to-ml-double params))
              (my-lexical/is-eq? func-name "DecisionTreeRegression") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/DecisionTreeRegression))) (to-ml-double params))
              (my-lexical/is-eq? func-name "RandomForestClassification") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/RandomForestClassification))) (to-ml-double params))
              (my-lexical/is-eq? func-name "RandomForestRegression") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/RandomForestRegression))) (to-ml-double params))
              (my-lexical/is-eq? func-name "KNNClassification") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/KNNClassification))) (to-ml-double params))
              (my-lexical/is-eq? func-name "KNNRegression") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/KNNRegression))) (to-ml-double params))
              (my-lexical/is-eq? func-name "KMeans") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/KMeans))) (to-ml-double params))
              (my-lexical/is-eq? func-name "GMM") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/GMM))) (to-ml-double params))
              (my-lexical/is-eq? func-name "NeuralNetwork") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/NeuralNetwork))) (to-ml-double params))
              )))

; 预测数据
(defn ml-predict [^Ignite ignite group_id ^Hashtable ht]
    (let [{table_name "table_name" schema_name "schema_name" ml_func_name "ml_func_name" params "params"} ht]
        (cond (my-lexical/is-eq? schema_name "public") (predict-model ignite (get-cache-name schema_name table_name) ml_func_name params)
              (my-lexical/is-str-not-empty? schema_name) (predict-model ignite (get-cache-name schema_name table_name) ml_func_name params)
              (my-lexical/is-eq? schema_name (second group_id)) (throw (Exception. "没有权限调用 %s 训练的结果！" schema_name))
              )))

(defn -myFit [^Ignite ignite group_id ^Hashtable ht]
    (ml-fit ignite group_id ht))

(defn -myPredict [^Ignite ignite group_id ^Hashtable ht]
    (ml-predict ignite group_id ht))


























