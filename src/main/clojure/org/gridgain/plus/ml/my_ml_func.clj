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
             (org.gridgain.smart.ml.regressions.linear MyLinearRegressionLSQRUtil MyLinearRegressionSGDUtil)
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

(defn get-cache-name [dataset_name table_name]
    (format "sm_ml_%s_%s" (str/lower-case dataset_name) (str/lower-case table_name)))

(defn -myGetCacheName [^String dataset_name ^String table_name]
    (get-cache-name dataset_name table_name))

(defn fit-model [^Ignite ignite ^String cache-name ^String func-name ^Hashtable func-ps]
    (cond (my-lexical/is-eq? func-name "LinearRegressionLSQR") (.getMdlToCache (MyLinearRegressionLSQRUtil.) ignite cache-name)
          (my-lexical/is-eq? func-name "LinearRegressionLSQRWithMinMaxScaler") (.getMinMaxScalerMdlToCache (MyLinearRegressionLSQRUtil.) ignite cache-name)
          ))

; 训练模型
(defn ml-fit [^Ignite ignite group_id ^Hashtable ht]
    (let [{table_name "table_name" dataset_name "dataset_name" ml_func_name "ml_func_name" ml_func_params "ml_func_params"} ht]
        (cond (my-lexical/is-eq? dataset_name "public") (fit-model ignite (get-cache-name dataset_name table_name) ml_func_name ml_func_params)
              (my-lexical/is-str-not-empty? dataset_name) (fit-model ignite (get-cache-name dataset_name table_name) ml_func_name ml_func_params)
              (my-lexical/is-eq? dataset_name (second group_id)) (throw (Exception. "没有权限训练 %s 中的数据！" dataset_name))
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
        (cond (my-lexical/is-eq? func-name "LinearRegressionLSQR") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/LinearRegressionLSQR))) (to-ml-double params))
              (my-lexical/is-eq? func-name "LinearRegressionLSQRWithMinMaxScaler") (.predict (.get cache (MyMModelKey. cache-name (MyMLMethodName/LinearRegressionLSQRWithMinMaxScaler))) (to-ml-double params))
              )))

; 预测数据
(defn ml-predict [^Ignite ignite group_id ^Hashtable ht]
    (let [{table_name "table_name" dataset_name "dataset_name" ml_func_name "ml_func_name" params "params"} ht]
        (cond (my-lexical/is-eq? dataset_name "public") (predict-model ignite (get-cache-name dataset_name table_name) ml_func_name params)
              (my-lexical/is-str-not-empty? dataset_name) (predict-model ignite (get-cache-name dataset_name table_name) ml_func_name params)
              (my-lexical/is-eq? dataset_name (second group_id)) (throw (Exception. "没有权限调用 %s 训练的结果！" dataset_name))
              )))

(defn -myFit [^Ignite ignite group_id ^Hashtable ht]
    (ml-fit ignite group_id ht))

(defn -myPredict [^Ignite ignite group_id ^Hashtable ht]
    (ml-predict ignite group_id ht))

























