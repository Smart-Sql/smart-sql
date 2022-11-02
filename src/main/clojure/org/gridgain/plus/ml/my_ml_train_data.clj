(ns org.gridgain.plus.ml.my-ml-train-data
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
             (org.tools MyConvertUtil))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.ml.MyMlTrainData
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [myDoubleTest [Object Object] Object]]
        ))

(defn hastable-to-cache [^Hashtable ht]
    (letfn [(get-ds-name [^Hashtable ht]
                (if (contains? ht "schema_name")
                    (get ht "schema_name")))
            (get-table-name [^Hashtable ht]
                (if (contains? ht "table_name")
                    (get ht "table_name")
                    (throw (Exception. "必须有 table_name 的属性！"))))
            (get-describe [^Hashtable ht]
                (if (contains? ht "describe")
                    (get ht "describe")))
            (get-is-clustering [^Hashtable ht]
                (if (contains? ht "is_clustering")
                    (get ht "is_clustering")
                    false))]
        (doto (MyMlCaches.) (.setSchema_name (get-ds-name ht))
                            (.setTable_name (get-table-name ht))
                            (.setDescribe (get-describe ht))
                            (.setIs_clustering (get-is-clustering ht)))))

(defn hashtable-to-trans-data [^Ignite ignite ^Hashtable ht]
    (letfn [(get-ds-name [^Hashtable ht]
                (if (contains? ht "schema_name")
                    (get ht "schema_name")))
            (get-table-name [^Hashtable ht]
                (if (contains? ht "table_name")
                    (get ht "table_name")
                    (throw (Exception. "必须有 table_name 的属性！"))))
            (get-value [^Hashtable ht]
                (if (contains? ht "value")
                    (get ht "value")
                    (throw (Exception. "必须有 value 的属性！"))))
            (get-label [^Hashtable ht]
                (if (contains? ht "label")
                    (MyConvertUtil/ConvertToDouble (get ht "label"))
                    (throw (Exception. "必须有 label 的属性！"))))
            (get-is-clustering [^Ignite ignite ^String ds-name ^String table-name]
                (let [m (.get (.cache ignite "ml_train_data") (MyCachePK. ds-name table-name))]
                    (.getIs_clustering m)))]
        (doto (MyTransData.)
            (.setSchema_name (get-ds-name ht))
            (.setTable_name (get-table-name ht))
            (.setValue (get-value ht))
            (.setLabel (get-label ht))
            (.setIs_clustering (get-is-clustering ignite (get-ds-name ht) (get-table-name ht))))))

(defn hastable-to-data-load [^Hashtable ht]
    (letfn [(get-ds-name [^Hashtable ht]
                (if (contains? ht "schema_name")
                    (get ht "schema_name")))
            (get-table-name [^Hashtable ht]
                (if (contains? ht "table_name")
                    (get ht "table_name")
                    (throw (Exception. "必须有 table_name 的属性！"))))
            (get-value [^Hashtable ht]
                (if (contains? ht "value")
                    (get ht "value")))
            (get-is-clustering [^Hashtable ht]
                (if (contains? ht "is_clustering")
                    (get ht "is_clustering")
                    false))]
        (doto (MyTransDataLoad.) (.setSchema_name (get-ds-name ht))
                                 (.setTable_name (get-table-name ht))
                                 (.setValue (get-value ht))
                                 (.setIs_clustering (get-is-clustering ht)))))
(defn to-double [m]
           (MyConvertUtil/ConvertToDouble m))

(defn my-to-double
    ([lst] (double-array (map to-double lst)))
    ([item lst]
     (double-array (cons (to-double item) (map to-double lst)))))

(defn ml-train-matrix [^Ignite ignite ^Hashtable ht]
    (if-let [m (hashtable-to-trans-data ignite ht)]
        (let [cacheName (MyTrianDataUtil/getCacheName m)]
            (let [key (.incrementAndGet (.atomicSequence ignite cacheName 0 true))]
                (if (true? (.getIs_clustering m))
                    (let [vs (VectorUtils/of (my-to-double 0 (.getValue m)))]
                        (.put (.cache ignite cacheName) key vs))
                    (let [vs (VectorUtils/of (my-to-double (.getLabel m) (.getValue m)))]
                        (.put (.cache ignite cacheName) key vs)))
                ))))

(defn ml-train-matrix-single [^Ignite ignite ^Hashtable ht]
    (if-let [m (hastable-to-data-load ht)]
        (let [cacheName (MyTrianDataUtil/getCacheName m)]
            (let [key (.incrementAndGet (.atomicSequence ignite cacheName 0 true))]
                (if-not (nil? (.getValue m))
                    (if (true? (.getIs_clustering m))
                        (let [vs (VectorUtils/of (my-to-double (cons 0 (.getValue m))))]
                            (.put (.cache ignite cacheName) key vs))
                        (let [vs (VectorUtils/of (my-to-double (.getValue m)))]
                            (.put (.cache ignite cacheName) key vs)))
                    )))))

(defn ml-create-train-matrix [^Ignite ignite ^MyMlCaches mlCaches]
    (let [cacheName (MyTrianDataUtil/getCacheName mlCaches)]
        (if-not (.cache ignite cacheName)
            (do
                (.getOrCreateCache ignite (MyTrianDataUtil/trainCfg cacheName))
                (.put (.cache ignite "ml_train_data") (MyCachePK. (.getSchema_name mlCaches) (.getTable_name mlCaches)) mlCaches)))))

; 定一个分布式的矩阵
(defn create-train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond (and (my-lexical/is-eq? ds-name "MY_META") (not (contains? ht "schema_name"))) (throw (Exception. "MY_META 下面不能创建机器学习的训练数据！"))
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (my-lexical/is-eq? (get ht "schema_name") "MY_META")) (throw (Exception. "MY_META 下面不能创建机器学习的训练数据！"))
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (not (my-lexical/is-eq? (get ht "schema_name") "MY_META"))) (ml-create-train-matrix ignite (hastable-to-cache ht))
              (not (contains? ht "schema_name")) (ml-create-train-matrix ignite (hastable-to-cache (doto ht (.put "schema_name" (str/lower-case ds-name)))))
              (contains? ht "schema_name") (cond (my-lexical/is-eq? (get ht "schema_name") "public") (ml-create-train-matrix ignite (hastable-to-cache ht))
                                                 (my-lexical/is-eq? (get ht "schema_name") ds-name) (ml-create-train-matrix ignite (hastable-to-cache ht))
                                                 :else (throw (Exception. "不能在其它非公共数据集下面不能创建机器学习的训练数据！")))
              :else
              (throw (Exception. "不能创建机器学习的训练数据！"))
            )
        ))

; 是否有个叫 "训练数据集" 的矩阵
(defn has-train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (not (my-lexical/is-eq? (get ht "schema_name") "MY_META"))) (MyTrianDataUtil/hasTrainMatrix ignite (hastable-to-cache ht))
              (not (contains? ht "schema_name")) (MyTrianDataUtil/hasTrainMatrix ignite (hastable-to-cache (doto ht (.put "schema_name" (str/lower-case ds-name)))))
              (contains? ht "schema_name") (cond (my-lexical/is-eq? (get ht "schema_name") "public") (MyTrianDataUtil/hasTrainMatrix ignite (hastable-to-cache ht))
                                                 (my-lexical/is-eq? (get ht "schema_name") ds-name) (MyTrianDataUtil/hasTrainMatrix ignite (hastable-to-cache ht))
                                                 :else (throw (Exception. "不能在其它非公共数据集下面查询机器学习的训练数据！")))
              :else
              (throw (Exception. "不存在机器学习的训练数据！"))
              )
        ))

; 删除有个叫 "训练数据集" 的矩阵
(defn drop-train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
            (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (not (my-lexical/is-eq? (get ht "schema_name") "MY_META"))) (MyTrianDataUtil/dropTrainMatrix ignite (hastable-to-cache ht))
            (not (contains? ht "schema_name")) (MyTrianDataUtil/dropTrainMatrix ignite (hastable-to-cache (doto ht (.put "schema_name" (str/lower-case ds-name)))))
            (contains? ht "schema_name") (cond (my-lexical/is-eq? (get ht "schema_name") "public") (MyTrianDataUtil/dropTrainMatrix ignite (hastable-to-cache ht))
                                               (my-lexical/is-eq? (get ht "schema_name") ds-name) (MyTrianDataUtil/dropTrainMatrix ignite (hastable-to-cache ht))
                                               :else (throw (Exception. "不能在其它非公共数据集下面删除机器学习的训练数据！")))
            :else
            (throw (Exception. "不存在机器学习的训练数据！"))
            )
        ))

; 为分布式矩阵添加数据
(defn train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
            (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (not (my-lexical/is-eq? (get ht "schema_name") "MY_META"))) (ml-train-matrix ignite ht)
            (not (contains? ht "schema_name")) (ml-train-matrix ignite (hastable-to-cache (doto ht (.put "schema_name" (str/lower-case ds-name)))))
            (contains? ht "schema_name") (cond (my-lexical/is-eq? (get ht "schema_name") "public") (ml-train-matrix ignite ht)
                                               (my-lexical/is-eq? (get ht "schema_name") ds-name) (ml-train-matrix ignite ht)
                                               :else (throw (Exception. "不能在其它非公共数据集下面添加机器学习的训练数据！")))
            :else
            (throw (Exception. "不存在机器学习的训练数据！"))
            )
        ))

; 为分布式矩阵添加数据 -- 在 load csv 中使用
(defn train-matrix-single [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
            (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (not (my-lexical/is-eq? (get ht "schema_name") "MY_META"))) (ml-train-matrix-single ignite ht)
            (not (contains? ht "schema_name")) (ml-train-matrix-single ignite (hastable-to-cache (doto ht (.put "schema_name" (str/lower-case ds-name)))))
            (contains? ht "schema_name") (cond (my-lexical/is-eq? (get ht "schema_name") "public") (ml-train-matrix-single ignite ht)
                                               (my-lexical/is-eq? (get ht "schema_name") ds-name) (ml-train-matrix-single ignite ht)
                                               :else (throw (Exception. "不能在其它非公共数据集下面添加机器学习的训练数据！")))
            :else
            (throw (Exception. "不存在机器学习的训练数据！"))
            )
        ))

; 测试 double-array
;(defn -myDoubleTest [item lst]
;    (letfn [(to-double [m]
;                (MyConvertUtil/ConvertToDouble m))]
;        (double-array (cons (to-double item) (map to-double lst)))))

































