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
             (java.util List ArrayList Hashtable Date Iterator)
             (org.gridgain.smart.ml MyMlDataCache)
             (cn.plus.model.ddl MyCachePK MyMlCaches MyTransData)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.ml.MyMlTrainData
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(defn hastable-to-cache [^Hashtable ht]
    (doto (MyMlCaches.) (.setDataset_name (get ht "dataset_name"))))

; 定一个分布式的矩阵
(defn create-train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond (and (my-lexical/is-eq? ds-name "MY_META") (not (contains? ht "dataset_name"))) (throw (Exception. "MY_META 下面不能创建机器学习的训练数据！"))
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "dataset_name") (my-lexical/is-eq? (get ht "dataset_name") "MY_META")) (throw (Exception. "MY_META 下面不能创建机器学习的训练数据！"))
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "dataset_name") (not (my-lexical/is-eq? (get ht "dataset_name") "MY_META"))) (do
                                                                                                                                                        (println ht)
                                                                                                                                                        (println (MyMlDataCache/hashtableToCache ht))
                                                                                                                                                        (MyMlDataCache/getMlDataCache ignite ht))
              (not (contains? ht "dataset_name")) (MyMlDataCache/getMlDataCache ignite (doto ht (.put "dataset_name" (str/lower-case ds-name))))
              (and (contains? ht "dataset_name") (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name))) (MyMlDataCache/getMlDataCache ignite ht)
              (and (contains? ht "dataset_name") (not (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name)))) (throw (Exception. "不能在其它非公共数据集下面不能创建机器学习的训练数据！"))
              :else
              (throw (Exception. "不能创建机器学习的训练数据！"))
            )
        ))

; 是否有个叫 "训练数据集" 的矩阵
(defn has-train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "dataset_name") (not (my-lexical/is-eq? (get ht "dataset_name") "MY_META"))) (MyMlDataCache/hasTrainMatrix ignite ht)
              (not (contains? ht "dataset_name")) (MyMlDataCache/hasTrainMatrix ignite (doto ht (.put "dataset_name" (str/lower-case ds-name))))
              (and (contains? ht "dataset_name") (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name))) (MyMlDataCache/hasTrainMatrix ignite ht)
              (and (contains? ht "dataset_name") (not (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name)))) (throw (Exception. "不能在其它非公共数据集下面查询机器学习的训练数据！"))
              :else
              (throw (Exception. "不存在机器学习的训练数据！"))
              )
        ))

; 删除有个叫 "训练数据集" 的矩阵
(defn drop-train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
            (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "dataset_name") (not (my-lexical/is-eq? (get ht "dataset_name") "MY_META"))) (MyMlDataCache/dropTrainMatrix ignite ht)
            (not (contains? ht "dataset_name")) (MyMlDataCache/dropTrainMatrix ignite (doto ht (.put "dataset_name" (str/lower-case ds-name))))
            (and (contains? ht "dataset_name") (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name))) (MyMlDataCache/dropTrainMatrix ignite ht)
            (and (contains? ht "dataset_name") (not (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name)))) (throw (Exception. "不能在其它非公共数据集下面删除机器学习的训练数据！"))
            :else
            (throw (Exception. "不存在机器学习的训练数据！"))
            )
        ))

; 为分布式矩阵添加数据
(defn train-matrix [^Ignite ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond
            (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "dataset_name") (not (my-lexical/is-eq? (get ht "dataset_name") "MY_META"))) (MyMlDataCache/trainMatrix ignite ht)
            (not (contains? ht "dataset_name")) (MyMlDataCache/trainMatrix ignite (doto ht (.put "dataset_name" (str/lower-case ds-name))))
            (and (contains? ht "dataset_name") (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name))) (MyMlDataCache/trainMatrix ignite ht)
            (and (contains? ht "dataset_name") (not (contains? #{(str/lower-case (get ht "dataset_name")) "public"} (str/lower-case ds-name)))) (throw (Exception. "不能在其它非公共数据集下面添加机器学习的训练数据！"))
            :else
            (throw (Exception. "不存在机器学习的训练数据！"))
            )
        ))


































