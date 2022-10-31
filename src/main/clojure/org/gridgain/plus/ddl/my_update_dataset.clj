(ns org.gridgain.plus.ddl.my-update-dataset
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyUpdateDataSet
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn get_update_data_set_obj [^String sql]
    (let [update_dataset (re-find #"^(?i)update\s+DATASET\s+" sql) last_line (str/replace sql #"^(?i)update\s+DATASET\s+" "")]
        (if (some? update_dataset)
            (if-let [items (str/split last_line #"\s+where\s+")]
                (if (= (count items) 2)
                    {:update_dataset update_dataset :schema_name (nth items 0) :time (nth items 1)}
                    (throw (Exception. "删除数据集语句错误！")))
                (throw (Exception. "删除数据集语句错误！")))
            (throw (Exception. "删除数据集语句错误！")))
        (throw (Exception. "删除数据集语句错误！"))))

(defn update_dataset [^Ignite ignite ^Long group_id ^String sql_line]
    ())




















































