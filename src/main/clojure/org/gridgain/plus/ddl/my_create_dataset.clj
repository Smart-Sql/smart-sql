(ns org.gridgain.plus.ddl.my-create-dataset
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.configuration CacheConfiguration)
             (cn.myservice MyInitFuncService)
             (cn.plus.model.ddl MyDataSet)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyCreateDataSet
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))


(defn get-dataset-name [^String sql]
    (let [lst (my-lexical/to-back (str/lower-case sql))]
        (cond (and (= (count lst) 6) (= '("create" "schema" "if" "not" "exists") (map str/lower-case (take 5 lst)))) (last lst)
              (and (= (count lst) 3) (= '("create" "schema") (map str/lower-case (take 2 lst)))) (last lst)
              :else
              (throw (Exception. "输入字符串错误！"))
              )))

; CREATE DATASET CRM_DATA_SET
(defn create_data_set [^Ignite ignite group_id ^String sql]
    (if (= (first group_id) 0)
        (if-let [schema_name (get-dataset-name sql)]
            (if-not (my-lexical/is-eq? schema_name "my_meta")
                (let [ds-cache (.cache ignite "my_meta_table")]
                    (let [schema_name_u (str/upper-case schema_name)]
                        (if (empty? (.getAll (.query ds-cache (.setArgs (SqlFieldsQuery. "SELECT m.SCHEMA_NAME FROM sys.SCHEMAS AS m WHERE m.SCHEMA_NAME = ?") (to-array [schema_name_u])))))
                            (if (some? (.getOrCreateCache ignite (doto (CacheConfiguration. (str (str/lower-case schema_name) "_meta"))
                                                                     (.setSqlSchema schema_name_u))))
                                (.initSchemaFunc (.getInitFunc (MyInitFuncService/getInstance)) ignite schema_name_u))
                            (throw (Exception. "该数据集已经存在了！"))))
                    )
                (throw (Exception. "该数据集已经存在了！")))
            (throw (Exception. "创建数据集语句的错误！")))
        (throw (Exception. "没有执行语句的权限！"))))













































