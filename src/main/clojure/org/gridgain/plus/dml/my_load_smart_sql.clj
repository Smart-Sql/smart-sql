(ns org.gridgain.plus.dml.my-load-smart-sql
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MyNoSqlCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (cn.plus.model.db MyScenesCache MyScenesCachePk ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator Hashtable)
             (java.sql Timestamp)
             (java.math BigDecimal)
             (cn.log MyLogger)
             )
    (:gen-class
        :implements [org.gridgain.superservice.ILoadSmartSql]
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyLoadSmartSql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [superSql [org.apache.ignite.Ignite Object Object] String]
        ;          ^:static [getGroupId [org.apache.ignite.Ignite String] Boolean]]
        ))

(defn save-to-cache [^Ignite ignite ^Long group_id ^String func-name ^String sql_code ^String smart_code]
    (let [pk (MyScenesCachePk. group_id func-name) cache (.cache ignite "my_scenes")]
        (if-not (.containsKey cache pk)
            (.put cache pk (MyScenesCache. group_id func-name sql_code smart_code)))))

(defn load-smart-sql [^Ignite ignite ^Long group_id ^String code]
    (loop [[f & r] (my-smart-sql/re-smart-segment (my-smart-sql/get-smart-segment (my-lexical/to-back code)))]
        (if (some? f)
            (do
                (cond (and (string? (first f)) (my-lexical/is-eq? (first f) "function")) (let [[sql func-name] (my-smart-clj/my-smart-to-clj ignite group_id f)]
                                                                                             (eval (read-string sql))
                                                                                             (save-to-cache ignite group_id func-name sql code))
                      (and (string? (first f)) (contains? #{"insert" "update" "delete" "select"} (str/lower-case (first f)))) (my-smart-db-line/query_sql ignite group_id f)
                      :else
                      (if (string? (first f))
                          (my-smart-clj/smart-lst-to-clj ignite group_id f)
                          (my-smart-clj/smart-lst-to-clj ignite group_id (apply concat f)))
                      )
                (recur r)))))


(defn -loadSmartSql [this ^Ignite ignite ^Long group_id ^String code]
    (load-smart-sql ignite group_id code))










































