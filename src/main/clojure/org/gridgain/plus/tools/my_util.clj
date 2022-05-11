(ns org.gridgain.plus.tools.my-util
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (java.util List ArrayList Hashtable Date Iterator)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (com.google.gson Gson GsonBuilder)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [gson [] com.google.gson.Gson]]
        ))

; 获取 gson 与 :methods 中定义的 gson 对应
(defn -gson []
    (.create (.setDateFormat (.enableComplexMapKeySerialization (GsonBuilder.)) "yyyy-MM-dd HH:mm:ss")))

;; 执行 sql
;(defn run-sql [sql args cache]
;    (.getAll (.query cache (.setArgs (SqlFieldsQuery. sql) args))))

; 调用方法
;(defn my_invoke [^Ignite ignite ^String scenes_name]
;    ())

(defn to_arryList
    ([lst] (to_arryList lst (ArrayList.)))
    ([[f & r] ^ArrayList lst]
     (if (some? f)
         (recur r (doto lst (.add f)))
         lst)))

(defn -toList [lst]
    (to_arryList lst))



















































