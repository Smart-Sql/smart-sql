(ns org.gridgain.plus.sql.my-smart-scenes
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar)
             (org.tools MyPlusUtil)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartScenes
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [invokeScenes [org.apache.ignite.Ignite Long String java.util.List] Object]
                  ^:static [invokeScenesLink [org.apache.ignite.Ignite Long String java.util.List] Object]]
        ))

; 调用 func
(defn my-invoke-func [^Ignite ignite ^String method-name & ps]
    (MyPlusUtil/invokeFunc ignite method-name ps))

; 调用 scenes
(defn my-invoke-scenes [^Ignite ignite ^Long group_id ^String method-name & ps]
    (let [my-method-name (str/lower-case method-name)]
        (try
            (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps))
            (catch Exception e
                (let [m (.get (.cache ignite "my_scenes") (MyScenesCachePk. (str/lower-case my-method-name) group_id))]
                    (my-lexical/get-value (apply (eval (read-string (.getSql_code m))) ignite group_id ps))
                    (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps)))))))

; 调用 scenes
(defn my-invoke-scenes-link [^Ignite ignite ^Long group_id ^String method-name & ps]
    (let [my-method-name (str/lower-case method-name)]
        (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps))))

; 首先调用方法，如果不存在，在从 cache 中读取数据在执行
(defn -invokeScenes [^Ignite ignite ^Long group_id ^String method-name ^List ps]
    (my-invoke-scenes ignite group_id method-name ps))

(defn -invokeScenesLink [^Ignite ignite ^Long group_id ^String method-name ^List ps]
    (my-invoke-scenes-link ignite group_id method-name ps))












































