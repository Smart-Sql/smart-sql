(ns org.gridgain.plus.context.my-context
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.context.MyContext
        ; 是否生成 class 的 main 方法
        :main false
        ))

; 获取用户组编译上下文
; 1、权限视图
; 2、场景
; 3、数据集

;(def ignite (IgnitionEx/start "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml"))
;
;(def my_meta (.cache ignite "my_meta_cache_context"))
;
; 获取 方法或者是场景
(defn get-func-scenes [ignite func-name]
    (cond (.containsKey (.cache ignite "my_meta_cache_all_func") func-name) "func"
          (.containsKey (.cache ignite "my_meta_cache_all_scenes") func-name) "scenes"
          ;(.containsKey (.cache ignite "my_meta_cache_all_builtin") func-name) "builtin"
          :else
          "builtin"
          ))

;; 获取 group 的 data set
;(defn user-data-set [ignite group-id]
;    (when-let [m (MyContextCacheUtil/getContextCache ignite group-id)]
;        (.getData_set m)))
;
;; 判断是否有该表的访问权限
;(defn user-data-set-table [ignite group-id table]
;    (when-let [m (MyContextCacheUtil/getContextCache ignite group-id)]
;        (contains? (.getData_set_table m) table)))
;
;; 获取 select_views 的权限限制
;(defn user-select-view-ast [ignite group-id table]
;    (when-let [m (MyContextCacheUtil/getContextCache ignite group-id)]
;        (get (.getSelect_views m) table)))
;
;; 获取 update_views 的权限限制
;(defn user-update-view-ast [ignite group-id table]
;    (when-let [m (MyContextCacheUtil/getContextCache ignite group-id)]
;        (get (.getUpdate_views m) table)))
;
;; 获取 insert_views 的权限限制
;(defn user-insert-view-ast [ignite group-id table]
;    (when-let [m (MyContextCacheUtil/getContextCache ignite group-id)]
;        (get (.getInsert_views m) table)))
;
;; 获取 delete_views 的权限限制
;(defn user-delete-view-ast [ignite group-id table]
;    (when-let [m (MyContextCacheUtil/getContextCache ignite group-id)]
;        (get (.getDelete_views m) table)))



















































