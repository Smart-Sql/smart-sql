(ns org.gridgain.plus.tools.my-scheduler
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             com.google.gson.GsonBuilder
             org.tools.MyDbUtil)
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyScheduler
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [getScheduler [] java.lang.Runnable]
                  ^{:static true} [addJob [org.apache.ignite.Ignite java.lang.String java.lang.String java.lang.String] org.apache.ignite.scheduler.SchedulerFuture]
                  ^{:static true} [addJob_1 [org.apache.ignite.Ignite java.lang.String java.lang.String] org.apache.ignite.scheduler.SchedulerFuture]
                  ]
        ))

; http://clojure-doc.org/articles/language/interop.html
(defn -getScheduler []
    (proxy [Object Runnable] []
        (run []
            (do
                (println (System/currentTimeMillis))
                (println "吴大富是大帅哥！")
                (Thread/sleep 8000)))))

(defn my-method [^String name]
    (do
        (println (System/currentTimeMillis))
        (println name)
        (Thread/sleep 8000)))


(defn get-scheduler [my-method]
    (proxy [Object Runnable] []
        (run []
            (my-method))))

;(defn -addJob [^Ignite ignite ^String name ^String msg]
;    (.scheduleLocal (.scheduler ignite) name (get-scheduler (my-method msg)) "{1,3} * * * * *"))

(defn -addJob [^Ignite ignite ^String name ^String msg ^String pattern]
    (.scheduleLocal (.scheduler ignite) name (get-scheduler (my-method msg)) pattern))

(defn -addJob_1 [^Ignite ignite ^String name ^String msg]
    (.scheduleLocal (.scheduler ignite) name (proxy [Object Runnable] []
                                                 (run []
                                                     (do
                                                         (println (System/currentTimeMillis))
                                                         (println msg)
                                                         (Thread/sleep 8000)))) "{1,3} * * * * *"))


















































