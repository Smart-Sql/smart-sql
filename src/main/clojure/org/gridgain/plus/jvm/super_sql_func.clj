(ns org.gridgain.plus.jvm.super-sql-func
    (:require
        [org.gridgain.plus.jvm.my-plus-func :as my-plus-func]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql)
             (java.sql Timestamp)
             (java.math BigDecimal)
             (org.jvm MyInfoService)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.jvm.SuperSqlFunc
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn show-msg [msg]
    (.showMsg (.getMyInfo (MyInfoService/getInstance)) msg))