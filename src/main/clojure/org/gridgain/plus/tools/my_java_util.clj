(ns org.gridgain.plus.tools.my-java-util
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (java.util List ArrayList Hashtable Date Iterator)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (com.google.gson Gson GsonBuilder)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery))
    (:gen-class
        :implements [org.gridgain.superservice.IJavaUtil]
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyJavaUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [[toArray [Object] java.util.ArrayList]]
        ))

(defn -toArray [this lst]
    (if (my-lexical/is-seq? lst)
        (my-lexical/to_arryList lst)
        lst))