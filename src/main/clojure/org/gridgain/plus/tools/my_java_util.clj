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
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.gridgain.smart MyVar))
    (:gen-class
        :implements [org.gridgain.superservice.IJavaUtil]
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyJavaUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [[toArray [Object] java.util.ArrayList]]
        ))

(declare my-to-arrayList to-my-val)

(defn get-vs [m]
    (if-not (instance? MyVar m)
        m
        (get-vs (.getVar m))))

(defn my-to-arrayList
    ([lst] (my-to-arrayList lst (ArrayList.)))
    ([[f & r] ^ArrayList lst]
     (if (some? f)
         (recur r (doto lst (.add (to-my-val f))))
         lst)))

(defn my-to-dic [m]
    (loop [[f & r] (keys m) ht (Hashtable.)]
        (if (some? f)
            (recur r (doto ht (.put (to-my-val f) (to-my-val (get m f)))))
            ht)))

(defn to-my-val [m]
    (cond (my-lexical/is-seq? m) (my-to-arrayList m)
          (map? m) (my-to-dic m)
          :else m
          ))

(defn -toArrayOrHashtable [this lst]
    (to-my-val lst))

(defn -isSeq [this m]
    (if (instance? MyVar m)
        (my-lexical/is-seq? (get-vs m))
        (my-lexical/is-seq? m)))

(defn -isDic [this m]
    (my-lexical/is-dic? m))

(defn -myToArrayList [this m]
    (if (instance? MyVar m)
        (my-to-arrayList (get-vs m))
        (my-to-arrayList m))
    )
