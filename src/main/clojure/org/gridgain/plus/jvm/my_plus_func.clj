(ns org.gridgain.plus.jvm.my-plus-func
  (:gen-class
    :implements [org.jvm.IMyInfo]
    ; 生成 class 的类名
    :name org.gridgain.plus.jvm.MySuperFuncs
    ; 是否生成 class 的 main 方法
    :main false
    ; 生成 java 静态的方法
    ;:methods [[showMsg [String] String]]
    ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
    ))

(defn -showMsg [this ^String s]
  (format "%s 是大帅哥！" s))

;(defn showMsg [^String s]
;  (format "%s 是大帅哥！" s))