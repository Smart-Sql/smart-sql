(ns org.gridgain.plus.tools.my-date-util
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyDateUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [getFormTime [String] String]]
        ))

(defn get-form-time [^String line]
    (cond (re-find #"^(?i)\d{4}-\d{2}-\d{2}$|^(?i)\d{4}-\d{2}-\d{1}$|^(?i)\d{4}-\d{1}-\d{2}$|^(?i)\d{4}-\d{1}-\d{1}$" line) "yyyy-MM-dd"
          (re-find #"^(?i)\d{4}-\d{2}-\d{2}\s+\d{2}\:\d{2}:\d{2}$|^(?i)\d{4}-\d{2}-\d{1}\s+\d{2}\:\d{2}:\d{2}$|^(?i)\d{4}-\d{1}-\d{2}\s+\d{2}\:\d{2}:\d{2}$|^(?i)\d{4}-\d{1}-\d{1}\s+\d{2}\:\d{2}:\d{2}$" line) "yyyy-MM-dd HH:mm:ss"
          (re-find #"^(?i)\d{4}/\d{2}/\d{2}\s+\d{2}\:\d{2}:\d{2}$|^(?i)\d{4}/\d{2}/\d{1}\s+\d{2}\:\d{2}:\d{2}$|^(?i)\d{4}/\d{1}/\d{2}\s+\d{2}\:\d{2}:\d{2}$|^(?i)\d{4}/\d{1}/\d{1}\s+\d{2}\:\d{2}:\d{2}$" line) "yyyy/MM/dd HH:mm:ss"
          (re-find #"^(?i)\d{4}/\d{2}/\d{2}$|^(?i)\d{4}/\d{2}/\d{1}$|^(?i)\d{4}/\d{1}/\d{2}$|^(?i)\d{4}/\d{1}/\d{1}$" line) "yyyy/MM/dd"
          (re-find #"^(?i)\d{8}\s+\d{2}\:\d{2}:\d{2}$" line) "yyyyMMdd HH:mm:ss"
          (re-find #"^(?i)\d{8}$" line) "yyyyMMdd"
          (re-find #"^(?i)\d{17}$" line) "yyyyMMddHHmmssSSS"
          (re-find #"^(?i)\d{14}$" line) "yyyyMMddHHmmss"
          ))

(defn -getFormTime [^String line]
    (get-form-time line))