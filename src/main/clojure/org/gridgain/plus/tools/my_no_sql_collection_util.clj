(ns org.gridgain.plus.tools.my-no-sql-collection-util
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.transactions Transaction)
             (org.tools MyConvertUtil KvSql)
             (cn.plus.model MyLogCache SqlType)
             (cn.mysuper.model MyUrlToken)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (cn.plus.model.db MyScenesCache)
             (java.util List ArrayList Hashtable Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyNoSqlCollectionUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn get-dic-lst-items
    ([lst] (letfn [(get-lst-obj
                       ([lst] (get-lst-obj lst [] [] [] []))
                       ([[f & r] stack stack-lst stack-line lst]
                        (if (some? f)
                            (cond (= f \[) (if (= (count stack) 0)
                                               (recur r (conj stack f) stack-lst [] (conj lst (str/join stack-line)))
                                               (throw (Exception. (format "字符串错误！%s" (str/join (cons f r))))))
                                  (= f \]) (if (> (count stack) 0)
                                               (if (= (count stack) 1)
                                                   (recur r [] [] stack-line (conj lst (str/join stack-lst)))
                                                   (recur r (pop stack) stack-lst stack-line lst))
                                               (throw (Exception. (format "字符串错误！%s" (str/join (cons f r))))))
                                  (> (count stack) 0) (recur r stack (conj stack-lst f) stack-line lst)
                                  :else
                                  (recur r stack stack-lst (conj stack-line f) lst)
                                  )
                            (if (and (empty? stack) (empty? stack-line) (empty? stack-lst) (= (count lst) 2))
                                {:item (nth lst 0) :index (nth lst 1)}))))]
               (loop [[f & r] (get-dic-lst-items lst [] []) rs-lst []]
                   (if (some? f)
                       (if-let [m (get-lst-obj f)]
                           (recur r (conj rs-lst m))
                           (recur r (conj rs-lst f))
                           )
                       rs-lst)
                   )))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (= f \.) (if (> (count stack) 0)
                            (recur r [] (concat lst [(str/join stack) "."]))
                            (recur r [] (concat lst ["."]))
                            )
               :else
               (recur r (conj stack f) lst)
               )
         (if (> (count stack) 0)
             (concat lst [(str/join stack)])
             lst))))

(defn get-vs-dic [vs-obj items-line]
    (loop [[f & r] (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-obj vs-obj]
        (if (some? f)
            (if (map? f)
                (if (= (get f :item) "")
                    (recur r (nth my-obj (MyConvertUtil/ConvertToInt (get f :index))))
                    (recur r (nth (get my-obj (get f :item)) (MyConvertUtil/ConvertToInt (get f :index)))))
                (recur r (get my-obj f)))
            (if-not (= my-obj vs-obj)
                my-obj
                nil)))
    )

; [f & r]: items-lst
; vs-obj: cache 的值
; query-vs: 要修改的值
(defn set-cache-vs-lst [[f & r] vs-obj query-vs]
    (if (some? f)
        (cond (map? vs-obj) (if (nil? r)
                                (assoc vs-obj f query-vs)
                                (recur r (get vs-obj f) query-vs))
              (and (my-lexical/is-seq? vs-obj) (map? f)) (if (nil? r)
                                                                                    (loop [[f-vs & r-vs] vs-obj index 0 my-index (get f :index) vs-lst []]
                                                                                        (if (some? f-vs)
                                                                                            (if-not (= index (MyConvertUtil/ConvertToLong my-index))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst f-vs))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst query-vs)))
                                                                                            vs-lst))
                                                                                    (loop [[f-vs & r-vs] vs-obj index 0 my-index (get f :index) vs-lst []]
                                                                                        (if (some? f-vs)
                                                                                            (if-not (= index (MyConvertUtil/ConvertToLong my-index))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst f-vs))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst (set-cache-vs-lst r f-vs query-vs))))
                                                                                            vs-lst)))
              :else
              query-vs
              )
        ))

; vs-obj:
(defn set-cache-vs [vs-obj items-line query-vs]
    (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) vs-obj query-vs))

(defn get-vs-map [^Ignite ignite ^String cache-name ^String query-line ^String query-items-line]
    (if-let [vs-obj (.get (.cache ignite cache-name) query-line)]
        (get-vs-dic vs-obj query-items-line)))

(defn get-vs [^Ignite ignite ^String cache-name ^String key]
    (.get (.cache ignite cache-name) key))

;; 添加
;(defn no-sql-add [^Ignite ignite ^String cache-name ^String key lst item]
;    (cond (vector? lst) (.put (.cache ignite cache-name) key (conj lst item))
;          (seq? lst) (.put (.cache ignite cache-name) key (into [] (concat lst [item])))
;          (list? lst) (.put (.cache ignite cache-name) key (into [] (concat lst [item])))
;          (instance? List lst) (.put (.cache ignite cache-name) key (doto lst (.add item)))
;          ))
;
;; 添加 dic
;(defn no-sql-put [dic key value]
;    (if (map? dic)
;        (assoc dic key value)))
;
;; pop
;(defn no-sql-pop [lst]
;    [(peek lst) (pop lst)]
;    )









































