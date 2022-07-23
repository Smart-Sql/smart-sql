(ns org.gridgain.plus.dml.my-load-smart-sql
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (cn.plus.model.db MyScenesCache MyScenesCachePk ScenesType MyScenesParams MyScenesParamsPk)
             )
    (:gen-class
        :implements [org.gridgain.superservice.ILoadSmartSql]
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyLoadSmartSql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [superSql [org.apache.ignite.Ignite Object Object] String]
        ;          ^:static [getGroupId [org.apache.ignite.Ignite String] Boolean]]
        ))

(defn get-notes-single
    ([lst] (get-notes-single lst [] nil [] 0 false))
    ([[f & r] stack mid-small stack-lst index is-note]
     (if (some? f)
         (cond (and (not (= f \})) (not (= f \;)) (nil? mid-small)) (cond (not (= f \-)) (recur r (conj stack f) mid-small stack-lst (+ index 1) is-note)
                                                                          (and (= f \-) (not (empty? r)) (= (first r) \-)) (recur nil stack mid-small stack-lst index true)
                                                                          (= f \') (recur r (conj stack f) "小" (conj stack-lst f) (+ index 1) is-note)
                                                                          (= f \") (recur r (conj stack f) "大" (conj stack-lst f) (+ index 1) is-note)
                                                                          )
               (and (or (= f \}) (= f \;)) (nil? mid-small)) (recur nil stack mid-small stack-lst index is-note)
               (= mid-small "小") (if (= f \')
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) (+ index 1) is-note)
                                         (recur r (conj stack f) "小" (pop stack-lst) (+ index 1) is-note))
                                     (recur r (conj stack f) mid-small stack-lst (+ index 1) is-note))
               (= mid-small "大") (if (= f \")
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) (+ index 1) is-note)
                                         (recur r (conj stack f) "大" (pop stack-lst) (+ index 1) is-note))
                                     (recur r (conj stack f) mid-small stack-lst (+ index 1) is-note))
               )
         (if (true? is-note)
             [(str/join stack) (+ index 2)]))))

(defn get-notes
    ([lst] (get-notes lst []))
    ([lst rs]
     (if-let [[note index] (get-notes-single lst)]
         (if (= note "")
             (str/join rs)
             (str/join (get-notes (drop index lst) (concat [(str/join (reverse note))] rs))))
         (str/join rs))))

(defn get-forward-code [func-name code]
    (let [my-matcher (re-matcher (re-pattern (str/join [#"(?i)\s*function\s+" func-name #"\s*"])) code)]
        (if (re-find my-matcher)
            (let [start (.start my-matcher) end (.end my-matcher)]
                (get-notes (reverse (subs code 0 start)))
                ;(println end)
                ;(subs code start end)
                )))
    )

;(get-forward-code "get_data_set_id" code)

(defn save-to-cache [^Ignite ignite ^Long group_id ^String func-name ^String sql_code ^String smart_code]
    (let [pk (MyScenesCachePk. group_id func-name) cache (.cache ignite "my_scenes")]
        (if-not (.containsKey cache pk)
            (.put cache pk (MyScenesCache. group_id func-name sql_code smart_code)))))

(defn load-smart-sql [^Ignite ignite ^Long group_id ^String code]
    (loop [[f & r] (my-smart-sql/re-smart-segment (my-smart-sql/get-smart-segment (my-lexical/to-back code)))]
        (if (some? f)
            (do
                (cond (and (string? (first f)) (my-lexical/is-eq? (first f) "function")) (let [[sql func-name] (my-smart-clj/my-smart-to-clj-lower ignite group_id f)]
                                                                                             (eval (read-string sql))
                                                                                             (save-to-cache ignite group_id func-name sql (first f)))
                      (and (string? (first f)) (contains? #{"insert" "update" "delete" "select"} (str/lower-case (first f)))) (my-smart-db-line/query_sql ignite group_id f)
                      :else
                      (if (string? (first f))
                          (my-smart-clj/smart-lst-to-clj ignite group_id f)
                          (my-smart-clj/smart-lst-to-clj ignite group_id (apply concat f)))
                      )
                (recur r)))))


(defn -loadSmartSql [this ^Ignite ignite ^Long group_id ^String code]
    (load-smart-sql ignite group_id code))










































