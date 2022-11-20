(ns org.gridgain.plus.dml.my-load-smart-sql
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (cn.plus.model.db MyScenesCache MyScenesCachePk ScenesType MyScenesParams MyScenesParamsPk)
             (com.google.common.base Strings)
             (org.gridgain.smart.ml MyTrianDataUtil)
             (java.util Hashtable))
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
    ([lst] (get-notes-single lst [] nil [] 0 false []))
    ([[f & r] stack mid-small stack-lst index is-note descript]
     (if (some? f)
         (cond (and (not (= f \})) (not (= f \;)) (nil? mid-small)) (cond (and (= f \-) (not (empty? r)) (= (first r) \-)) (recur nil (conj stack \- \-) mid-small stack-lst index true descript)
                                                                          (= f \') (recur r (conj stack f) "小" (conj stack-lst f) (+ index 1) is-note (conj descript f))
                                                                          (= f \") (recur r (conj stack f) "大" (conj stack-lst f) (+ index 1) is-note (conj descript f))
                                                                          :else
                                                                          (recur r (conj stack f) mid-small stack-lst (+ index 1) is-note (conj descript f))
                                                                          )
               (and (or (= f \}) (= f \;)) (nil? mid-small)) (recur nil stack mid-small stack-lst index is-note descript)
               (= mid-small "小") (if (= f \')
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) (+ index 1) is-note descript)
                                         (recur r (conj stack f) "小" (pop stack-lst) (+ index 1) is-note descript))
                                     (recur r (conj stack f) mid-small stack-lst (+ index 1) is-note descript))
               (= mid-small "大") (if (= f \")
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) (+ index 1) is-note descript)
                                         (recur r (conj stack f) "大" (pop stack-lst) (+ index 1) is-note descript))
                                     (recur r (conj stack f) mid-small stack-lst (+ index 1) is-note descript))
               )
         (if (true? is-note)
             [(str/join (reverse stack)) (+ index 2) (str/join (reverse descript))]))))

;(defn get-notes-multi
;    ([lst] (get-notes-multi lst [] nil [] [] nil []))
;    ([[f & r] stack mid-small stack-lst stack-flag flag descript]
;     (if (some? f)
;         (cond (and (not (= f \})) (not (= f \;)) (nil? mid-small)) (cond (and (= f \/) (not (empty? r)) (= (first r) \*)) (recur (rest r) (conj stack \/ \*) mid-small stack-lst (conj stack-flag "(") "start" descript)
;                                                                          (and (= f \*) (not (empty? r)) (= (first r) \/)) (if (= (count stack-flag) 1)
;                                                                                                                             (recur (rest r) (conj stack \* \/) mid-small stack-lst (pop stack-flag) "end" descript)
;                                                                                                                             (recur (rest r) (conj stack \* \/) mid-small stack-lst (pop stack-flag) flag descript))
;                                                                          (= f \') (recur r (conj stack f) "小" (conj stack-lst f) stack-flag flag (conj descript f))
;                                                                          (= f \") (recur r (conj stack f) "大" (conj stack-lst f) stack-flag flag (conj descript f))
;                                                                          :else
;                                                                          (recur r (conj stack f) mid-small stack-lst stack-flag flag (conj descript f)))
;               (and (or (= f \}) (= f \;)) (nil? mid-small)) (recur nil stack mid-small stack-lst stack-flag flag descript)
;               (= mid-small "小") (if (= f \')
;                                     (if (= (count stack-lst) 1)
;                                         (recur r (conj stack f) nil (pop stack-lst) stack-flag flag descript)
;                                         (recur r (conj stack f) "小" (pop stack-lst) stack-flag flag descript))
;                                     (recur r (conj stack f) mid-small stack-lst stack-flag flag descript))
;               (= mid-small "大") (if (= f \")
;                                     (if (= (count stack-lst) 1)
;                                         (recur r (conj stack f) nil (pop stack-lst) stack-flag flag descript)
;                                         (recur r (conj stack f) "大" (pop stack-lst) stack-flag flag descript))
;                                     (recur r (conj stack f) mid-small stack-lst stack-flag flag descript))
;               )
;         (if (= flag "end")
;             [(str/join (reverse stack)) (str/trim (str/join (reverse descript)))]))))

(defn get-notes
    ([lst] (get-notes lst [] []))
    ([lst rs rs-descript]
     (if-let [[note index descript] (get-notes-single lst)]
         (if (= note "")
             [(str/join rs) (str/join rs-descript)]
             (let [[my-notes my-descript] (get-notes (drop index lst))]
                 [(str/join (concat [my-notes] (conj rs note))) (str/join (concat [my-descript] (conj rs-descript descript)))])
             )
         [(str/join rs) (str/join rs-descript)])))

(defn get-notes-multi
    ([lst] (get-notes-multi lst [] nil [] [] nil []))
    ([[f & r] stack mid-small stack-lst stack-flag flag descript]
     (if (some? f)
         (cond (and (not (= f \})) (not (= f \;)) (nil? mid-small)) (cond (and (= f \/) (not (empty? r)) (= (first r) \*)) (recur (rest r) (conj stack \/ \*) mid-small stack-lst (conj stack-flag "(") "start" descript)
                                                                          (and (= f \*) (not (empty? r)) (= (first r) \/)) (if (= (count stack-flag) 1)
                                                                                                                               (recur (rest r) (conj stack \* \/) mid-small stack-lst (pop stack-flag) "end" descript)
                                                                                                                               (recur (rest r) (conj stack \* \/) mid-small stack-lst (pop stack-flag) flag descript))
                                                                          (= f \') (recur r (conj stack f) "小" (conj stack-lst f) stack-flag flag (conj descript f))
                                                                          (= f \") (recur r (conj stack f) "大" (conj stack-lst f) stack-flag flag (conj descript f))
                                                                          :else
                                                                          (recur r (conj stack f) mid-small stack-lst stack-flag flag (conj descript f)))
               (and (or (= f \}) (= f \;)) (nil? mid-small)) (recur nil stack mid-small stack-lst stack-flag flag descript)
               (= mid-small "小") (if (= f \')
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) stack-flag flag descript)
                                         (recur r (conj stack f) "小" (pop stack-lst) stack-flag flag descript))
                                     (recur r (conj stack f) mid-small stack-lst stack-flag flag descript))
               (= mid-small "大") (if (= f \")
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) stack-flag flag descript)
                                         (recur r (conj stack f) "大" (pop stack-lst) stack-flag flag descript))
                                     (recur r (conj stack f) mid-small stack-lst stack-flag flag descript))
               )
         (if (= flag "end")
             [(str/join (reverse stack)) (str/trim (str/join (reverse descript)))]))))

(defn is-multi?
    ([lst] (is-multi? lst nil))
    ([[f & r] flag]
     (if (some? f)
         (cond (= f \newline) (recur r flag)
               (= f \space) (recur r flag)
               (and (= f \/) (not (empty? r)) (= (first r) \*)) (recur nil "mutil")
               :else
               (recur r flag)
               )
         (if (= flag "mutil")
             true false))))

(defn get-func-code
    ([lst] (get-func-code lst [] nil [] [] nil))
    ([[f & r] stack mid-small stack-lst stack-k flag]
     (if (some? f)
         (cond (and (nil? flag) (nil? mid-small) (not (= f \{))) (recur r (conj stack f) mid-small stack-lst stack-k flag)
               (and (nil? flag) (nil? mid-small) (= f \{)) (recur r (conj stack f) mid-small stack-lst (conj stack-k f) "start")
               (and (= f \') (nil? mid-small)) (recur r (conj stack f) "小" (conj stack-lst f) stack-k flag)
               (and (= f \") (nil? mid-small)) (recur r (conj stack f) "大" (conj stack-lst f) stack-k flag)
               (= mid-small "小") (if (= f \')
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) stack-k flag)
                                         (recur r (conj stack f) "小" (pop stack-lst) stack-k flag))
                                     (recur r (conj stack f) mid-small stack-lst stack-k flag))
               (= mid-small "大") (if (= f \")
                                     (if (= (count stack-lst) 1)
                                         (recur r (conj stack f) nil (pop stack-lst) stack-k flag)
                                         (recur r (conj stack f) "大" (pop stack-lst) stack-k flag))
                                     (recur r (conj stack f) mid-small stack-lst stack-k flag))
               (= f \{) (recur r (conj stack f) mid-small stack-lst (conj stack-k f) flag)
               (= f \}) (if (= (count stack-k) 1)
                            (recur nil (conj stack f) mid-small stack-lst [] "end")
                            (recur r (conj stack f) mid-small stack-lst (pop stack-k) flag))
               :else
               (recur r (conj stack f) mid-small stack-lst stack-k flag)
               )
         (if (= flag "end")
             (str/join stack)))))

(defn get-forward-code [lst]
    (if (is-multi? lst)
        (get-notes-multi lst)
        (get-notes lst)))

(defn get-smart-func-code [func-name code]
    (let [my-matcher (re-matcher (re-pattern (str/join [#"(?i)\s*function\s+" func-name #"\s*"])) code)]
        (if (re-find my-matcher)
            (let [start (.start my-matcher) end (.end my-matcher)]
                (let [[my-notes my-descript] (get-forward-code (reverse (subs code 0 (+ start 1)))) func-code (get-func-code (subs code end))]
                    [(str/join [my-notes (subs code start end) func-code]) my-descript])
                ))))

; 添加 scenes
(defn save-scenes-to-cache [^Ignite ignite group_id ^String func-name ^String sql_code ^String smart_code ^String descrip]
    (let [pk (MyScenesCachePk. (first group_id) func-name) cache (.cache ignite "my_scenes")]
        (if-not (.containsKey cache pk)
            (.put cache pk (MyScenesCache. (first group_id) func-name sql_code smart_code descrip))
            (.replace cache pk (MyScenesCache. (first group_id) func-name sql_code smart_code descrip)))))

; 删除 scenes
(defn delete-scenes-to-cache [^Ignite ignite group_id ^String func-name-0]
    (if (= (first group_id) 0)
        (let [func-name (str/lower-case func-name-0)]
            (let [pk (MyScenesCachePk. (first group_id) (str/lower-case func-name)) cache (.cache ignite "my_scenes")]
                (if-not (.containsKey cache pk)
                    (do
                        (try
                            (eval (read-string func-name))
                            (eval (read-string (format "(def %s nil)" func-name)))
                            (catch Exception e
                                ))
                        (.remove cache pk)))))
        (throw (Exception. "只有 root 用户才有权限删除！"))))

(defn load-smart-sql [^Ignite ignite group_id ^String code]
    (loop [[f & r] (my-smart-sql/re-smart-segment (my-smart-sql/get-smart-segment (my-lexical/to-back code)))]
        (if (some? f)
            (do
                (cond (and (string? (first f)) (my-lexical/is-eq? (first f) "function")) (let [[sql func-name] (my-smart-clj/my-smart-to-clj-lower ignite group_id f)]
                                                                                             (eval (read-string sql))
                                                                                             (let [[func-code my-descript] (get-smart-func-code func-name code)]
                                                                                                 (save-scenes-to-cache ignite group_id func-name sql func-code my-descript)))
                      (and (string? (first f)) (contains? #{"insert" "update" "delete" "select"} (str/lower-case (first f)))) (my-smart-db-line/query_sql ignite group_id f)
                      :else
                      (if (string? (first f))
                          (my-smart-clj/smart-lst-to-clj ignite group_id f)
                          (my-smart-clj/smart-lst-to-clj ignite group_id (apply concat f)))
                      )
                (recur r)))))

(defn csv-update-to-db [^Ignite ignite group_id schema_name table_name vs]
    (if (or (nil? schema_name) (Strings/isNullOrEmpty schema_name))
        (if (my-lexical/is-eq? (second group_id) "my_meta")
            (throw (Exception. "MY_META 下面不能存在机器学习的训练数据！"))
            (MyTrianDataUtil/loadTrainMatrix ignite (second group_id) table_name vs))
        (cond (and (my-lexical/is-eq? (second group_id) schema_name) (my-lexical/is-eq? (second group_id) "my_meta")) (throw (Exception. "MY_META 下面不能存在机器学习的训练数据！"))
              (and (not (my-lexical/is-eq? (second group_id) schema_name)) (my-lexical/is-eq? schema_name "public")) (MyTrianDataUtil/loadTrainMatrix ignite "public" table_name vs)
              (and (my-lexical/is-eq? (second group_id) schema_name) (not (my-lexical/is-eq? (second group_id) "my_meta"))) (MyTrianDataUtil/loadTrainMatrix ignite schema_name table_name vs)
              ))
    )

(defn load-csv [^Ignite ignite group_id ^Hashtable ht]
    (if (some? ht)
        (let [table_name (get ht "table_name") schema_name (get ht "schema_name") code (get ht "csv_code")]
            (if-not (Strings/isNullOrEmpty code)
                (let [lst (str/split code #"\s*\n\s*")]
                    (loop [[f & r] lst]
                        (if (some? f)
                            (let [vs (str/split f #"\s*,\s*")]
                                (csv-update-to-db ignite group_id schema_name table_name vs)
                                (recur r)))))))))

(defn -loadSmartSql [this ^Ignite ignite ^Object group_id ^String code]
    (load-smart-sql ignite group_id code))

(defn -loadCsv [this ^Ignite ignite ^Object group_id ^Hashtable ht]
    (load-csv ignite group_id ht))










































