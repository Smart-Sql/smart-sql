(ns org.gridgain.plus.sql.my-super-sql
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [org.gridgain.plus.nosql.my-super-cache :as my-super-cache]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             (cn.log MyLogger)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.sql.MySuperSql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [superSql [org.apache.ignite.Ignite Object Object] String]
                  ^:static [getGroupId [org.apache.ignite.Ignite String] Boolean]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn cull-semicolon [lst]
    (if (= (last lst) ";")
        (cull-semicolon (drop-last lst))
        lst))

(defn get-lst-sql
    ([^String sql] (get-lst-sql sql [] [] []))
    ([[f & r] stack stack-lst lst]
     (if (some? f)
         (cond (and (= f \-) (= (first r) \-) (empty? stack)) (recur (rest r) (conj stack "单注释") (concat stack-lst [\- \-]) lst)
               (and (= f \newline) (not (empty? stack))) (let [m (peek stack)]
                                                             (if (= m "单注释")
                                                                 (recur r (pop stack) (concat stack-lst [f]) lst)
                                                                 (recur r stack (concat stack-lst [f]) lst)))
               (and (= f \/) (= (first r) \*) (empty? stack)) (recur (rest r) (conj stack "双注释") (concat stack-lst [\/ \*]) lst)
               (and (= f \*) (= (first r) \\) (not (empty? stack))) (let [m (peek stack)]
                                                                        (if (= m "双注释")
                                                                            (recur (rest r) (pop stack) (concat stack-lst [f]) lst)
                                                                            (recur (rest r) stack (concat stack-lst [f]) lst)))
               (and (= f \") (empty? stack)) (recur r (conj stack "双字符") (concat stack-lst [f]) lst)
               (and (= f \") (not (empty? stack))) (let [m (peek stack)]
                                                       (if (= m "双字符")
                                                           (recur r (pop stack) (concat stack-lst [f]) lst)
                                                           (recur r stack (concat stack-lst [f]) lst)))
               (and (= f \') (empty? stack)) (recur r (conj stack "单字符") (concat stack-lst [f]) lst)
               (and (= f \') (not (empty? stack))) (let [m (peek stack)]
                                                       (if (= m "单字符")
                                                           (recur r (pop stack) (concat stack-lst [f]) lst)
                                                           (recur r stack (concat stack-lst [f]) lst)))
               (and (= f \;) (empty? stack) (not (empty? stack-lst))) (recur r [] [] (conj lst (str/join stack-lst)))
               :else
               (recur r stack (concat stack-lst [f]) lst)
               )
         (if (empty? stack-lst)
             lst
             (conj lst (str/join stack-lst))))))

; 通过 userToken 获取 group_id
(defn get_group_id [^Ignite ignite ^String userToken]
    (if (my-lexical/is-eq? userToken (.getRoot_token (.configuration ignite)))
        [0 "MY_META" "ALL" -1]
        (when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select g.id, m.dataset_name, g.group_type, m.id from my_users_group as g, my_dataset as m where g.data_set_id = m.id and g.user_token = ?") (to-array [userToken])))))]
            m))
    )

(def my_group_id (memoize get_group_id))

; 是否 select 语句
(defn has-from? [[f & r]]
    (if (some? f)
        (if (my-lexical/is-eq? f "from")
            true
            (recur r))
        false))

(defn is-scenes? [lst]
    (if (and (my-lexical/is-eq? (first lst) "scenes") (= (second lst) "(") (= (last lst) ")"))
        true
        false))

(defn get-scenes [lst]
    (loop [index 2 my-count (count lst) rs []]
        (if (< (+ index 1) my-count)
            (recur (+ index 1) my-count (conj rs (nth lst index)))
            (my-lexical/get_str_value (str/join " " rs)))))

(defn my_plus_sql [^Ignite ignite ^Long group_id lst-sql]
    (if-let [ast (my-select/sql-to-ast lst-sql)]
        (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql)
        (throw (Exception. (format "查询字符串 %s 错误！" (str/join " " lst-sql))))))

; 执行 smart sql
;(defn my-smart-sql [^Ignite ignite ^Long group_id ^String userToken ^clojure.lang.LazySeq smart-code-lst]
;    (let [])
;    (cond (and (my-lexical/is-seq? smart-code-lst) (my-lexical/is-seq? (first smart-code-lst)) (= (count smart-code-lst) 1)) (let [sql (my-smart-clj/express-to-clj ignite group_id smart-code-lst nil)]
;                                                                                                                                 (eval (read-string sql)))
;          (and (my-lexical/is-seq? smart-code-lst) (my-lexical/is-seq? (first smart-code-lst)) (> (count smart-code-lst) 1)) (let [sql (my-smart-clj/smart-lst-to-clj ignite group_id smart-code-lst)]
;                                                                                                                                 (eval (read-string sql)))
;          (and (my-lexical/is-seq? smart-code-lst) (string? (first smart-code-lst))) (let [sql (my-smart-clj/express-to-clj ignite group_id [smart-code-lst] nil)]
;                                                                                                                                 (eval (read-string sql)))
;          ))

; smart-code-lst = (my-lexical/to-back sql)
(defn my-smart-sql [^Ignite ignite ^Long group_id ^clojure.lang.LazySeq smart-code-lst]
    (my-smart-clj/smart-lst-to-clj ignite group_id smart-code-lst))

(defn super-sql-lst-0 [^Ignite ignite ^Long group_id ^String userToken ^String dataset_name ^String group_type ^Long dataset_id [lst & r] ^StringBuilder sb]
    (if (some? lst)
        (do
            (if-not (nil? (first lst))
                (cond (and (string? (first lst)) (contains? #{"insert" "update" "delete" "select"} (str/lower-case (first lst)))) (my-smart-db-line/query_sql ignite group_id lst)
                      ;(my-lexical/is-eq? (first lst) "insert") (let [rs (my-insert/insert_run ignite group_id lst sql)]
                      ;                                             (if-not (nil? rs)
                      ;                                                 (.append sb (format "select show_msg('%s') as tip;" (first (first rs))))
                      ;                                                 (.append sb "select show_msg('true') as tip;")))
                      ;(my-lexical/is-eq? (first lst) "update") (let [rs (my-update/update_run ignite group_id lst sql)]
                      ;                                             (if-not (nil? rs)
                      ;                                                 (.append sb (format "select show_msg('%s') as tip;" (first (first rs))))
                      ;                                                 (.append sb "select show_msg('true') as tip;")))
                      ;(my-lexical/is-eq? (first lst) "delete") (let [rs (my-delete/delete_run ignite group_id lst sql)]
                      ;                                             (if-not (nil? rs)
                      ;                                                 (.append sb (format "select show_msg('%s') as tip;" (first (first rs))))
                      ;                                                 (.append sb "select show_msg('true') as tip;")))
                      ;(my-lexical/is-eq? (first lst) "select") (if (has-from? (rest lst))
                      ;                                             (.append sb (str (my_plus_sql ignite group_id lst) ";"))
                      ;                                             (.append sb (str sql ";")))

                      ; ddl
                      ; create dataset
                      (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-create-dataset/create_data_set ignite group_id (str/join " " lst))]
                                                                                                                                          (if (nil? rs)
                                                                                                                                              (.append sb "select show_msg('true') as tip;")
                                                                                                                                              (.append sb "select show_msg('false') as tip;")))
                      ; alert dataset
                      ;(and (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-alter-dataset/alter_data_set ignite group_id sql)]
                      ;                                                                                             (if (nil? rs)
                      ;                                                                                                 "select show_msg('true') as tip"
                      ;                                                                                                 "select show_msg('false') as tip"))
                      ; drop dataset
                      ;(and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-drop-dataset/drop_data_set ignite group_id sql)]
                      ;                                                                                            (if (nil? rs)
                      ;                                                                                                "select show_msg('true') as tip"
                      ;                                                                                                "select show_msg('false') as tip"))
                      ; create table
                      (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-create-table/create-table ignite group_id dataset_name group_type dataset_id (str/join " " lst))]
                                                                                                                                        (if (nil? rs)
                                                                                                                                            (.append sb "select show_msg('true') as tip;")
                                                                                                                                            (.append sb "select show_msg('false') as tip;")))
                      ; alter table
                      (and (string? (first lst)) (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-alter-table/alter_table ignite group_id dataset_name group_type dataset_id (str/join " " lst))]
                                                                                                                                       (if (nil? rs)
                                                                                                                                           (.append sb "select show_msg('true') as tip;")
                                                                                                                                           (.append sb "select show_msg('false') as tip;")))
                      ; drop table
                      (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-drop-table/drop_table ignite group_id dataset_name group_type dataset_id (str/join " " lst))]
                                                                                                                                      (if (nil? rs)
                                                                                                                                          (.append sb "select show_msg('true') as tip;")
                                                                                                                                          (.append sb "select show_msg('false') as tip;")))
                      ; create index
                      (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-create-index/create_index ignite group_id dataset_name group_type dataset_id (str/join " " lst))]
                                                                                                                                        (if (nil? rs)
                                                                                                                                            (.append sb "select show_msg('true') as tip;")
                                                                                                                                            (.append sb "select show_msg('false') as tip;")))
                      ; drop index
                      (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-drop-index/drop_index ignite group_id dataset_name group_type dataset_id (str/join " " lst))]
                                                                                                                                      (if (nil? rs)
                                                                                                                                          (.append sb "select show_msg('true') as tip;")
                                                                                                                                          (.append sb "select show_msg('false') as tip;")))
                      ; update dataset
                      ;(and (my-lexical/is-eq? (first lst) "update") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-update-dataset/update_dataset ignite group_id sql)]
                      ;                                                                                              (if (nil? rs)
                      ;                                                                                                  "select show_msg('true') as tip"
                      ;                                                                                                  "select show_msg('false') as tip"))
                      ; no sql
                      ;(contains? #{"no_sql_create" "no_sql_insert" "no_sql_update" "no_sql_delete" "no_sql_query" "no_sql_drop" "push" "pop"} (str/lower-case (first lst))) (.append sb (str (my-super-cache/my-no-lst ignite group_id lst (str/join " " lst)) ";"))
                      (and (string? (first lst)) (contains? #{"noSqlInsert" "noSqlUpdate" "noSqlDelete" "noSqlDrop"} (str/lower-case (first lst)))) (let [my-code (my-smart-clj/token-to-clj ignite group_id (my-select/sql-to-ast lst) nil)]
                                                                                                                                                        (.append sb (str (eval (read-string my-code)))))
                      :else
                      (if (string? (first lst))
                          (my-smart-sql ignite group_id [lst])
                          (my-smart-sql ignite group_id (apply concat lst)))
                      ;(throw (Exception. "输入字符有错误！不能解析，请确认输入正确！"))
                      ))
            (recur ignite group_id userToken dataset_name group_type dataset_id r sb))
        (.toString sb)))

(defn super-sql-lst
    ([^Ignite ignite ^Long group_id ^String userToken ^String dataset_name ^String group_type ^Long dataset_id lst] (super-sql-lst ignite group_id userToken dataset_name group_type dataset_id lst []))
    ([^Ignite ignite ^Long group_id ^String userToken ^String dataset_name ^String group_type ^Long dataset_id [lst & r] lst-rs]
     (if (some? lst)
         (if-not (nil? (first lst))
             (cond (and (string? (first lst)) (my-lexical/is-eq? (first lst) "insert")) (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "update")) (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "delete")) (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "select")) (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   ; create dataset
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-create-dataset/create_data_set ignite group_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                       (if (nil? rs)
                                                                                                                                           (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                           (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                           ))
                   ; drop dataset
                   (and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-drop-dataset/drop-data-set-lst ignite group_id (cull-semicolon lst))]
                                                                                                               (if (nil? rs)
                                                                                                                   "select show_msg('true') as tip"
                                                                                                                   "select show_msg('false') as tip"))
                   ; create table
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-create-table/my_create_table_lst ignite group_id dataset_name group_type dataset_id "" (cull-semicolon lst))]
                                                                                                                                     (if (nil? rs)
                                                                                                                                         (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                         (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                         ))
                   ; alter table
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-alter-table/alter_table ignite group_id dataset_name group_type dataset_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                    (if (nil? rs)
                                                                                                                                        (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                        (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                        ))
                   ; drop table
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-drop-table/drop_table ignite group_id dataset_name group_type dataset_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                   (if (nil? rs)
                                                                                                                                       (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                       (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                       ))
                   ; create index
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-create-index/create_index ignite group_id dataset_name group_type dataset_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                     (if (nil? rs)
                                                                                                                                         (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                         (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                         ))
                   ; drop index
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-drop-index/drop_index ignite group_id dataset_name group_type dataset_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                   (if (nil? rs)
                                                                                                                                       (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                       (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                       ))
                   ; no sql
                   ;(contains? #{"no_sql_create" "no_sql_insert" "no_sql_update" "no_sql_delete" "no_sql_query" "no_sql_drop" "push" "pop"} (str/lower-case (first lst))) (.append sb (str (my-super-cache/my-no-lst ignite group_id lst (str/join " " lst)) ";"))
                   (and (string? (first lst)) (contains? #{"noSqlInsert" "noSqlUpdate" "noSqlDelete" "noSqlDrop"} (str/lower-case (first lst)))) (let [my-code (my-smart-clj/token-to-clj ignite group_id (my-select/sql-to-ast (cull-semicolon lst)) nil)]
                                                                                                                                                     (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (format "select show_msg('%s') as tip;" (str (eval (read-string my-code))))))
                                                                                                                                                     )
                   :else
                   (if (string? (first lst))
                       (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (format "select show_msg('%s') as tip;" (my-smart-sql ignite group_id [(cull-semicolon lst)]))))
                       (recur ignite group_id userToken dataset_name group_type dataset_id r (conj lst-rs (format "select show_msg('%s') as tip;" (my-smart-sql ignite group_id (apply concat lst)))))
                       )
                   ;(throw (Exception. "输入字符有错误！不能解析，请确认输入正确！"))
                   ))
         (if-not (empty? lst-rs)
             (last lst-rs)))))

(defn super-sql [^Ignite ignite ^String userToken ^List lst]
    (let [[group_id dataset_name group_type dataset_id] (my_group_id ignite userToken)]
        ;(.myWriter (MyLogger/getInstance) (format "%s %s" sql group_id))
        (super-sql-lst ignite group_id userToken dataset_name group_type dataset_id lst)))

(defn super-sql-line [^Ignite ignite ^String userToken ^String line]
    (let [[group_id dataset_name group_type dataset_id] (my_group_id ignite userToken)]
        ;(.myWriter (MyLogger/getInstance) (format "%s %s" sql group_id))
        (super-sql-lst ignite group_id userToken dataset_name group_type dataset_id (my-smart-sql/get-my-smart-segment line))))

; 传入 [["select" "name" ...], ["update" ...], ["insert" ...]]
(defn -superSql [^Ignite ignite ^Object userToken ^Object lst-sql]
    (if (some? userToken)
        (if-let [m-obj (MyCacheExUtil/restore lst-sql)]
            (cond (string? m-obj) (super-sql-line ignite (MyCacheExUtil/restoreToLine userToken) m-obj)
                  (my-lexical/is-seq? m-obj) (super-sql ignite (MyCacheExUtil/restoreToLine userToken) (my-smart-sql/re-super-smart-segment m-obj))
                  ))
        (throw (Exception. "没有权限不能访问数据库！"))))

(defn -getGroupId [^Ignite ignite ^String userToken]
    (if-let [group_id (my_group_id ignite userToken)]
        true
        false))











































































