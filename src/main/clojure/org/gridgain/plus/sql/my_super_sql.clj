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
        [org.gridgain.plus.tools.my-user-group :as my-user-group]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
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
             (java.util ArrayList List Hashtable Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        :implements [org.gridgain.superservice.IMySmartSql]
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

(defn my_plus_sql [^Ignite ignite group_id lst-sql]
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


(defn ht-to-line [^Hashtable ht]
    (format "'%s', %s" (str/join ["sm_ml_" (get ht "schema_name") "_" (get ht "table_name")]) (get ht "item_size")))

(defn ht-to-ps [ignite group_id ^Hashtable ht]
    (let [ds-name (second group_id)]
        (cond (and (my-lexical/is-eq? ds-name "MY_META") (not (contains? ht "schema_name"))) (throw (Exception. "MY_META 下面不能创建机器学习的训练数据！"))
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (my-lexical/is-eq? (get ht "schema_name") "MY_META")) (throw (Exception. "MY_META 下面不能创建机器学习的训练数据！"))
              (and (my-lexical/is-eq? ds-name "MY_META") (contains? ht "schema_name") (not (my-lexical/is-eq? (get ht "schema_name") "MY_META"))) (ht-to-line ht)
              (not (contains? ht "schema_name")) (ht-to-line (doto ht (.put "schema_name" (str/lower-case ds-name))))
              (contains? ht "schema_name") (cond (my-lexical/is-eq? (get ht "schema_name") "public") (ht-to-line ht)
                                                 (my-lexical/is-eq? (get ht "schema_name") ds-name) (ht-to-line ht)
                                                 :else (throw (Exception. "不能在其它非公共数据集下面不能创建机器学习的训练数据！")))
              :else
              (throw (Exception. "不能创建机器学习的训练数据！"))
              )
        ))

(defn call-show-train-data [ignite group_id lst]
    (let [{func-name :func-name lst_ps :lst_ps} (my-select/sql-to-ast lst)]
        (if (= (count lst_ps) 1)
            (let [ht-line (my-smart-token-clj/token-to-clj ignite group_id (first lst_ps) nil)]
                (ht-to-ps ignite group_id (eval (read-string ht-line)))
                ))))

(defn to-sql [lst]
    (loop [[f & r] lst rs []]
        (if (some? f)
            (if (string? f)
                (recur r (concat rs [f]))
                (recur r (concat rs (to-sql f))))
            rs)))

; smart-code-lst = (my-lexical/to-back sql)
(defn my-smart-sql [^Ignite ignite group_id ^clojure.lang.LazySeq smart-code-lst]
    (if-let [func-ast (my-smart-clj/is-jdbc-preparedStatement smart-code-lst)]
        {:sql (str/join (to-sql (my-select/ast_to_sql ignite group_id func-ast)))}
        (my-smart-clj/smart-lst-to-clj ignite group_id smart-code-lst)))

(defn super-sql-lst
    ([^Ignite ignite ^Long group_id ^String userToken ^String schema_name ^String group_type lst] (super-sql-lst ignite [group_id schema_name group_type] lst []))
    ([^Ignite ignite group_id [lst & r] lst-rs]
     (if (some? lst)
         (if-not (nil? (first lst))
             (cond (and (string? (first lst)) (my-lexical/is-eq? (first lst) "insert")) (recur ignite group_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "update")) (recur ignite group_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "delete")) (recur ignite group_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "select")) (recur ignite group_id r (conj lst-rs (my-smart-db-line/query_sql ignite group_id (cull-semicolon lst))))
                   ; create dataset
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "schema")) (let [rs (my-create-dataset/create_data_set ignite group_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                       (if (nil? rs)
                                                                                                                                           (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                           (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                           ))
                   ; drop dataset
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "schema")) (let [rs (my-drop-dataset/drop-data-set-lst ignite group_id (cull-semicolon lst))]
                                                                                                                                     (if (nil? rs)
                                                                                                                                         (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                         (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))))
                   ; create table
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-create-table/my_create_table_lst ignite group_id (cull-semicolon lst))]
                                                                                                                                     (if (nil? rs)
                                                                                                                                         (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                         (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                         ))
                   ; alter table
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-alter-table/alter_table ignite group_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                    (if (nil? rs)
                                                                                                                                        (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                        (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                        ))
                   ; drop table
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-drop-table/drop_table ignite group_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                   (if (nil? rs)
                                                                                                                                       (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                       (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                       ))
                   ; create index
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-create-index/create_index ignite group_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                     (if (nil? rs)
                                                                                                                                         (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                         (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                         ))
                   ; drop index
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-drop-index/drop_index ignite group_id (str/join " " (cull-semicolon lst)))]
                                                                                                                                   (if (nil? rs)
                                                                                                                                       (recur ignite group_id r (conj lst-rs "select show_msg('true') as tip;"))
                                                                                                                                       (recur ignite group_id r (conj lst-rs "select show_msg('false') as tip;"))
                                                                                                                                       ))
                   ; no sql
                   ;(contains? #{"no_sql_create" "no_sql_insert" "no_sql_update" "no_sql_delete" "no_sql_query" "no_sql_drop" "push" "pop"} (str/lower-case (first lst))) (.append sb (str (my-super-cache/my-no-lst ignite group_id lst (str/join " " lst)) ";"))
                   (and (string? (first lst)) (contains? #{"noSqlInsert" "noSqlUpdate" "noSqlDelete" "noSqlDrop"} (str/lower-case (first lst)))) (let [my-code (my-smart-clj/token-to-clj ignite group_id (my-select/sql-to-ast (cull-semicolon lst)) nil)]
                                                                                                                                                     (recur ignite group_id r (conj lst-rs (format "select show_msg('%s') as tip;" (str (eval (read-string my-code))))))
                                                                                                                                                     )
                   (and (string? (first lst)) (my-lexical/is-eq? (first lst) "show_train_data")) (if-let [show-sql (call-show-train-data ignite group_id (cull-semicolon lst))]
                                                                                                     (recur ignite group_id r (conj lst-rs (format "select show_train_data(%s) as tip;" show-sql))))
                   :else
                   (if (string? (first lst))
                       (let [smart-sql-obj (my-smart-sql ignite group_id lst)]
                           (if (map? smart-sql-obj)
                               (recur ignite group_id r (conj lst-rs (format "select %s;" (-> smart-sql-obj :sql))))
                               (recur ignite group_id r (conj lst-rs (format "select show_msg('%s') as tip;" smart-sql-obj)))))
                       (let [smart-sql-obj (my-smart-sql ignite group_id (apply concat lst))]
                           (if (map? smart-sql-obj)
                               (recur ignite group_id r (conj lst-rs (format "select %s;" (-> smart-sql-obj :sql))))
                               (recur ignite group_id r (conj lst-rs (format "select show_msg('%s') as tip;" smart-sql-obj)))))
                       )
                   ;(throw (Exception. "输入字符有错误！不能解析，请确认输入正确！"))
                   ))
         (if-not (empty? lst-rs)
             (last lst-rs)))))

(defn super-sql [^Ignite ignite ^String userToken ^List lst]
    (let [[group_id schema_name group_type] (my-user-group/get_user_group ignite userToken)]
        (if-not (nil? group_id)
            (super-sql-lst ignite group_id userToken schema_name group_type lst)
            (throw (Exception. (format "userToken: %s 不存在！" userToken))))))

(defn super-sql-line [^Ignite ignite ^String userToken ^String line]
    (let [[group_id schema_name group_type] (my-user-group/get_user_group ignite userToken)]
        (if-not (nil? group_id)
            (super-sql-lst ignite group_id userToken schema_name group_type (my-smart-sql/re-super-smart-segment (my-smart-sql/get-my-smart-segment line)))
            (throw (Exception. (format "userToken: %s 不存在！" userToken))))))

(defn -recovery_ddl [this ^Ignite ignite ^String line]
    (let [userToken (.getRoot_token (.configuration ignite))]
        (super-sql-line ignite userToken line)))

; 传入 [["select" "name" ...], ["update" ...], ["insert" ...]]
(defn -superSql [^Ignite ignite ^Object userToken ^Object lst-sql]
    (if (some? userToken)
        (if-let [m-obj (MyCacheExUtil/restore lst-sql)]
            (cond (string? m-obj) (super-sql-line ignite (MyCacheExUtil/restoreToLine userToken) m-obj)
                  ;(my-lexical/is-seq? m-obj) (super-sql ignite (MyCacheExUtil/restoreToLine userToken) (my-smart-sql/re-super-smart-segment m-obj))
                  (my-lexical/is-seq? m-obj) (super-sql ignite (MyCacheExUtil/restoreToLine userToken) (my-smart-sql/re-super-smart-segment m-obj))
                  ))
        (throw (Exception. "没有权限不能访问数据库！"))))

(defn -getGroupId [^Ignite ignite ^String userToken]
    (if-let [group_id (my-user-group/get_user_group ignite userToken)]
        true
        false))











































































