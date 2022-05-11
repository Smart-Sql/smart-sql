(ns org.gridgain.plus.sql.my-super-sql
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.ddl.my-update-dataset :as my-update-dataset]
        [org.gridgain.plus.nosql.my-super-cache :as my-super-cache]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache)
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
    ;(if (= userToken ))
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
    (if-let [ast (my-select/get_my_ast ignite group_id lst-sql)]
        (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql)
        (throw (Exception. (format "查询字符串 %s 错误！" (str/join " " lst-sql))))))

(defn super-sql-lst [^Ignite ignite ^Long group_id ^String dataset_name ^String group_type ^Long dataset_id [sql & r] ^StringBuilder sb]
    (if (some? sql)
        (do
            (let [lst (my-lexical/to-back sql)]
                ;(.myWriter (MyLogger/getInstance) (format "%s %s" sql group_id))
                (if-not (nil? (first lst))
                    (cond (my-lexical/is-eq? (first lst) "insert") (let [rs (my-insert/insert_run ignite group_id lst sql)]
                                                                       (if-not (nil? rs)
                                                                           (.append sb (format "select show_msg('%s') as tip;" (first (first rs))))
                                                                           (.append sb "select show_msg('true') as tip;")))
                          (my-lexical/is-eq? (first lst) "update") (let [rs (my-update/update_run ignite group_id lst sql)]
                                                                       (if-not (nil? rs)
                                                                           (.append sb (format "select show_msg('%s') as tip;" (first (first rs))))
                                                                           (.append sb "select show_msg('true') as tip;")))
                          (my-lexical/is-eq? (first lst) "delete") (let [rs (my-delete/delete_run ignite group_id lst sql)]
                                                                       (if-not (nil? rs)
                                                                           (.append sb (format "select show_msg('%s') as tip;" (first (first rs))))
                                                                           (.append sb "select show_msg('true') as tip;")))
                          (my-lexical/is-eq? (first lst) "select") (if (has-from? (rest lst))
                                                                       (.append sb (str (my_plus_sql ignite group_id lst) ";"))
                                                                       (.append sb (str sql ";")))
                          ; ddl
                          ; create dataset
                          (and (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-create-dataset/create_data_set ignite group_id sql)]
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
                          (and (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-create-table/create-table ignite group_id dataset_name group_type dataset_id sql)]
                                                                                                                      (if (nil? rs)
                                                                                                                          (.append sb "select show_msg('true') as tip;")
                                                                                                                          (.append sb "select show_msg('false') as tip;")))
                          ; alter table
                          (and (my-lexical/is-eq? (first lst) "ALTER") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-alter-table/alter_table ignite group_id dataset_name group_type dataset_id sql)]
                                                                                                                     (if (nil? rs)
                                                                                                                         (.append sb "select show_msg('true') as tip;")
                                                                                                                         (.append sb "select show_msg('false') as tip;")))
                          ; drop table
                          (and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "table")) (let [rs (my-drop-table/drop_table ignite group_id dataset_name group_type dataset_id sql)]
                                                                                                                    (if (nil? rs)
                                                                                                                        (.append sb "select show_msg('true') as tip;")
                                                                                                                        (.append sb "select show_msg('false') as tip;")))
                          ; create index
                          (and (my-lexical/is-eq? (first lst) "create") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-create-index/create_index ignite group_id dataset_name group_type dataset_id sql)]
                                                                                                                      (if (nil? rs)
                                                                                                                          (.append sb "select show_msg('true') as tip;")
                                                                                                                          (.append sb "select show_msg('false') as tip;")))
                          ; drop index
                          (and (my-lexical/is-eq? (first lst) "DROP") (my-lexical/is-eq? (second lst) "INDEX")) (let [rs (my-drop-index/drop_index ignite group_id dataset_name group_type dataset_id sql)]
                                                                                                                    (if (nil? rs)
                                                                                                                        (.append sb "select show_msg('true') as tip;")
                                                                                                                        (.append sb "select show_msg('false') as tip;")))
                          ; update dataset
                          ;(and (my-lexical/is-eq? (first lst) "update") (my-lexical/is-eq? (second lst) "dataset")) (let [rs (my-update-dataset/update_dataset ignite group_id sql)]
                          ;                                                                                              (if (nil? rs)
                          ;                                                                                                  "select show_msg('true') as tip"
                          ;                                                                                                  "select show_msg('false') as tip"))
                          ; no sql
                          (contains? #{"no_sql_create" "no_sql_insert" "no_sql_update" "no_sql_delete" "no_sql_query" "no_sql_drop" "push" "pop"} (str/lower-case (first lst))) (.append sb (str (my-super-cache/my-no-lst ignite group_id lst sql) ";"))
                          :else
                          (throw (Exception. "输入字符有错误！不能解析，请确认输入正确！"))
                          )))
            (recur ignite group_id dataset_name group_type dataset_id r sb))
        (.toString sb)))

(defn super-sql [^Ignite ignite ^String userToken ^String sql]
    (if-not (Strings/isNullOrEmpty sql)
        (let [lst (get-lst-sql sql) [group_id dataset_name group_type dataset_id] (my_group_id ignite userToken)]
            ;(.myWriter (MyLogger/getInstance) (format "%s %s" sql group_id))
            (super-sql-lst ignite group_id dataset_name group_type dataset_id lst (StringBuilder.)))))

(defn -superSql [^Ignite ignite ^Object userToken ^Object sql]
    (if (some? userToken)
        (super-sql ignite (MyCacheExUtil/restoreToLine userToken) (MyCacheExUtil/restoreToLine sql))
        (throw (Exception. "没有权限不能访问数据库！"))))

;(defn -superSql [^Ignite ignite ^Long group_id ^Object sql]
;    (if-not (> group_id -1)
;        (super-sql ignite group_id (MyCacheExUtil/restoreToLine sql))
;        (MyCacheExUtil/restoreToLine sql)))

(defn -getGroupId [^Ignite ignite ^String userToken]
    (if-let [group_id (my_group_id ignite userToken)]
        true
        false))











































































