(ns org.gridgain.plus.dml.my-smart-db-line
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.tools MyGson MyColumnMeta MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MyNoSqlCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator Hashtable)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartDbLine
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_query_sql [org.apache.ignite.Ignite Object java.util.List] Object]]
        ))

; 在类似 DBeaver 这种工具中开发用的，一条，几条语句一起执行

(defn my-cache-name [^String schema_name ^String table_name]
    (if (my-lexical/is-eq? schema_name "MY_META")
        (str/lower-case table_name)
        (format "f_%s_%s" (str/lower-case schema_name) (str/lower-case table_name))))

(defn insert-to-cache [ignite group_id lst]
    (if-let [insert_obj (my-insert/my_insert_obj ignite group_id lst)]
        (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
            (let [my_pk_rs (my-smart-db/re-pk_rs ignite pk_rs (-> insert_obj :schema_name) (-> insert_obj :table_name))]
                (if (or (nil? my_pk_rs) (empty? my_pk_rs))
                    (throw (Exception. "插入数据的表不存在，或者主键为空！"))
                    (let [my-key (my-smart-db/get-insert-pk ignite group_id my_pk_rs {:dic {}, :keys []}) my-value (my-smart-db/get-insert-data ignite group_id data_rs {:dic {}, :keys []})]
                        (cond (and (my-lexical/is-seq? my-key) (empty? my-key)) (throw (Exception. "插入数据主键不能为空！"))
                              (nil? my-key) (throw (Exception. "插入数据主键不能为空！"))
                              (empty? my-value) (throw (Exception. "插入数据不能为空！"))
                              :else (MyLogCache. (my-cache-name (-> insert_obj :schema_name) (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) my-key my-value (SqlType/INSERT))
                              ))))
            ))
    )

(defn insert-to-cache-no-authority [ignite group_id lst]
    (if-let [insert_obj (my-insert/my_insert_obj-no-authority ignite group_id lst)]
        (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
            (let [my_pk_rs (my-smart-db/re-pk_rs ignite pk_rs (-> insert_obj :schema_name) (-> insert_obj :table_name))]
                (if (or (nil? my_pk_rs) (empty? my_pk_rs))
                    (throw (Exception. "插入数据主键不能为空！"))
                    (let [my-key (my-smart-db/get-insert-pk ignite group_id my_pk_rs {:dic {}, :keys []}) my-value (my-smart-db/get-insert-data ignite group_id data_rs {:dic {}, :keys []})]
                        (cond (and (my-lexical/is-seq? my-key) (empty? my-key)) (throw (Exception. "插入数据主键不能为空！"))
                              (nil? my-key) (throw (Exception. "插入数据主键不能为空！"))
                              (empty? my-value) (throw (Exception. "插入数据不能为空！"))
                              :else (MyLogCache. (my-cache-name (-> insert_obj :schema_name) (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) my-key my-value (SqlType/INSERT))
                              ))))
            ))
    )

(defn update-to-cache [ignite group_id lst]
    (if-let [m-obj (my-update/my_update_obj ignite group_id lst {})]
        (if (contains? m-obj :k-v)
            (let [{schema_name :schema_name table_name :table_name k-v :k-v items :items select-args :args} m-obj]
                [(MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-update-k-v-key ignite group_id k-v select-args) (my-smart-db/get-update-k-v-value ignite group_id select-args items) (SqlType/UPDATE))])
            (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} m-obj]
                (loop [it (.iterator (.query (.cache ignite (my-cache-name schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                   (.setArgs (to-array select-args))
                                                                                                                   (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-update-key row (filter #(-> % :is-pk) query-lst)) (my-smart-db/get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE)))))
                        lst-rs))))
        ))

(defn update-to-cache-no-authority [ignite group_id lst]
    (if-let [m-obj (my-update/my_update_obj-authority ignite group_id lst {})]
        (if (contains? m-obj :k-v)
            (let [{schema_name :schema_name table_name :table_name k-v :k-v items :items select-args :args} m-obj]
                [(MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-update-k-v-key ignite group_id k-v select-args) (my-smart-db/get-update-k-v-value ignite group_id select-args items) (SqlType/UPDATE))])
            (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} m-obj]
                (loop [it (.iterator (.query (.cache ignite (my-cache-name schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                   (.setArgs (to-array select-args))
                                                                                                                   (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-update-key row (filter #(-> % :is-pk) query-lst)) (my-smart-db/get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE)))))
                        lst-rs))))
        ))

(defn delete-to-cache [ignite group_id lst]
    (if-let [m-obj (my-delete/my_delete_obj ignite group_id lst {})]
        (if (contains? m-obj :k-v)
            (let [{schema_name :schema_name table_name :table_name k-v :k-v select-args :args} m-obj]
                [(MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-update-k-v-key ignite group_id k-v select-args) nil (SqlType/DELETE))])
            (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} m-obj]
                (loop [it (.iterator (.query (.cache ignite (my-cache-name schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                   (.setArgs (to-array select-args))
                                                                                                                   (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (do
                                ;(println m-obj)
                                ;(println pk_lst)
                                ;(println (MyLogCache. table_name schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE)))
                                ;(println "**********************")
                                (recur it (conj lst-rs (MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE)))))
                            ;(recur it (conj lst-rs (MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE))))
                            )
                        lst-rs))))
        )
    )

(defn delete-to-cache-no-authority [ignite group_id lst]
    (if-let [m-obj (my-delete/my_delete_obj-no-authority ignite group_id lst {})]
        (if (contains? m-obj :k-v)
            (let [{schema_name :schema_name table_name :table_name k-v :k-v select-args :args} m-obj]
                [(MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-update-k-v-key ignite group_id k-v select-args) nil (SqlType/DELETE))])
            (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} m-obj]
                (loop [it (.iterator (.query (.cache ignite (my-cache-name schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                   (.setArgs (to-array select-args))
                                                                                                                   (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (my-cache-name schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE)))))
                        lst-rs))))
        ))

(defn query-sql-no-args [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")
                                                           (throw (Exception. "要更新的数据为空！或者没有权限！"))))
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")
                                                           (throw (Exception. "要删除的数据为空！或者没有权限！")))
                                                       )
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn query-sql-no-args-log-no-authority [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache-no-authority ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache-no-authority ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")
                                                           (throw (Exception. "要更新的数据为空！或者没有权限！")))
                                                       )
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache-no-authority ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")
                                                           (throw (Exception. "要删除的数据为空！或者没有权限！"))))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

; 1、有  my log，也有权限
; 2、有 my log, 没有权限

; 1、有  my log，也有权限
(defn my-log-authority [ignite group_id lst]
    (query-sql-no-args ignite group_id lst))

; 2、有 my log, 没有权限
(defn my-log-no-authority [ignite group_id lst]
    (query-sql-no-args-log-no-authority ignite group_id lst))

(defn query_sql [ignite group_id lst]
    (if (.isMultiUserGroup (.configuration ignite))
        (my-log-authority ignite group_id lst)
        (my-log-no-authority ignite group_id lst)
        ))

(defn rpc-query-sql-no-args [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "true"
                                                           "false"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "true"
                                                               "false")
                                                           (throw (Exception. "要更新的数据为空！或者没有权限！"))))
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "true"
                                                               "false")
                                                           (throw (Exception. "要删除的数据为空！或者没有权限！")))
                                                       )
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn rpc-query-sql-no-args-log-no-authority [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache-no-authority ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "true"
                                                           "false"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache-no-authority ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "true"
                                                               "false")
                                                           (throw (Exception. "要更新的数据为空！或者没有权限！")))
                                                       )
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache-no-authority ignite group_id lst)]
                                                       (if-not (empty? logCache)
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "true"
                                                               "false")
                                                           (throw (Exception. "要删除的数据为空！或者没有权限！"))))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn rpc-my-log-authority [ignite group_id lst]
    (rpc-query-sql-no-args ignite group_id lst))

; 2、有 my log, 没有权限
(defn rpc-my-log-no-authority [ignite group_id lst]
    (rpc-query-sql-no-args-log-no-authority ignite group_id lst))

(defn rpc-query_sql [ignite group_id lst]
    (if (.isMultiUserGroup (.configuration ignite))
        (rpc-my-log-authority ignite group_id lst)
        (rpc-my-log-no-authority ignite group_id lst)
        ))

(defn lst-to-sql [lst]
    (loop [[f & r] lst sb (StringBuilder.)]
        (if (some? f)
            (cond (and (= f ".") (some? (first r))) (recur (rest r) (doto (StringBuilder.) (.append (.trim (.toString sb))) (.append ".") (.append (first r)) (.append " ")))
                  ;(and (= f "(") (some? (first r))) (recur (rest r) (doto (StringBuilder.) (.append (.trim (.toString sb))) (.append "(") (.append (first r)) (.append " ")))
                  ;(and (contains? #{"select" "from" "where" "by" "limit" "having" "on" "join" "left" "inner" "right" "outer" "cross"} (str/lower-case f)) (= (first r) "(")) (recur (rest r) (doto sb (.append f) (.append " (")))
                  ;(and (= f ")") (some? (first r))) (recur (rest r) (doto (StringBuilder.) (.append (.trim (.toString sb))) (.append ")") (.append (first r)) (.append " ")))
                  :else (recur r (doto sb (.append f) (.append " "))))
            (.trim (.toString sb)))))

(defn rpc-limit [^Integer start ^Integer size]
    [{:table_alias "", :item_name (format "%s" start), :item_type "", :java_item_type java.lang.Integer, :const true}
     {:comma_symbol ","}
     {:table_alias "", :item_name (format "%s" size), :item_type "", :java_item_type java.lang.Integer, :const true}])

(defn rpc-ast-limit [ast ^Integer start ^Integer size]
    (loop [[f & r] ast lst []]
        (if (some? f)
            (if (contains? f :sql_obj)
                (if (nil? (-> f :sql_obj :limit))
                    (recur r (conj lst (assoc f :sql_obj (assoc (-> f :sql_obj) :limit (rpc-limit start size)))))
                    (recur r (conj lst f)))
                (recur r (conj lst f)))
            lst)))

(defn rpc-ast-count [ast]
    (loop [[f & r] ast lst []]
        (if (some? f)
            (if (contains? f :sql_obj)
                (recur r (conj lst (assoc f :sql_obj (assoc (-> f :sql_obj) :query-items [{:func-name "count", :lst_ps [{:operation_symbol "*"}]}]))))
                (recur r (conj lst f)))
            lst)))

;(defn rpc_select-authority [ignite group_id lst ps]
;    (if-let [ast (my-select-plus/sql-to-ast lst)]
;        (if (nil? ps)
;            (let [{start :start size :size} (MyGson/getHashtable ps)]
;                (let [ast-limit (rpc-ast-limit ast start size) ast-count (rpc-ast-count ast)]
;                    )))
;        ;(-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql)
;        ))

(defn re-rpc-select [ignite group_id lst ps]
    (let [ast (my-select-plus/sql-to-ast lst) limit-size (MyGson/getHashtable ps)]
        (let [ast-limit (rpc-ast-limit ast (get limit-size "start") (get limit-size "limit")) ast-count (rpc-ast-count ast)]
            (let [sql-limit (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast-limit) :sql) sql-count (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast-count) :sql) sql (lst-to-sql lst)]
                (let [totalProperty (first (first (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. sql-count))))) root (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. sql-limit))) ht (MyColumnMeta/getColumnMeta sql)]
                    (doto (Hashtable.) (.put "totalProperty" totalProperty) (.put "root" (MyColumnMeta/getColumnRow ht root))))))))

(defn rpc_select-authority [ignite group_id lst ps]
    (if (my-lexical/null-or-empty? ps)
        (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil (my-select-plus/sql-to-ast lst)) :sql))))
        (cond (my-lexical/is-eq? ps "meta") (MyColumnMeta/getColumnMeta (lst-to-sql lst))
              (my-lexical/is-eq? ps "count") (MyColumnMeta/getColumnCount ignite (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil (my-select-plus/sql-to-ast lst)) :sql))
              (my-lexical/is-eq? ps "schema") (let [lst-small (map str/lower-case lst)]
                                                  (cond (= '("select" "schema_name" "from" "sys" "." "schemas") lst-small) (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. "SELECT SCHEMA_NAME FROM sys.SCHEMAS")))
                                                        (= '("select" "table_name" "from" "sys" "." "tables" "where" "schema_name" "=") (drop-last lst-small)) (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (format "SELECT TABLE_NAME FROM sys.TABLES WHERE SCHEMA_NAME = %s" (last lst)))))
                                                        (= '("select" "m" "." "id" "from" "my_meta" "." "my_users_group" "m" "where" "m" "." "group_name" "=") (drop-last lst-small)) (if (my-lexical/is-eq? (format "'%s'" (.getRoot_token (.configuration ignite))) (last lst))
                                                                                                                                                                                          (ArrayList.)
                                                                                                                                                                                          (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (format "SELECT m.id FROM MY_META.MY_USERS_GROUP m WHERE m.GROUP_NAME = %s" (last lst))))))
                                                        (= '("select" "m" "." "id" "from" "my_meta" "." "my_users_group" "m" "where" "m" "." "user_token" "=") (drop-last lst-small)) (if (my-lexical/is-eq? (format "'%s'" (.getRoot_token (.configuration ignite))) (last lst))
                                                                                                                                                                                          (doto (ArrayList.) (.add (doto (ArrayList.) (.add 0))))
                                                                                                                                                                                          (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (format "SELECT m.id FROM MY_META.MY_USERS_GROUP m WHERE m.USER_TOKEN = %s" (last lst))))))
                                                        ))
              ;(my-lexical/is-eq? ps "row") (MyColumnMeta/getColumnRow ignite sql)
              :else
              (let [ht (MyGson/getHashtable ps)]
                  (cond (.containsKey ht "row") (MyColumnMeta/getColumnRow ignite (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil (my-select-plus/sql-to-ast lst)) :sql) ht)
                        (.containsKey ht "select") (re-rpc-select ignite group_id lst ps)
                        ))
              )))

(defn re-rpc-select-no-authority [ignite group_id lst ps]
    (let [ast (my-select-plus/sql-to-ast lst) limit-size (MyGson/getHashtable ps)]
        (let [ast-limit (rpc-ast-limit ast (get limit-size "start") (get limit-size "limit")) ast-count (rpc-ast-count ast)]
            (let [sql-limit (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast-limit) :sql) sql-count (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast-count) :sql) sql (lst-to-sql lst)]
                (let [totalProperty (first (first (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. sql-count))))) root (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. sql-limit))) ht (MyColumnMeta/getColumnMeta sql)]
                    (doto (Hashtable.) (.put "totalProperty" totalProperty) (.put "root" (MyColumnMeta/getColumnRow ht root))))))))

(defn rpc_select-no-authority [ignite group_id lst ps]
    (if (my-lexical/null-or-empty? ps)
        (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil (my-select-plus/sql-to-ast lst)) :sql))))
        (cond (my-lexical/is-eq? ps "meta") (MyColumnMeta/getColumnMeta (lst-to-sql lst))
              (my-lexical/is-eq? ps "count") (MyColumnMeta/getColumnCount ignite (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil (my-select-plus/sql-to-ast lst)) :sql))
              (my-lexical/is-eq? ps "schema") (let [lst-small (map str/lower-case lst)]
                                                  (cond (= '("select" "schema_name" "from" "sys" "." "schemas") lst-small) (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. "SELECT SCHEMA_NAME FROM sys.SCHEMAS")))
                                                        (= '("select" "table_name" "from" "sys" "." "tables" "where" "schema_name" "=") (drop-last lst-small)) (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (format "SELECT TABLE_NAME FROM sys.TABLES WHERE SCHEMA_NAME = %s" (last lst)))))
                                                        (= '("select" "m" "." "id" "from" "my_meta" "." "my_users_group" "m" "where" "m" "." "group_name" "=") (drop-last lst-small)) (if (my-lexical/is-eq? (format "'%s'" (.getRoot_token (.configuration ignite))) (last lst))
                                                                                                                                                                                          (ArrayList.)
                                                                                                                                                                                          (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (format "SELECT m.id FROM MY_META.MY_USERS_GROUP m WHERE m.GROUP_NAME = %s" (last lst))))))
                                                        (= '("select" "m" "." "id" "from" "my_meta" "." "my_users_group" "m" "where" "m" "." "user_token" "=") (drop-last lst-small)) (if (my-lexical/is-eq? (format "'%s'" (.getRoot_token (.configuration ignite))) (last lst))
                                                                                                                                                                                          (doto (ArrayList.) (.add (doto (ArrayList.) (.add 0))))
                                                                                                                                                                                          (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. (format "SELECT m.id FROM MY_META.MY_USERS_GROUP m WHERE m.USER_TOKEN = %s" (last lst))))))
                                                        ))
              ;(my-lexical/is-eq? ps "row") (MyColumnMeta/getColumnRow ignite sql)
              :else
              (let [ht (MyGson/getHashtable ps)]
                  (cond (.containsKey ht "row") (MyColumnMeta/getColumnRow ignite (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil (my-select-plus/sql-to-ast lst)) :sql) ht)
                        (.containsKey ht "select") (re-rpc-select-no-authority ignite group_id lst ps)
                        ))
              )))

;(defn rpc_select-no-authority [ignite group_id lst ps]
;    (if-let [ast (my-select-plus/sql-to-ast lst)]
;        (let [sql (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast) :sql)]
;            (if (my-lexical/null-or-empty? ps)
;                (.getAll (.query (.cache ignite "public_meta") (SqlFieldsQuery. sql)))
;                (cond (my-lexical/is-eq? ps "meta") (MyColumnMeta/getColumnMeta sql)
;                      (my-lexical/is-eq? ps "count") (MyColumnMeta/getColumnCount ignite sql)
;                      ;(my-lexical/is-eq? ps "row") (MyColumnMeta/getColumnRow ignite sql)
;                      :else
;                      (let [ht (MyGson/getHashtable ps)]
;                          (cond (.containsKey ht "row") (MyColumnMeta/getColumnRow ignite sql ht)))
;                      )))
;        ))

(defn rpc_select_sql [ignite group_id lst ps]
    (if (.isMultiUserGroup (.configuration ignite))
        (rpc_select-authority ignite group_id lst ps)
        (rpc_select-no-authority ignite group_id lst ps)
        ))

(defn -my_query_sql [^Ignite ignite group_id ^List lst]
    (query_sql ignite group_id lst))










































