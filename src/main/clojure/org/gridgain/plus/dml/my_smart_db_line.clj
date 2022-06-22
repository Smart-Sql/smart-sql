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
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
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
             (cn.log MyLogger)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartDbLine
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_query_sql [org.apache.ignite.Ignite Long java.util.List] Object]]
        ))

; 在类似 DBeaver 这种工具中开发用的，一条，几条语句一起执行

(defn insert-to-cache [ignite group_id lst]
    (let [insert_obj (my-insert/my_insert_obj ignite group_id lst)]
        (if (and (boolean? insert_obj) (true? insert_obj))
            (str/join " " lst)
            (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
                (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (my-smart-db/get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (my-smart-db/get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT)))))
    )

(defn insert-to-cache-no-authority [ignite group_id lst]
    (let [insert_obj (my-insert/my_insert_obj-no-authority ignite group_id lst)]
        (if (and (boolean? insert_obj) (true? insert_obj))
            (str/join " " lst)
            (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
                (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (my-smart-db/get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (my-smart-db/get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT)))))
    )

(defn update-to-cache [ignite group_id lst]
    (let [m-obj (my-update/my_update_obj ignite group_id lst {})]
        (if (and (boolean? m-obj) (true? m-obj))
            (str/join " " lst)
            (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} m-obj]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-update-key row (filter #(-> % :is-pk) query-lst)) (my-smart-db/get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))
                            )
                        lst-rs))))))

(defn update-to-cache-no-authority [ignite group_id lst]
    (let [m-obj (my-update/my_update_obj-authority ignite group_id lst {})]
        (if (and (boolean? m-obj) (true? m-obj))
            (str/join " " lst)
            (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} m-obj]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-update-key row (filter #(-> % :is-pk) query-lst)) (my-smart-db/get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))
                            )
                        lst-rs))))))

(defn delete-to-cache [ignite group_id lst]
    (let [m-obj (my-delete/my_delete_obj ignite group_id lst {})]
        (if (and (boolean? m-obj) (true? m-obj))
            (str/join " " lst)
            (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} m-obj]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE))))
                            )
                        lst-rs)))))
    )

(defn delete-to-cache-no-authority [ignite group_id lst]
    (let [m-obj (my-delete/my_delete_obj-no-authority ignite group_id lst {})]
        (if (and (boolean? m-obj) (true? m-obj))
            (str/join " " lst)
            (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} m-obj]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE))))
                            )
                        lst-rs))))))

(defn query-sql-no-args [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache ignite group_id lst)]
                                                       (if (string? logCache)
                                                           logCache
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")))
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache ignite group_id lst)]
                                                       (if (string? logCache)
                                                           logCache
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn query-sql-no-args-log-no-authority [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache-no-authority ignite group_id lst)]
                                                       (if (string? logCache)
                                                           logCache
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache-no-authority ignite group_id lst)]
                                                       (if (string? logCache)
                                                           logCache
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")))
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache-no-authority ignite group_id lst)]
                                                       (if (string? logCache)
                                                           logCache
                                                           (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                               "select show_msg('true') as tip;"
                                                               "select show_msg('false') as tip;")))
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

(defn -my_query_sql [^Ignite ignite ^Long group_id ^List lst]
    (query_sql ignite group_id lst))










































