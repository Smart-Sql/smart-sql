(ns org.gridgain.plus.dml.my-smart-db
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MyNoSqlCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator Hashtable)
             (java.sql Timestamp)
             (java.math BigDecimal)
             (cn.log MyLogger)
             )
    (:gen-class
        :implements [org.gridgain.superservice.INoSqlFun]
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartDb
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [superSql [org.apache.ignite.Ignite Object Object] String]
        ;          ^:static [getGroupId [org.apache.ignite.Ignite String] Boolean]]
        ))

(declare my-insert my-insert-tran my-update my-update-tran my-delete my-delete-tran my-drop)

(defn -myInsert [this ignite group_id m]
    (my-insert ignite group_id m))

(defn -myInsertTran [this ignite group_id m]
    (my-insert-tran ignite group_id m))

(defn -myUpdate [this ignite group_id m]
    (my-update ignite group_id m))

(defn -myUpdateTran [this ignite group_id m]
    (my-update-tran ignite group_id m))

(defn -myDelete [this ignite group_id m]
    (my-delete ignite group_id m))

(defn -myDeleteTran [this ignite group_id m]
    (my-delete-tran ignite group_id m))

(defn -myDrop [this ignite group_id m]
    (my-drop ignite group_id m))

(defn args-to-dic
    ([args] (args-to-dic args {} []))
    ([[f & r] dic keys-lst]
     (if (some? f)
         (let [args-key (str "?$p_s_5_c_f_" (gensym "n$#c"))]
             (recur r (assoc dic args-key [f (type f)]) (conj keys-lst args-key)))
         {:dic dic :keys keys-lst})))

(defn get-args-to-lst
    ([lst args-key-lst] (get-args-to-lst lst args-key-lst []))
    ([[f & r] args-key-lst lst-rs]
     (if (some? f)
         (if (= f "?")
             (recur r (drop 1 args-key-lst) (conj lst-rs (first args-key-lst)))
             (recur r args-key-lst (conj lst-rs f)))
         lst-rs)))

(defn get-insert-pk [ignite group_id pk-rs args-dic]
    (if (= (count pk-rs) 1)
        (let [tokens (my-select-plus/sql-to-ast (-> (first pk-rs) :item_value))]
            (my-lexical/get_jave_vs (-> (first pk-rs) :column_type) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id tokens args-dic)))
        (loop [[f & r] pk-rs lst-rs []]
            (if (some? f)
                (recur r (conj lst-rs (MyKeyValue. (-> f :column_name) (my-lexical/get_jave_vs (-> f :column_type) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> f :item_value)) args-dic)))))
                lst-rs))))
(defn get-insert-data [ignite group_id data-rs args-dic]
    (loop [[f & r] data-rs lst-rs []]
        (if (some? f)
            (recur r (conj lst-rs (MyKeyValue. (-> f :column_name) (my-lexical/get_jave_vs (-> f :column_type) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> f :item_value)) args-dic)))))
            lst-rs)))

(defn get-update-key [row pk-lst]
    (if (= (count pk-lst) 1)
        (.get row (-> (first pk-lst) :index))
        (loop [[f & r] pk-lst lst-rs []]
            (if (some? f)
                (recur r (conj lst-rs (MyKeyValue. (-> f :column_name) (.get row (-> f :index)))))
                lst-rs))))
(defn re-update-args-dic [row data-lst args-dic]
    (cond (empty? data-lst) args-dic
          :else
          (loop [[f & r] data-lst dic (-> args-dic :dic)]
              (if (some? f)
                  (recur r (assoc dic (-> f :column_name) (.get row (-> f :index))))
                  (assoc args-dic :dic dic)))
          ))

; 如果里面有表中的列，那么就要把对于的值先找出来
(defn get-update-value [ignite group_id row data-lst args-dic items]
    (let [dic (re-update-args-dic row data-lst args-dic)]
        (loop [[f & r] items lst []]
            (if (some? f)
                (recur r (conj lst (MyKeyValue. (-> f :item_name) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id (-> f :item_obj) dic))))
                lst))))

(defn get-delete-key [row pk-lst]
    (if (= (count pk-lst) 1)
        (.get row 0)
        (loop [index 0 lst-rs []]
            (if (< index (count pk-lst))
                (recur (+ index 1) (conj lst-rs (MyKeyValue. (-> (nth pk-lst index) :item_name) (nth row index))))
                lst-rs))
        ))

(defn insert-to-cache [ignite group_id sql args]
    (if (some? args)
        (let [args-dic (args-to-dic args)]
            (let [insert_obj (my-insert/my_insert_obj ignite group_id (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)))]
                (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
                    (if (my-lexical/is-eq? (-> insert_obj :schema_name) "MY_META")
                        (MyLogCache. (str/lower-case (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs args-dic) (get-insert-data ignite group_id data_rs args-dic) (SqlType/INSERT))
                        (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs args-dic) (get-insert-data ignite group_id data_rs args-dic) (SqlType/INSERT)))
                    )
                ))
        (let [insert_obj (my-insert/my_insert_obj ignite group_id (my-lexical/to-back sql))]
            (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
                (if (my-lexical/is-eq? (-> insert_obj :schema_name) "MY_META")
                    (MyLogCache. (str/lower-case (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT))
                    (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT)))
                )
            ))
    )

(defn insert-to-cache-no-authority [ignite group_id sql args]
    (if (some? args)
        (let [args-dic (args-to-dic args)]
            (let [insert_obj (my-insert/my_insert_obj-no-authority ignite group_id (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)))]
                (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
                    (if (my-lexical/is-eq? (-> insert_obj :schema_name) "MY_META")
                        (MyLogCache. (str/lower-case (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs args-dic) (get-insert-data ignite group_id data_rs args-dic) (SqlType/INSERT))
                        (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs args-dic) (get-insert-data ignite group_id data_rs args-dic) (SqlType/INSERT)))
                    )
                ))
        (let [insert_obj (my-insert/my_insert_obj-no-authority ignite group_id (my-lexical/to-back sql))]
            (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
                (if (my-lexical/is-eq? (-> insert_obj :schema_name) "MY_META")
                    (MyLogCache. (str/lower-case (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT))
                    (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT))))
            ))
    )

(defn update-to-cache [ignite group_id sql args]
    (if (some? args)
        (let [args-dic (args-to-dic args)]
            (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} (my-update/my_update_obj ignite group_id (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)) (-> args-dic :dic))]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (if (my-lexical/is-eq? schema_name "MY_META")
                                (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) args-dic items) (SqlType/UPDATE))))
                                (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) args-dic items) (SqlType/UPDATE)))))
                            )
                        lst-rs))))
        (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} (my-update/my_update_obj ignite group_id (get-args-to-lst (my-lexical/to-back sql) []) {})]
            (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                  (.setArgs (to-array select-args))
                                                                                                                  (.setLazy true)))) lst-rs []]
                (if (.hasNext it)
                    (if-let [row (.next it)]
                        (if (my-lexical/is-eq? schema_name "MY_META")
                            (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))))
                    lst-rs)))
        ))

(defn update-to-cache-no-authority [ignite group_id sql args]
    (if (some? args)
        (let [args-dic (args-to-dic args)]
            (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} (my-update/my_update_obj-authority ignite group_id (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)) (-> args-dic :dic))]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (if (my-lexical/is-eq? schema_name "MY_META")
                                (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) args-dic items) (SqlType/UPDATE))))
                                (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) args-dic items) (SqlType/UPDATE))))))
                        lst-rs))))
        (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} (my-update/my_update_obj-authority ignite group_id (get-args-to-lst (my-lexical/to-back sql) []) {})]
            (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                  (.setArgs (to-array select-args))
                                                                                                                  (.setLazy true)))) lst-rs []]
                (if (.hasNext it)
                    (if-let [row (.next it)]
                        (if (my-lexical/is-eq? schema_name "MY_META")
                            (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-update-key row (filter #(-> % :is-pk) query-lst)) (get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))))
                    lst-rs)))
        ))

(defn delete-to-cache [ignite group_id sql args]
    (if (some? args)
        (let [args-dic (args-to-dic args)]
            (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} (my-delete/my_delete_obj ignite group_id (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)) (-> args-dic :dic))]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (if (my-lexical/is-eq? schema_name "MY_META")
                                (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))
                                (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))))
                        lst-rs))))
        (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} (my-delete/my_delete_obj ignite group_id (my-lexical/to-back sql) {})]
            (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                  (.setArgs (to-array select-args))
                                                                                                                  (.setLazy true)))) lst-rs []]
                (if (.hasNext it)
                    (if-let [row (.next it)]
                        (if (my-lexical/is-eq? schema_name "MY_META")
                            (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))))
                    lst-rs)))))

(defn delete-to-cache-no-authority [ignite group_id sql args]
    (if (some? args)
        (let [args-dic (args-to-dic args)]
            (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} (my-delete/my_delete_obj-no-authority ignite group_id (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)) (-> args-dic :dic))]
                (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                      (.setArgs (to-array select-args))
                                                                                                                      (.setLazy true)))) lst-rs []]
                    (if (.hasNext it)
                        (if-let [row (.next it)]
                            (if (my-lexical/is-eq? schema_name "MY_META")
                                (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))
                                (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))))
                        lst-rs))))
        (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} (my-delete/my_delete_obj-no-authority ignite group_id (my-lexical/to-back sql) {})]
            (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                                  (.setArgs (to-array select-args))
                                                                                                                  (.setLazy true)))) lst-rs []]
                (if (.hasNext it)
                    (if-let [row (.next it)]
                        (if (my-lexical/is-eq? schema_name "MY_META")
                            (recur it (conj lst-rs (MyLogCache. table_name schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))
                            (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-delete-key row pk_lst) nil (SqlType/DELETE))))))
                    lst-rs)))))

; no sql
(defn my-insert [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (let [{key "key" value "value"} (my-lexical/get-value my-obj)]
                                               (.put (.cache ignite (format "%s_%s" schema_name table_name)) key value)))
              (instance? Hashtable my-obj) (let [{key "key" value "value"} my-obj]
                                               (.put (.cache ignite (format "%s_%s" schema_name table_name)) key value))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-insert-tran [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (let [{key "key" value "value"} (my-lexical/get-value my-obj)]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table_name) schema_name table_name key value (SqlType/INSERT))
                                               ))
              (instance? Hashtable my-obj) (let [{key "key" value "value"} my-obj]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table_name) schema_name table_name key value (SqlType/INSERT)))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-update [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (let [{key "key" value "value"} (my-lexical/get-value my-obj)]
                                               (.replace (.cache ignite (format "%s_%s" schema_name table_name)) key value)))
              (instance? Hashtable my-obj) (let [{key "key" value "value"} my-obj]
                                               (.replace (.cache ignite (format "%s_%s" schema_name table_name)) key value))
              :else
              (throw (Exception. "No Sql 修改格式错误！")))))

(defn my-update-tran [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (let [{key "key" value "value"} (my-lexical/get-value my-obj)]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table_name) schema_name table_name key value (SqlType/UPDATE))))
              (instance? Hashtable my-obj) (let [{key "key" value "value"} my-obj]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table_name) schema_name table_name key value (SqlType/UPDATE)))
              :else
              (throw (Exception. "No Sql 修改格式错误！")))))

(defn my-delete [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (let [{key "key"} (my-lexical/get-value my-obj)]
                                               (.remove (.cache ignite (format "%s_%s" schema_name table_name)) key)))
              (instance? Hashtable my-obj) (let [{key "key"} my-obj]
                                               (.remove (.cache ignite (format "%s_%s" schema_name table_name)) key))
              :else
              (throw (Exception. "No Sql 删除格式错误！")))))

(defn my-delete-tran [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (let [{key "key"} (my-lexical/get-value my-obj)]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table_name) schema_name table_name key nil (SqlType/DELETE))
                                               ))
              (instance? Hashtable my-obj) (let [{key "key"} my-obj]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table_name) schema_name table_name key nil (SqlType/DELETE)))
              :else
              (throw (Exception. "No Sql 删除格式错误！")))))

(defn my-drop [ignite group_id my-obj]
    (let [{schema_name :schema_name table_name :table_name} (my-lexical/get_obj_schema_name ignite group_id my-obj)]
        (cond (instance? MyVar my-obj) (if (instance? Hashtable (my-lexical/get-value my-obj))
                                           (.destroy (.cache ignite (format "%s_%s" schema_name table_name))))
              (instance? Hashtable my-obj) (.destroy (.cache ignite (format "%s_%s" schema_name table_name)))
              :else
              (throw (Exception. "No Sql 清除格式错误！")))))

(defn query-sql-no-args [ignite group_id sql]
    (cond (re-find #"^(?i)select\s+" sql) (if-let [ast (my-select-plus/sql-to-ast (my-lexical/to-back sql))]
                                              (let [sql (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql)]
                                                  (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. sql) (.setLazy true))))))
          (re-find #"^(?i)insert\s+" sql) (let [logCache (insert-to-cache ignite group_id sql nil)]
                                              (if (instance? MyLogCache logCache)
                                                  (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache]))))
          (re-find #"^(?i)update\s+" sql) (let [logCache (update-to-cache ignite group_id sql nil)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          (re-find #"^(?i)delete\s+" sql) (let [logCache (delete-to-cache ignite group_id sql nil)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn query-sql-args [ignite group_id sql args]
    (cond (re-find #"^(?i)select\s+" sql) (let [args-dic (args-to-dic args)]
                                              (if-let [ast (my-select-plus/sql-to-ast (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)))]
                                                  (let [{sql :sql args-1 :args} (my-select-plus-args/my-ast-to-sql ignite group_id args-dic ast)]
                                                      (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. sql) (.setLazy true) (.setArgs (to-array args-1))))))))
          (re-find #"^(?i)insert\s+" sql) (let [logCache (insert-to-cache ignite group_id sql args)]
                                              (if (instance? MyLogCache logCache)
                                                  (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                              )
          (re-find #"^(?i)update\s+" sql) (let [logCache (update-to-cache ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          (re-find #"^(?i)delete\s+" sql) (let [logCache (delete-to-cache ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn query-sql-no-args-log-no-authority [ignite group_id sql]
    (cond (re-find #"^(?i)select\s+" sql) (if-let [ast (my-select-plus/sql-to-ast (my-lexical/to-back sql))]
                                              (let [sql (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast) :sql)]
                                                  (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. sql) (.setLazy true))))))
          (re-find #"^(?i)insert\s+" sql) (let [logCache (insert-to-cache-no-authority ignite group_id sql nil)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
          (re-find #"^(?i)update\s+" sql) (let [logCache (update-to-cache-no-authority ignite group_id sql nil)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          (re-find #"^(?i)delete\s+" sql) (let [logCache (delete-to-cache-no-authority ignite group_id sql nil)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn query-sql-args-log-no-authority [ignite group_id sql args]
    (cond (re-find #"^(?i)select\s+" sql) (let [args-dic (args-to-dic args)]
                                              (if-let [ast (my-select-plus/sql-to-ast (get-args-to-lst (my-lexical/to-back sql) (-> args-dic :keys)))]
                                                  (let [{sql :sql args-1 :args} (my-select-plus-args/my-ast-to-sql ignite group_id args-dic ast)]
                                                      (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. sql) (.setLazy true) (.setArgs (to-array args-1))))))))
          (re-find #"^(?i)insert\s+" sql) (let [logCache (insert-to-cache-no-authority ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
          (re-find #"^(?i)update\s+" sql) (let [logCache (update-to-cache-no-authority ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          (re-find #"^(?i)delete\s+" sql) (let [logCache (delete-to-cache-no-authority ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

; 1、有  my log，也有权限
; 2、有 my log, 没有权限

; 1、有  my log，也有权限
(defn my-log-authority [ignite group_id sql args]
    (if (nil? args)
        (query-sql-no-args ignite group_id (my-lexical/get-value sql))
        (if (and (= (count args) 1) (my-lexical/is-seq? (first (my-lexical/get-value args))))
            (apply query-sql-args ignite group_id (my-lexical/get-value sql) (my-lexical/get-value args))
            (query-sql-args ignite group_id (my-lexical/get-value sql) (my-lexical/get-value args)))))

; 2、有 my log, 没有权限
(defn my-log-no-authority [ignite group_id sql args]
    (if (nil? args)
        (query-sql-no-args-log-no-authority ignite group_id (my-lexical/get-value sql))
        (if (and (= (count args) 1) (my-lexical/is-seq? (first (my-lexical/get-value args))))
            (apply query-sql-args-log-no-authority ignite group_id (my-lexical/get-value sql) (my-lexical/get-value args))
            (query-sql-args-log-no-authority ignite group_id (my-lexical/get-value sql) (my-lexical/get-value args)))))

(defn query_sql [ignite group_id sql args]
    (if (.isMultiUserGroup (.configuration ignite))
        (my-log-authority ignite group_id sql args)
        (my-log-no-authority ignite group_id sql args)
        ))

(defn trans
    ([ignite group_id lst] (trans ignite group_id lst []))
    ([ignite group_id [f & r] lst-rs]
     (if (some? f)
         (cond (and (not (instance? MyNoSqlCache f)) (string? (first f)) (re-find #"^(?i)insert\s+" (first f)) (.isMultiUserGroup (.configuration ignite))) (let [logCache (insert-to-cache ignite group_id (first f) (last f))]
                                                                                                                                                                (recur ignite group_id r (concat lst-rs [logCache])))
               (and (not (instance? MyNoSqlCache f)) (string? (first f)) (re-find #"^(?i)update\s+" (first f)) (.isMultiUserGroup (.configuration ignite))) (let [logCache (update-to-cache ignite group_id (first f) (last f))]
                                                                                                                                                                (recur ignite group_id r (concat lst-rs logCache)))
               (and (not (instance? MyNoSqlCache f)) (string? (first f)) (re-find #"^(?i)delete\s+" (first f)) (.isMultiUserGroup (.configuration ignite))) (let [logCache (delete-to-cache ignite group_id (first f) (last f))]
                                                                                                                                                                (recur ignite group_id r (concat lst-rs logCache)))
               (and (not (instance? MyNoSqlCache f)) (string? (first f)) (re-find #"^(?i)insert\s+" (first f)) (false? (.isMultiUserGroup (.configuration ignite)))) (let [logCache (insert-to-cache-no-authority ignite group_id (first f) (last f))]
                                                                                                                                                                         (recur ignite group_id r (concat lst-rs [logCache])))
               (and (not (instance? MyNoSqlCache f)) (string? (first f)) (re-find #"^(?i)update\s+" (first f)) (false? (.isMultiUserGroup (.configuration ignite)))) (let [logCache (update-to-cache-no-authority ignite group_id (first f) (last f))]
                                                                                                                                                                         (recur ignite group_id r (concat lst-rs logCache)))
               (and (not (instance? MyNoSqlCache f)) (string? (first f)) (re-find #"^(?i)delete\s+" (first f)) (false? (.isMultiUserGroup (.configuration ignite)))) (let [logCache (delete-to-cache-no-authority ignite group_id (first f) (last f))]
                                                                                                                                                                         (recur ignite group_id r (concat lst-rs logCache)))
               (instance? MyNoSqlCache f) (recur ignite group_id r (concat lst-rs [f]))
               :else
               (throw (Exception. "trans 只能执行 insert、update、delete 语句！"))
               )
         (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList lst-rs)))))




