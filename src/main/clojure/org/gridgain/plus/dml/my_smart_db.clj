(ns org.gridgain.plus.dml.my-smart-db
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.nosql.my-super-cache :as my-super-cache]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MyNoSqlCache SqlType)
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
        :name org.gridgain.plus.dml.MySmartDb
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [superSql [org.apache.ignite.Ignite Object Object] String]
        ;          ^:static [getGroupId [org.apache.ignite.Ignite String] Boolean]]
        ))

(defn args-to-dic
    ([args] (args-to-dic args {}))
    ([[f & r] dic]
     (if (some? f)
         (recur r (assoc dic (str "?$p_s_5_c_f_" (gensym "n$@#c")) f))
         dic)))

(defn insert-to-cache [ignite group_id sql & args]
    (letfn [(get-insert-pk [ignite group_id pk-rs args-dic]
                (if (= (count pk-rs) 1)
                    (let [tokens (my-select-plus/sql-to-ast (-> (first pk-rs) :item_value))]
                        (my-smart-func-args-token-clj/func-token-to-clj ignite group_id tokens args-dic))
                    (loop [[f & r] pk-rs lst-rs []]
                        (if (some? f)
                            (recur r (conj lst-rs (MyKeyValue. (-> f :column_name) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> f :item_value)) args-dic))))
                            lst-rs))))
            (get-insert-data [ignite group_id data-rs args-dic]
                (loop [[f & r] data-rs lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs (MyKeyValue. (-> f :column_name) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> f :item_value)) args-dic))))
                        lst-rs)))]
        (let [args-dic (args-to-dic args)]
            (let [insert_obj (my-insert/get_insert_obj (my-lexical/to-back (apply format (str/replace sql #"\?" "%s") (keys args-dic))))]
                (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :table_name)) insert_obj)]
                    (MyLogCache. (format "f_%s_%s" (-> insert_obj :schema_name) (-> insert_obj :table_name)) (-> insert_obj :schema_name) (-> insert_obj :table_name) (get-insert-pk ignite group_id pk_rs args-dic) (get-insert-data ignite group_id data_rs args-dic) (SqlType/INSERT))))))
    )

(defn update-to-cache [ignite group_id sql & args]
    (letfn [(get-key [row pk-lst]
                (if (= (count pk-lst) 1)
                    (get row 0)
                    (loop [index 0 lst-rs []]
                        (if (< index (count pk-lst))
                            (recur (+ index 1) (conj lst-rs (MyKeyValue. (-> (nth pk-lst index) :item_name) (nth row index))))
                            lst-rs))
                    ))
            (get-value
                ([ignite group_id args-dic items] (get-value ignite group_id args-dic items []))
                ([ignite group_id args-dic [f & r] lst]
                 (if (some? f)
                     (recur ignite group_id args-dic r (conj lst (MyKeyValue. (-> f :item_name) (my-smart-func-args-token-clj/func-token-to-clj ignite group_id (-> f :item_obj) args-dic))))
                     lst)))]
        (let [args-dic (args-to-dic args)]
            (let [{schema_name :schema_name table_name :table_name sql :sql items :items pk_lst :pk_lst} (my-update/get_update_obj ignite group_id (my-lexical/to-back (apply format (str/replace sql #"\?" "%s") (keys args-dic))))]
                (let [{select-sql :sql select-args :args} (my-select-plus-args/my-ast-to-sql ignite group_id (my-select-plus/sql-to-ast (my-lexical/to-back sql)) args-dic)]
                    (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. select-sql)
                                                                                                                          (.setArgs (to-array select-args))
                                                                                                                          (.setLazy true)))) lst-rs []]
                        (if (.hasNext it)
                            (if-let [row (.next it)]
                                (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-key row pk_lst) (get-value ignite group_id args-dic items) (SqlType/UPDATE))))
                                )
                            lst-rs)))))))

(defn delete-to-cache [ignite group_id sql & args]
    (letfn [(get-key [row pk-lst]
                (if (= (count pk-lst) 1)
                    (get row 0)
                    (loop [index 0 lst-rs []]
                        (if (< index (count pk-lst))
                            (recur (+ index 1) (conj lst-rs (MyKeyValue. (-> (nth pk-lst index) :item_name) (nth row index))))
                            lst-rs))
                    ))]
        (let [args-dic (args-to-dic args)]
            (let [{schema_name :schema_name table_name :table_name sql :sql pk_lst :pk_lst} (my-delete/get_delete_obj ignite group_id (my-lexical/to-back (apply format (str/replace sql #"\?" "%s") (keys args-dic))))]
                (let [{select-sql :sql select-args :args} (my-select-plus-args/my-ast-to-sql ignite group_id (my-select-plus/sql-to-ast (my-lexical/to-back sql)) args-dic)]
                    (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. select-sql)
                                                                                                                          (.setArgs (to-array select-args))
                                                                                                                          (.setLazy true)))) lst-rs []]
                        (if (.hasNext it)
                            (if-let [row (.next it)]
                                (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (get-key row pk_lst) nil (SqlType/DELETE))))
                                )
                            lst-rs)))))))

; no sql
(defn my-insert [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name" key "key" value "value"} (.getVar my-obj)]
                                               (.put (.cache ignite (format "%s_%s" schema_name table-name)) key value)))
              (map? my-obj) (let [{table-name "table_name" key "key" value "value"} my-obj]
                                (.put (.cache ignite (format "%s_%s" schema_name table-name)) key value))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-insert-tran [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name" key "key" value "value"} (.getVar my-obj)]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table-name) schema_name table-name key value (SqlType/INSERT))
                                               ))
              (map? my-obj) (let [{table-name "table_name" key "key" value "value"} my-obj]
                                (MyNoSqlCache. (format "%s_%s" schema_name table-name) schema_name table-name key value (SqlType/INSERT)))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-update [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name" key "key" value "value"} (.getVar my-obj)]
                                               (.replace (.cache ignite (format "%s_%s" schema_name table-name)) key value)))
              (map? my-obj) (let [{table-name "table_name" key "key" value "value"} my-obj]
                                (.replace (.cache ignite (format "%s_%s" schema_name table-name)) key value))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-update-tran [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name" key "key" value "value"} (.getVar my-obj)]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table-name) schema_name table-name key value (SqlType/UPDATE))))
              (map? my-obj) (let [{table-name "table_name" key "key" value "value"} my-obj]
                                (MyNoSqlCache. (format "%s_%s" schema_name table-name) schema_name table-name key value (SqlType/UPDATE)))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-delete [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name" key "key"} (.getVar my-obj)]
                                               (.remove (.cache ignite (format "%s_%s" schema_name table-name)) key)))
              (map? my-obj) (let [{table-name "table_name" key "key"} my-obj]
                                (.remove (.cache ignite (format "%s_%s" schema_name table-name)) key))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-delete-tran [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name" key "key"} (.getVar my-obj)]
                                               (MyNoSqlCache. (format "%s_%s" schema_name table-name) schema_name table-name key nil (SqlType/DELETE))
                                               ))
              (map? my-obj) (let [{table-name "table_name" key "key"} my-obj]
                                (MyNoSqlCache. (format "%s_%s" schema_name table-name) schema_name table-name key nil (SqlType/DELETE)))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn my-drop [ignite group_id my-obj]
    (let [[schema_name] (my-lexical/my_group_schema_name ignite group_id)]
        (cond (instance? MyVar my-obj) (if (map? (.getVar my-obj))
                                           (let [{table-name "table_name"} (.getVar my-obj)]
                                               (.destroy (.cache ignite (format "%s_%s" schema_name table-name)))))
              (map? my-obj) (let [{table-name "table_name"} my-obj]
                                (.destroy (.cache ignite (format "%s_%s" schema_name table-name))))
              :else
              (throw (Exception. "No Sql 插入格式错误！")))))

(defn query_sql [ignite group_id sql & args]
    (cond (re-find #"^(?i)select\s+" sql) (if (nil? args)
                                              (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. (my-select-plus/my_plus_sql ignite group_id (my-lexical/to-back sql))) (.setLazy true))))
                                              (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. (my-select-plus/my_plus_sql ignite group_id (my-lexical/to-back sql))) (.setLazy true) (.setArgs (to-array args))))))
          (re-find #"^(?i)insert\s+" sql) (let [logCache (insert-to-cache ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
          (re-find #"^(?i)update\s+" sql) (let [logCache (update-to-cache ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          (re-find #"^(?i)delete\s+" sql) (let [logCache (delete-to-cache ignite group_id sql args)]
                                              (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
          :else
          (throw (Exception. "query_sql 只能执行 DML 语句！"))
          ))

(defn trans [ignite group_id [f & r] lst-rs]
    (if (some? f)
        (cond (re-find #"^(?i)insert\s+" (first f)) (let [logCache (insert-to-cache ignite group_id (first f) (last f))]
                                                        (recur ignite group_id r (concat lst-rs [logCache])))
              (re-find #"^(?i)update\s+" (first f)) (let [logCache (update-to-cache ignite group_id (first f) (last f))]
                                                        (recur ignite group_id r (concat lst-rs logCache)))
              (re-find #"^(?i)delete\s+" (first f)) (let [logCache (delete-to-cache ignite group_id (first f) (last f))]
                                                        (recur ignite group_id r (concat lst-rs logCache)))
              :else
              (throw (Exception. "trans 只能执行 insert、update、delete 语句！"))
              )
        (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList lst-rs))))




