(ns org.gridgain.plus.dml.my-delete
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDelete
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 获取名字
(defn get_table_name [[f & r]]
    (if (and (some? f) (my-lexical/is-eq? f "DELETE") (my-lexical/is-eq? (first r) "FROM") (my-lexical/is-eq? (first (rest (rest r))) "WHERE"))
        {:table_name (second r) :where_lst (my-lexical/double-to-signal (rest (rest (rest r))))}))

; UPDATE categories set categoryname, description where categoryname = '白酒'
(defn get_view_db [^Ignite ignite ^Long group_id ^String table_name]
    (when-let [lst_rs (first (.getAll (.query (.cache ignite "my_delete_views") (.setArgs (SqlFieldsQuery. "select m.code from my_delete_views as m join my_group_view as v on m.id = v.view_id where m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [table_name group_id "删"])))))]
        (if (> (count lst_rs) 0) (get_table_name (my-lexical/to-back (nth lst_rs 0))))))

; 判断权限
(defn get-authority [^Ignite ignite ^Long group_id lst-sql]
    (if-let [{my_table_name :table_name where_lst :where_lst} (get_table_name lst-sql)]
        (let [{schema_name :schema_name table_name :table_name} (my-lexical/get-schema my_table_name)]
            (if (and (my-lexical/is-eq? schema_name "my_meta") (> group_id 0))
                (throw (Exception. "用户不存在或者没有权限！"))
                (if-let [{v_table_name :table_name v_where_lst :where_lst} (get_view_db ignite group_id table_name)]
                    (if (my-lexical/is-eq? table_name v_table_name)
                        {:schema_name schema_name :table_name table_name :where_lst (my-update/merge_where where_lst v_where_lst)})
                    {:schema_name schema_name :table_name table_name :where_lst where_lst}
                    ))
            )
        ))

(defn get-authority-lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector sql_lst]
    (if-let [{my_table_name :table_name where_lst :where_lst} (get_table_name sql_lst)]
        (let [{schema_name :schema_name table_name :table_name} (my-lexical/get-schema my_table_name)]
            (if (and (my-lexical/is-eq? schema_name "my_meta") (> group_id 0))
                (throw (Exception. "用户不存在或者没有权限！"))
                (if-let [{v_table_name :table_name v_where_lst :where_lst} (get_view_db ignite group_id table_name)]
                    (if (my-lexical/is-eq? table_name v_table_name)
                        {:schema_name schema_name :table_name table_name :where_lst (my-update/merge_where where_lst v_where_lst)})
                    {:schema_name schema_name :table_name table_name :where_lst where_lst}
                    )))
        ))

(defn get_delete_query_sql [^Ignite ignite obj]
    (when-let [{pk_line :line lst :lst lst_pk :lst_pk dic :dic} (my-update/get_pk_def_map ignite (str/lower-case (-> obj :schema_name)) (str/lower-case (-> obj :table_name)))]
        (letfn [(get_pk_lst [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic f)
                            (recur r dic (conj lst {:item_name f :item_type (get dic f)}))
                            (recur r dic lst))
                        lst))]
            {:schema_name (-> obj :schema_name) :table_name (-> obj :table_name) :sql (format "select %s from %s.%s where %s" pk_line (-> obj :schema_name) (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_lst))) :pk_lst (get_pk_lst lst_pk dic []) :lst lst :dic dic})
        ))

; delete obj
(defn get_delete_obj [^Ignite ignite ^Long group_id lst-sql]
    (if-let [m (get-authority ignite group_id lst-sql)]
        (get_delete_query_sql ignite m)
        (throw (Exception. "删除语句字符串错误！"))))

(defn delete_run_super_admin [^Ignite ignite ^String sql]
    (if-let [{table_name :table_name} (get_table_name (my-lexical/to-back sql))]
        (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name))
            (.getAll (.query (.cache ignite (str/lower-case table_name)) (SqlFieldsQuery. sql)))
            (throw (Exception. "超级管理员不能删除具体的业务数据！")))
        (throw (Exception. "删除语句字符串错误！"))))

; 删除数据
(defn delete_run_log [^Ignite ignite ^clojure.lang.PersistentArrayMap delet_obj]
    (if-let [{schema_name :schema_name table_name :table_name sql :sql pk_lst :pk_lst} delet_obj]
        (if-let [it (.iterator (.query (.cache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                      (.setLazy true))))]
            (letfn [(get_key_obj [^Ignite ignite ^String schema_name ^String table_name row pk_lst]
                        (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s_%s" schema_name table_name)))]
                            (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder lst_kv (ArrayList.)]
                                (if (and (some? f) (some? f_pk))
                                    (let [key (format "%s_pk" (-> f_pk :item_name)) value (my-lexical/get_jave_vs (-> f_pk :item_type) f)]
                                        (recur r r_pk (doto kp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value))))
                                        )
                                    [(.build kp) lst_kv]))))
                    (get_cache_pk [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (loop [itr it lst []]
                            (if (.hasNext itr)
                                (if-let [row (.next itr)]
                                    (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                          (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite schema_name table_name row pk_lst)))
                                          :else
                                          (throw (Exception. "表没有主键！"))
                                          ))
                                lst)))
                    (get_cache_data [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (if-let [lst_pk (get_cache_pk ignite schema_name table_name it pk_lst)]
                            (loop [[f_pk & r_pk] lst_pk lst_rs []]
                                (if (some? f_pk)
                                    (if (my-lexical/is-seq? f_pk)
                                        (let [[pk kv_pk] f_pk log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                            (recur r_pk (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s_%s" schema_name table_name)) pk nil (SqlType/DELETE))
                                                                        (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id (format "%s.%s" schema_name table_name) (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name kv_pk nil (SqlType/DELETE)))) (SqlType/INSERT))])))
                                        (let [log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                            (recur r_pk (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s_%s" schema_name table_name)) f_pk nil (SqlType/DELETE))
                                                                        (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id (format "%s.%s" schema_name table_name) (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name f_pk nil (SqlType/DELETE)))) (SqlType/INSERT))]))))
                                    lst_rs))))
                    ]
                (get_cache_data ignite schema_name table_name it pk_lst))
            (throw (Exception. "要删除的数据为空！")))
        (throw (Exception. "删除语句字符串错误！"))))

; 删除数据 no log
(defn delete_run_no_log [^Ignite ignite ^clojure.lang.PersistentArrayMap delet_obj]
    (if-let [{schema_name :schema_name table_name :table_name sql :sql pk_lst :pk_lst} delet_obj]
        (if-let [it (.iterator (.query (.cache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                      (.setLazy true))))]
            (letfn [(get_key_obj [^Ignite ignite ^String schema_name ^String table_name row pk_lst]
                        (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s_%s" schema_name table_name)))]
                            (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder]
                                (if (and (some? f) (some? f_pk))
                                    (let [key (format "%s_pk" (-> f_pk :item_name)) value (my-lexical/get_jave_vs (-> f_pk :item_type) f)]
                                        (recur r r_pk (doto kp (.setField key value)))
                                        )
                                    (.build kp)))))
                    (get_cache_pk [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (loop [itr it lst []]
                            (if (.hasNext itr)
                                (if-let [row (.next itr)]
                                    (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                          (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite schema_name table_name row pk_lst)))
                                          :else
                                          (throw (Exception. "表没有主键！"))
                                          ))
                                lst)))
                    (get_cache_data [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (if-let [lst_pk (get_cache_pk ignite schema_name table_name it pk_lst)]
                            (loop [[f_pk & r_pk] lst_pk lst_rs []]
                                (if (some? f_pk)
                                    (recur r_pk (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s_%s" schema_name table_name)) f_pk nil (SqlType/DELETE))]))
                                    lst_rs))))
                    ]
                (get_cache_data ignite schema_name table_name it pk_lst))
            (throw (Exception. "要删除的数据为空！")))
        (throw (Exception. "删除语句字符串错误！"))))


; 删除数据
(defn delete_run_log_fun [^Ignite ignite ^clojure.lang.PersistentArrayMap delete_obj]
    (if-let [{schema_name :schema_name table_name :table_name sql :sql args :args pk_lst :pk_lst} delete_obj]
        (if-let [it (.iterator (.query (.cache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                      (.setArgs args)
                                                                                      (.setLazy true))))]
            (letfn [(get_key_obj [^Ignite ignite ^String schema_name ^String table_name row pk_lst]
                        (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s_%s" schema_name table_name)))]
                            (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder lst_kv (ArrayList.)]
                                (if (and (some? f) (some? f_pk))
                                    (let [key (format "%s_pk" (-> f_pk :item_name)) value (my-lexical/get_jave_vs (-> f_pk :item_type) f)]
                                        (recur r r_pk (doto kp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value))))
                                        )
                                    [(.build kp) lst_kv]))))
                    (get_cache_pk [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (loop [itr it lst []]
                            (if (.hasNext itr)
                                (if-let [row (.next itr)]
                                    (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                          (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite schema_name table_name row pk_lst)))
                                          :else
                                          (throw (Exception. "表没有主键！"))
                                          ))
                                lst)))
                    (get_cache_data [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (if-let [lst_pk (get_cache_pk ignite schema_name table_name it pk_lst)]
                            (loop [[f_pk & r_pk] lst_pk lst_rs []]
                                (if (some? f_pk)
                                    (if (my-lexical/is-seq? f_pk)
                                        (let [[pk kv_pk] f_pk log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                            (recur r_pk (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s_%s" schema_name table_name)) pk nil (SqlType/DELETE))
                                                                        (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id (format "%s.%s" schema_name table_name) (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name kv_pk nil (SqlType/DELETE)))) (SqlType/INSERT))])))
                                        (let [log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                            (recur r_pk (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s_%s" schema_name table_name)) f_pk nil (SqlType/DELETE))
                                                                        (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id (format "%s.%s" schema_name table_name) (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name f_pk nil (SqlType/DELETE)))) (SqlType/INSERT))]))))
                                    lst_rs))))
                    ]
                (get_cache_data ignite schema_name table_name it pk_lst))
            (throw (Exception. "要删除的数据为空！")))
        (throw (Exception. "删除语句字符串错误！"))))

; 删除数据 no log
(defn delete_run_no_log_fun [^Ignite ignite ^clojure.lang.PersistentArrayMap delete_obj]
    (if-let [{schema_name :schema_name table_name :table_name sql :sql args :args pk_lst :pk_lst} delete_obj]
        (if-let [it (.iterator (.query (.cache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                      (.setArgs args)
                                                                                      (.setLazy true))))]
            (letfn [(get_key_obj [^Ignite ignite ^String schema_name ^String table_name row pk_lst]
                        (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s_%s" schema_name table_name)))]
                            (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder]
                                (if (and (some? f) (some? f_pk))
                                    (let [key (format "%s_pk" (-> f_pk :item_name)) value (my-lexical/get_jave_vs (-> f_pk :item_type) f)]
                                        (recur r r_pk (doto kp (.setField key value)))
                                        )
                                    (.build kp)))))
                    (get_cache_pk [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (loop [itr it lst []]
                            (if (.hasNext itr)
                                (if-let [row (.next itr)]
                                    (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                          (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite schema_name table_name row pk_lst)))
                                          :else
                                          (throw (Exception. "表没有主键！"))
                                          ))
                                lst)))
                    (get_cache_data [^Ignite ignite ^String schema_name ^String table_name it pk_lst]
                        (if-let [lst_pk (get_cache_pk ignite schema_name table_name it pk_lst)]
                            (loop [[f_pk & r_pk] lst_pk lst_rs []]
                                (if (some? f_pk)
                                    (recur r_pk (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s_%s" schema_name table_name)) f_pk nil (SqlType/DELETE))]))
                                    lst_rs))))
                    ]
                (get_cache_data ignite schema_name table_name it pk_lst))
            (throw (Exception. "要删除的数据为空！")))
        (throw (Exception. "删除语句字符串错误！"))))

; 1、判断用户组在实时数据集，还是非实时数据
; 如果是非实时数据集,
; 获取表名后，查一下，表名是否在 对应的 my_dataset_table 中，如果在就不能添加，否则直接执行 insert sql
; 2、如果是在实时数据集是否需要 log
(defn delete_run [^Ignite ignite ^Long group_id lst-sql sql]
    (if (= group_id 0)
        ; 超级用户
        (.getAll (.query (.cache ignite "my_meta_table") (SqlFieldsQuery. sql)))
        (if-let [delete_obj (get_delete_obj ignite group_id lst-sql)]
            (if (true? (.isDataSetEnabled (.configuration ignite)))
                (my-lexical/trans ignite (delete_run_log ignite delete_obj))
                (my-lexical/trans ignite (delete_run_no_log ignite delete_obj)))))
    )

























































