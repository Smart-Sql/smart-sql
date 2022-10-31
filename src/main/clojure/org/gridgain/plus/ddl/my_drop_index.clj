(ns org.gridgain.plus.ddl.my-drop-index
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (cn.plus.model.ddl MyDataSet MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyDdlUtilEx MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (cn.plus.model MyIndexAstPk)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDropIndex
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn index_exists [^String create_index]
    (if (re-find #"(?i)\sIF\sEXISTS$" create_index)
        true
        false))

(defn index-schema-name [line]
    (if-let [lst (str/split line #"\s+.\s+")]
        (cond (= (count lst) 1) {:schema-index nil :index-name (first lst)}
              (= (count lst) 2) {:schema-index (first lst) :index-name (last lst)}
              :else
              (throw (Exception. "索引名字格式错误！"))
              )))

(defn get_drop_index_obj [^String sql]
    (let [drop_index (re-find #"^(?i)DROP\s+INDEX\s+IF\s+EXISTS\s+|^(?i)DROP\s+INDEX\s+" sql) index_name (str/replace sql #"^(?i)DROP\s+INDEX\s+IF\s+EXISTS\s+|^(?i)DROP\s+INDEX\s+" "")]
        (if (some? drop_index)
            (let [{schema-index :schema-index index-name :index-name} (index-schema-name (str/trim index_name))]
                {:drop_line (str/trim drop_index) :is_exists (index_exists (str/trim drop_index)) :schema-index schema-index :index_name index-name})
            (throw (Exception. "删除索引语句错误！")))))

(defn drop-index-obj [^Ignite ignite ^String schema_name ^String sql_line]
    (letfn [(get-index-id [^Ignite ignite ^String index_name]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_index") (.setArgs (SqlFieldsQuery. "select m.id, m.table_id from table_index as m where m.index_name = ?") (to-array [index_name])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:table_id (second f) :index_id (first f)}))
                        lst-rs)))
            (get-index-schema [^Ignite ignite ^Long table_id]
                (let [data_set_id (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.data_set_id from my_meta_tables as m where m.id = ?") (to-array [table_id]))))))]
                    (if (= data_set_id 0)
                        "PUBLIC"
                        (first (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.schema_name from my_dataset as m where m.id = ?") (to-array [data_set_id])))))))))
            (get-table-indexs-item-id [^Ignite ignite ^Long index_id]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_index_item") (.setArgs (SqlFieldsQuery. "select m.id from table_index_item as m where m.index_no = ?") (to-array [index_id])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:index_id index_id :index_item_id (first f)}))
                        lst-rs)))
            (get-cachex [^Ignite ignite ^String index_name ^ArrayList lst]
                (let [{table_id :table_id index_id :index_id} (get-index-id ignite index_name)]
                    (let [items (get-table-indexs-item-id ignite index_id)]
                        (loop [[f & r] items]
                            (if (some? f)
                                (do
                                    (let [my-key (MyTableItemPK. (-> f :index_item_id) (-> f :index_id))]
                                        (if-not (Strings/isNullOrEmpty (.getMyLogCls (.configuration ignite)))
                                            (doto lst (.add (MyCacheEx. (.cache ignite "table_index_item") my-key nil (SqlType/DELETE) (MyLogCache. "table_index_item" "MY_META" "table_index_item" my-key nil (SqlType/DELETE)))))
                                            (doto lst (.add (MyCacheEx. (.cache ignite "table_index_item") my-key nil (SqlType/DELETE) nil)))))
                                    (recur r))))
                        (let [my-key (MyTableItemPK. index_id table_id)]
                            (if-not (Strings/isNullOrEmpty (.getMyLogCls (.configuration ignite)))
                                {:schema_name (get-index-schema ignite table_id) :lst_cachex (doto lst (.add (MyCacheEx. (.cache ignite "table_index") my-key nil (SqlType/DELETE) (MyLogCache. "table_index" "MY_META" "table_index" my-key nil (SqlType/DELETE)))))}
                                {:schema_name (get-index-schema ignite table_id) :lst_cachex (doto lst (.add (MyCacheEx. (.cache ignite "table_index") my-key nil (SqlType/DELETE) nil)))}))
                        )))
            ]
        (let [{index_name :index_name} (get_drop_index_obj sql_line)]
            (if-let [{schema_name :schema_name lst_cachex :lst_cachex} (get-cachex ignite index_name (ArrayList.))]
                (if-not (nil? lst_cachex)
                    (cond (and (= schema_name "") (not (= schema_name "")) (not (my-lexical/is-eq? schema_name "MY_META"))) {:sql sql_line :lst_cachex lst_cachex}
                          (and (= schema_name "") (my-lexical/is-eq? schema_name "MY_META")) (throw (Exception. "没有删除索引的权限！"))
                          (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name) (not (my-lexical/is-eq? schema_name "MY_META"))) {:sql sql_line :lst_cachex lst_cachex}
                          (and (not (= schema_name "")) (not (my-lexical/is-eq? schema_name "MY_META")) (my-lexical/is-eq? schema_name "MY_META")) {:sql sql_line :lst_cachex lst_cachex}
                          ;(or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) {:sql sql_line :lst_cachex lst_cachex}
                          :else
                          (throw (Exception. "没有删除索引的权限！"))
                          )
                    (throw (Exception. "没有删除索引的权限！")))
                ))))

; 实时数据集
(defn run_ddl_real_time [^Ignite ignite ^String schema_name ^String sql_line]
    (let [{sql :sql lst_cachex :lst_cachex} (drop-index-obj ignite schema_name sql_line)]
        (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.add sql)) :lst_cachex lst_cachex} sql_line)))

; 删除表索引
; group_id : ^Long group_id ^String schema_name ^String group_type ^Long dataset_id
;(defn drop_index [^Ignite ignite group_id ^String sql_line]
;    (let [sql_code (str/lower-case sql_line)]
;        (if (= (first group_id) 0)
;            (run_ddl_real_time ignite (second group_id) sql_line)
;            (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
;                (run_ddl_real_time ignite (second group_id) sql_line)
;                (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))))

(defn drop_index [^Ignite ignite group_id ^String sql_line]
    (if-let [{schema-index :schema-index index_name :index_name} (get_drop_index_obj sql_line)]
        (if-let [{schema_name :schema_name} (.get (.cache ignite "index_ast") (MyIndexAstPk. schema-index index_name))]
            (if (= (first group_id) 0)
                (MyDdlUtilEx/deleteIndexCache ignite {:sql sql_line :index {:schema_index schema-index :index_name index_name}})
                (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
                    (if (my-lexical/is-eq? schema_name (second group_id))
                        (MyDdlUtilEx/deleteIndexCache ignite {:sql sql_line :index {:schema-index schema-index :index_name index_name}}))
                    (throw (Exception. "该用户组没有执行 DDL 语句的权限！")))))))



















































