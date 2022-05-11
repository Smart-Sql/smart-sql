(ns org.gridgain.plus.ddl.my-drop-table
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.context.my-context :as my-context]
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
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (org.log MyCljLogger)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDropTable
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn table_exists [^String create_index]
    (if (some? (re-find #"(?i)\sIF\sEXISTS$" create_index))
        {:create_index_line create_index :exists true}
        {:create_index_line create_index :exists false}))

(defn get_drop_table_obj [^String sql_line]
    (when-let [sql (my-create-table/get_sql sql_line)]
        (let [drop_index (re-find #"^(?i)DROP\sTable\sIF\sEXISTS\s|^(?i)DROP\sTable\s" sql) table_name (str/replace sql #"^(?i)DROP\sTable\sIF\sEXISTS\s|^(?i)DROP\sTable\s" "")]
            (if (some? drop_index)
                (assoc (my-lexical/get-schema (str/trim table_name)) :drop_line (str/trim drop_index) :is_exists (table_exists (str/trim drop_index)))
                (throw (Exception. "删除表语句错误！"))))))

(defn drop-table-obj [^Ignite ignite ^String data_set_name ^String sql_line]
    (letfn [(get-table-id [^Ignite ignite ^String data_set_name ^String table_name]
                (if (my-lexical/is-eq? "public" data_set_name)
                    (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = 0 and m.table_name = ?") (to-array [(str/lower-case table_name)]))))))
                    (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m, my_dataset as d where m.data_set_id = d.id and d.dataset_name = ? and m.table_name = ?") (to-array [(str/lower-case data_set_name) (str/lower-case table_name)])))))))
                )
            (get-table-items-id [^Ignite ignite ^Long table_id]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_item") (.setArgs (SqlFieldsQuery. "select m.id from table_item as m where m.table_id = ?") (to-array [table_id])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:table_id table_id :item_id (first f)}))
                        lst-rs)))
            (get-table-indexs-id [^Ignite ignite ^Long table_id]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_index") (.setArgs (SqlFieldsQuery. "select m.id from table_index as m where m.table_id = ?") (to-array [table_id])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:table_id table_id :index_id (first f)}))
                        lst-rs)))
            (get-table-indexs-item-id [^Ignite ignite ^Long index_id]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_index_item") (.setArgs (SqlFieldsQuery. "select m.id from table_index_item as m where m.index_no = ?") (to-array [index_id])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:index_id index_id :index_item_id (first f)}))
                        lst-rs)))
            (get-cachex [^Ignite ignite ^String data_set_name ^String table_name ^ArrayList lst]
                (if-let [table_id (get-table-id ignite data_set_name table_name)]
                    (let [item-pk (get-table-items-id ignite table_id) index-pk (get-table-indexs-id ignite table_id)]
                        (loop [[f & r] item-pk]
                            (if (some? f)
                                (do
                                    (doto lst (.add (MyCacheEx. (.cache ignite "table_item") (MyTableItemPK. (-> f :item_id) table_id) nil (SqlType/DELETE))))
                                    (recur r))))
                        (loop [[f & r] index-pk]
                            (if (some? f)
                                (do
                                    (doto lst (.add (MyCacheEx. (.cache ignite "table_index") (MyTableItemPK. (-> f :index_id) table_id) nil (SqlType/DELETE))))
                                    (loop [[index_f & index_r] (get-table-indexs-item-id ignite (-> f :index_id))]
                                        (if (some? index_f)
                                            (do
                                                (doto lst (.add (MyCacheEx. (.cache ignite "table_index_item") (MyTableItemPK. (-> index_f :index_item_id) (-> f :index_id)) nil (SqlType/DELETE))))
                                                (recur index_r))))
                                    (recur r))))
                        (doto lst (.add (MyCacheEx. (.cache ignite "my_meta_tables") table_id nil (SqlType/DELETE))))
                        )))]
        (let [{schema_name :schema_name table_name :table_name drop_line :drop_line {create_index_line :create_index_line exists :exists} :is_exists} (get_drop_table_obj sql_line)]
            (cond (and (= schema_name "") (not (= data_set_name ""))) {:sql (format "%s %s.%s" create_index_line data_set_name table_name) :lst_cachex (get-cachex ignite data_set_name table_name (ArrayList.))}
                  (or (and (not (= schema_name "")) (my-lexical/is-eq? data_set_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name data_set_name))) {:sql (format "%s %s.%s" create_index_line schema_name table_name) :lst_cachex (get-cachex ignite schema_name (str/lower-case table_name) (ArrayList.))}
                  :else
                  (throw (Exception. "没有删除表语句的权限！"))
                  )))
    )

; 实时数据集
(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^String dataset_name]
    (if-let [m (get_drop_table_obj sql_line)]
        (if-not (my-lexical/is-eq? (-> m :schema_name) "my_meta")
            (let [{sql :sql lst_cachex :lst_cachex} (drop-table-obj ignite dataset_name sql_line)]
                 (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.add sql)) :lst_cachex lst_cachex}))
            (throw (Exception. "没有执行语句的权限！")))
        ))

; 删除表
(defn drop_table [^Ignite ignite ^Long group_id ^String dataset_name ^String group_type ^Long dataset_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite sql_code dataset_name)
            (if (contains? #{"ALL" "DDL"} (str/upper-case group_type))
                (do
                    (MyCljLogger/showParams [sql_code dataset_name])
                    (run_ddl_real_time ignite sql_code dataset_name))
                (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))))

;(defn drop_table [^Ignite ignite ^Long group_id ^String sql_line]
;    (let [sql_code (str/lower-case sql_line)]
;        (if (= group_id 0)
;            (run_ddl_real_time ignite sql_code 0)
;            (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
;                (let [group_type (.getGroup_type my_group) dataset (.get (.cache ignite "my_dataset") (.getData_set_id my_group))]
;                    (if (contains? #{"ALL" "DDL"} group_type)
;                        (if (true? (.getIs_real dataset))
;                            (run_ddl_real_time ignite sql_code (.getId dataset))
;                            (if-let [m (get_drop_table_obj sql_code)]
;                                (if-not (my-lexical/is-eq? (-> m :schema_name) "my_meta")
;                                    (if-not (my-lexical/is-eq? (-> (my-lexical/get-schema (-> m :table_name)) :schema_name) "my_meta")
;                                        (if-let [tables (first (.getAll (.query (.cache ignite "my_dataset_table") (.setArgs (SqlFieldsQuery. "select COUNT(t.id) from my_dataset_table as t WHERE t.dataset_id = ? and t.table_name = ?") (to-array [(.getData_set_id my_group) (str/trim (-> m :table_name))])))))]
;                                            (if (> (first tables) 0)
;                                                (throw (Exception. (format "该用户组不能删除实时数据集对应到该数据集中的表：%s！" (str/trim (-> m :table_name)))))
;                                                (run_ddl ignite sql_code (.getId dataset) group_id)
;                                                ))
;                                        (throw (Exception. "没有执行语句的权限！")))
;                                    (throw (Exception. "没有执行语句的权限！")))
;                                (throw (Exception. "删除表语句错误！请仔细检查并参考文档"))))
;                        (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))
;                (throw (Exception. "不存在该用户组！"))
;                ))))














































