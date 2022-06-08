(ns org.gridgain.plus.ddl.my-alter-table
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.ddl MyTableItemPK)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyAlterTable
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 获取要添加或删除的 item 定义
(defn get_items_line [items_line]
    (if (and (= (first items_line) \() (= (last items_line) \)))
        (str/trim (subs items_line 1 (- (count items_line) 1)))
        (str/trim items_line)
        ))

(defn get_items_obj [items_line]
    (my-create-table/items_obj (my-create-table/get_items (my-lexical/to-back (get_items_line items_line)))))

(defn get_obj
    ([items] (get_obj items (ArrayList.) (StringBuilder.)))
    ([[f & r] ^ArrayList lst ^StringBuilder sb]
     (if (some? f)
         (when-let [{table_item :table_item code_line :code} (my-create-table/to_item f)]
             (do
                 (.add lst table_item)
                 (.append sb (.concat (.trim (.toString code_line)) ","))
                 (recur r lst sb)
                 ))
         (let [code (str/trim (.toString sb))]
             {:lst_table_item (my-create-table/table_items lst) :code_line (str/trim (subs code 0 (- (count code) 1)))})
         )))

(defn add_or_drop [^String line]
    (cond (not (nil? (re-find #"^(?i)DROP\s+COLUMN\s+IF\s+EXISTS\s*" line))) {:line (str/lower-case line) :is_drop true :is_add false :is_exists true :is_no_exists false}
          (not (nil? (re-find #"^(?i)ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s*" line))) {:line (str/lower-case line) :is_drop false :is_add true :is_exists false :is_no_exists true}
          (not (nil? (re-find #"^(?i)DROP\s+COLUMN\s*" line))) {:line (str/lower-case line) :is_drop true :is_add false :is_exists false :is_no_exists false}
          (not (nil? (re-find #"^(?i)ADD\s+COLUMN\s*" line))) {:line (str/lower-case line) :is_drop false :is_add true :is_exists false :is_no_exists false}
          :else
          (throw (Exception. (format "修改表的语句错误！位置：%s" line)))
          ))

; 获取 alter obj
(defn get_table_alter_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [alter_table (re-find #"^(?i)ALTER\sTABLE\sIF\sEXISTS\s|^(?i)ALTER\sTABLE\s" sql) last_line (str/replace sql #"^(?i)ALTER\sTABLE\sIF\sEXISTS\s|^(?i)ALTER\sTABLE\s" "")]
            (if (some? alter_table)
                (let [table_name (str/trim (re-find #"^(?i)\w+\.\w+\s|^(?i)\w+\s" last_line)) last_line_1 (str/replace last_line #"^(?i)\w+\.\w+\s|^(?i)\w+\s" "")]
                    (if (some? table_name)
                        (let [add_or_drop_line (re-find #"^(?i)ADD\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)ADD\sCOLUMN\sIF\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sEXISTS\s|^(?i)ADD\sCOLUMN\s|^(?i)DROP\sCOLUMN\s|^(?i)ADD\s|^(?i)DROP\s" last_line_1) colums_line (str/replace last_line_1 #"^(?i)ADD\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)ADD\sCOLUMN\sIF\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sEXISTS\s|^(?i)ADD\sCOLUMN\s|^(?i)DROP\sCOLUMN\s|^(?i)ADD\s|^(?i)DROP\s" "")]
                            ;(println (get_items_obj colums_line))
                            (assoc (my-lexical/get-schema table_name) :alter_table alter_table :add_or_drop (add_or_drop add_or_drop_line) :colums (get_obj (get_items_obj colums_line)))
                            )
                        (throw (Exception. (format "修改表的语句错误！位置：%s" table_name)))))
                (throw (Exception. "修改表的语句错误！"))))
        (throw (Exception. "修改表的语句错误！"))))

(defn alter-table-obj [^Ignite ignite ^String data_set_name ^String sql_line]
    (letfn [(get-table-id [^Ignite ignite ^String data_set_name ^String table_name]
                (if (my-lexical/is-eq? "public" data_set_name)
                    (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = 0 and m.table_name = ?") (to-array [(str/lower-case table_name)]))))))
                    (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m, my_dataset as d where m.data_set_id = d.id and d.dataset_name = ? and m.table_name = ?") (to-array [(str/lower-case data_set_name) (str/lower-case table_name)])))))))
                )
            (get-add-table-item [^Ignite ignite ^Long table_id lst_table_item]
                (loop [[f & r] lst_table_item lst-rs (ArrayList.)]
                    (if (some? f)
                        (if (nil? (first (first (.getAll (.query (.cache ignite "table_item") (.setArgs (SqlFieldsQuery. "select id from MY_META.table_item where table_id = ? and column_name = ?") (to-array [table_id (str/lower-case (.getColumn_name f))])))))))
                            (let [table-item-id (.incrementAndGet (.atomicSequence ignite "table_item" 0 true))]
                                (recur r (doto lst-rs (.add (MyCacheEx. (.cache ignite "table_index") (MyTableItemPK. table-item-id table_id) (doto f (.setId table-item-id)
                                                                                                                                                      (.setTable_id table_id)) (SqlType/INSERT))))))
                            (recur r lst-rs))
                        lst-rs)))
            (get-drop-table-item [^Ignite ignite ^Long table_id lst_table_item]
                (loop [[f & r] lst_table_item lst-rs (ArrayList.)]
                    (if (some? f)
                        (if-let [table-item-id (first (first (.getAll (.query (.cache ignite "table_item") (.setArgs (SqlFieldsQuery. "select id from MY_META.table_item where table_id = ? and column_name = ?") (to-array [table_id (str/lower-case (.getColumn_name f))]))))))]
                            (recur r (doto lst-rs (.add (MyCacheEx. (.cache ignite "table_index") (MyTableItemPK. table-item-id table_id) nil (SqlType/DELETE)))))
                            (throw (Exception. (format "要删除的列 %s 不存在！" (.getColumn_name f)))))
                        lst-rs)))
            (re-obj [^String data_set_name ^String sql_line]
                (if-let [m (get_table_alter_obj sql_line)]
                    (cond (and (= (-> m :schema_name) "") (not (= data_set_name ""))) (assoc m :schema_name data_set_name)
                          (or (and (not (= (-> m :schema_name) "")) (my-lexical/is-eq? data_set_name "MY_META")) (and (not (= (-> m :schema_name) "")) (my-lexical/is-eq? (-> m :schema_name) data_set_name))) m
                          :else
                          (throw (Exception. "没有修改表的权限！"))
                          )))
            ]
        (let [{alter_table :alter_table schema_name :schema_name my-table_name :table_name {line :line is_drop :is_drop is_add :is_add} :add_or_drop {lst_table_item :lst_table_item code_line :code_line} :colums} (re-obj data_set_name sql_line)]
            (let [table_name (str/lower-case my-table_name)]
                (if-let [table_id (get-table-id ignite schema_name table_name)]
                    (cond (and (true? is_drop) (false? is_add)) {:sql (format "%s %s.%s %s (%s)" alter_table schema_name table_name line code_line) :lst_cachex (get-drop-table-item ignite table_id lst_table_item)}
                          (and (false? is_drop) (true? is_add)) {:sql (format "%s %s.%s %s (%s)" alter_table schema_name table_name line code_line) :lst_cachex (get-add-table-item ignite table_id lst_table_item)}
                          :else
                          (throw (Exception. "修改表的语句有错误！")))
                    (throw (Exception. "要修改的表不存在！"))))
            )))

; 执行实时数据集中的 ddl
(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id ^String dataset_name]
    (let [{sql :sql lst_cachex :lst_cachex} (alter-table-obj ignite dataset_name sql_line)]
        (if-not (nil? lst_cachex)
            (if (true? (.isDataSetEnabled (.configuration ignite)))
                (let [ddl_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                    {:sql (doto (ArrayList.) (.add sql)) :un_sql nil :lst_cachex (doto lst_cachex (.add (MyCacheEx. (.cache ignite "my_log") ddl_id (MyLog. ddl_id "ddl_log" (MyCacheExUtil/objToBytes sql)) (SqlType/INSERT))))})
                {:sql (doto (ArrayList.) (.add sql)) :un_sql nil :lst_cachex lst_cachex})
            (throw (Exception. "修改表的语句有错误！")))
        )
    )

; 1、如果要修改的是实时数据集，则修改实时数据集的时候要同步修改在其它数据集中的表
; 2、判断要修改的表是否是实时数据集映射到，批处理数据集中的，如果是就不能修改，如果不是就可以修改
; 执行 alter table
(defn alter_table [^Ignite ignite ^Long group_id ^String dataset_name ^String group_type ^Long dataset_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite sql_code -1 group_id dataset_name)
            (if (contains? #{"ALL" "DDL"} (str/upper-case group_type))
                (run_ddl_real_time ignite sql_code dataset_id group_id dataset_name)
                (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))))














































