(ns org.gridgain.plus.ddl.my-alter-table
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyNoSqlCache MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.ddl MySchemaTable MyTableItemPK)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyDdlUtilEx MyDdlUtil)
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
(defn get_table_alter_obj [^String sql]
    (let [alter_table (re-find #"^(?i)ALTER\s+TABLE\s+IF\s+EXISTS\s+|^(?i)ALTER\s+TABLE\s+" sql) last_line (str/replace sql #"^(?i)ALTER\s+TABLE\s+IF\s+EXISTS\s+|^(?i)ALTER\s+TABLE\s+" "")]
        (if (some? alter_table)
            (let [table_name (str/trim (re-find #"^(?i)\w+\s*\.\s*\w+\s+|^(?i)\w+\s+" last_line)) last_line_1 (str/replace last_line #"^(?i)\w+\s*\.\s*\w+\s+|^(?i)\w+\s+" "")]
                (if (some? table_name)
                    (let [add_or_drop_line (re-find #"^(?i)ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+|^(?i)DROP\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+|^(?i)ADD\s+COLUMN\s+IF\s+EXISTS\s+|^(?i)DROP\s+COLUMN\s+IF\s+EXISTS\s+|^(?i)ADD\s+COLUMN\s+|^(?i)DROP\s+COLUMN\s+|^(?i)ADD\s+|^(?i)DROP\s+" last_line_1) colums_line (str/replace last_line_1 #"^(?i)ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+|^(?i)DROP\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+|^(?i)ADD\s+COLUMN\s+IF\s+EXISTS\s+|^(?i)DROP\s+COLUMN\s+IF\s+EXISTS\s+|^(?i)ADD\s+COLUMN\s+|^(?i)DROP\s+COLUMN\s+|^(?i)ADD\s+|^(?i)DROP\s+" "")]
                        ;(println (get_items_obj colums_line))
                        (assoc (my-lexical/get-schema table_name) :alter_table alter_table :add_or_drop (add_or_drop add_or_drop_line) :colums (get_obj (get_items_obj colums_line)))
                        )
                    (throw (Exception. (format "修改表的语句错误！位置：%s" table_name)))))
            (throw (Exception. "修改表的语句错误！")))))

(declare get-table-id get-add-table-item get-drop-table-item re-obj)

(defn get-table-id [^Ignite ignite ^String schema_name ^String table_name]
              (if (my-lexical/is-eq? "public" schema_name)
                  (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = 0 and m.table_name = ?") (to-array [(str/lower-case table_name)]))))))
                  (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m, my_dataset as d where m.data_set_id = d.id and d.schema_name = ? and m.table_name = ?") (to-array [(str/lower-case schema_name) (str/lower-case table_name)])))))))
              )
(defn get-add-table-item [^Ignite ignite ^Long table_id lst_table_item]
                    (loop [[f & r] lst_table_item lst-rs (ArrayList.)]
                        (if (some? f)
                            (if-not (nil? (first (first (.getAll (.query (.cache ignite "table_item") (.setArgs (SqlFieldsQuery. "select id from MY_META.table_item where table_id = ? and column_name = ?") (to-array [table_id (str/lower-case (.getColumn_name f))])))))))
                                (let [table-item-id (.incrementAndGet (.atomicSequence ignite "table_item" 0 true))]
                                    (let [my-key (MyTableItemPK. table-item-id table_id) my-value (doto f (.setId table-item-id)
                                                                                                          (.setTable_id table_id))]
                                        (if-not (Strings/isNullOrEmpty (.getMyLogCls (.configuration ignite)))
                                            (recur r (doto lst-rs (.add (MyCacheEx. (.cache ignite "table_index") my-key my-value (SqlType/INSERT) (MyLogCache. "table_index" "MY_META" "table_index" my-key my-value (SqlType/INSERT))))))
                                            (recur r (doto lst-rs (.add (MyCacheEx. (.cache ignite "table_index") my-key my-value (SqlType/INSERT) nil)))))
                                        )
                                    )
                                (recur r lst-rs))
                            lst-rs)))
(defn get-drop-table-item [^Ignite ignite ^Long table_id lst_table_item]
                     (loop [[f & r] lst_table_item lst-rs (ArrayList.)]
                         (if (some? f)
                             (if-let [table-item-id (first (first (.getAll (.query (.cache ignite "table_item") (.setArgs (SqlFieldsQuery. "select id from MY_META.table_item where table_id = ? and column_name = ?") (to-array [table_id (str/lower-case (.getColumn_name f))]))))))]
                                 (let [my-key (MyTableItemPK. table-item-id table_id)]
                                     (if-not (Strings/isNullOrEmpty (.getMyLogCls (.configuration ignite)))
                                         (recur r (doto lst-rs (.add (MyCacheEx. (.cache ignite "table_index") my-key nil (SqlType/DELETE) (MyLogCache. "table_index" "MY_META" "table_index" my-key nil (SqlType/DELETE))))))
                                         (recur r (doto lst-rs (.add (MyCacheEx. (.cache ignite "table_index") my-key nil (SqlType/DELETE) nil)))))
                                     )
                                 (throw (Exception. (format "要删除的列 %s 不存在！" (.getColumn_name f)))))
                             lst-rs)))
(defn re-obj [^String schema_name ^String sql_line]
        (if-let [m (get_table_alter_obj sql_line)]
            (cond (and (= (-> m :schema_name) "") (not (= schema_name ""))) (assoc m :schema_name schema_name)
                  (or (and (not (= (-> m :schema_name) "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= (-> m :schema_name) "")) (my-lexical/is-eq? (-> m :schema_name) schema_name))) m
                  :else
                  (throw (Exception. "没有修改表的权限！"))
                  )))

(defn get-date-items [data]
    (loop [[f & r] data lst #{}]
        (if (some? f)
            (recur r (conj lst (str/lower-case (-> f :column_name))))
            lst)))

(defn repace-ast [^Ignite ignite ^String schema_name ^String table_name lst_table_item is-add]
    (let [vs (.get (.cache ignite "table_ast") (MySchemaTable. schema_name table_name)) {pk :pk data :data} (my-create-table/get_pk_data lst_table_item)]
        (if (> (count pk) 0)
            (throw (Exception. "不能修改主键！"))
            (if (true? is-add)
                (MyNoSqlCache. "table_ast" schema_name table_name (MySchemaTable. schema_name table_name) (assoc vs :data (concat (-> vs :data) data)) (SqlType/UPDATE))
                (loop [[f & r] data items (get-date-items data) lst []]
                    (if (some? f)
                        (if (contains? items (str/lower-case (-> f :column_name)))
                            (recur r items lst)
                            (recur r items (conj lst f)))
                        (MyNoSqlCache. "table_ast" schema_name table_name (MySchemaTable. schema_name table_name) (assoc vs :data lst) (SqlType/UPDATE))))
                ))
        ))

(defn alter-table-obj [^Ignite ignite ^String schema_name ^String sql_line]
    (let [{alter_table :alter_table schema_name :schema_name my-table_name :table_name {line :line is_drop :is_drop is_add :is_add} :add_or_drop {lst_table_item :lst_table_item code_line :code_line} :colums} (re-obj schema_name sql_line)]
        (let [table_name (str/lower-case my-table_name)]
            (if-not (and (my-lexical/is-eq? schema_name "my_meta") (contains? plus-init-sql/my-grid-tables-set table_name))
                (if-let [table_id (get-table-id ignite schema_name table_name)]
                    (cond (and (true? is_drop) (false? is_add)) {:sql (format "%s %s.%s %s (%s)" alter_table schema_name table_name line code_line) :lst_cachex (get-drop-table-item ignite table_id lst_table_item) :nosql (repace-ast ignite schema_name table_name lst_table_item true)}
                          (and (false? is_drop) (true? is_add)) {:sql (format "%s %s.%s %s (%s)" alter_table schema_name table_name line code_line) :lst_cachex (get-add-table-item ignite table_id lst_table_item) :nosql (repace-ast ignite schema_name table_name lst_table_item false)}
                          :else
                          (throw (Exception. "修改表的语句有错误！")))
                    (throw (Exception. "要修改的表不存在！")))
                (throw (Exception. "MY_META 数据集中的表不能被修改！"))))
        ))

; 执行实时数据集中的 ddl
(defn run_ddl_real_time [^Ignite ignite ^String sql_line group_id]
    (let [{sql :sql lst_cachex :lst_cachex nosql :nosql} (alter-table-obj ignite (second group_id) sql_line)]
        (if-not (nil? lst_cachex)
            (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.add sql)) :un_sql nil :lst_cachex lst_cachex :nosql nosql} sql_line)
            (throw (Exception. "修改表的语句有错误！")))
        )
    )

; 1、如果要修改的是实时数据集，则修改实时数据集的时候要同步修改在其它数据集中的表
; 2、判断要修改的表是否是实时数据集映射到，批处理数据集中的，如果是就不能修改，如果不是就可以修改
; 执行 alter table
; group_id: ^Long group_id ^String schema_name ^String group_type ^Long dataset_id
;(defn alter_table [^Ignite ignite group_id ^String sql_line]
;    (let [sql_code (str/lower-case sql_line)]
;        (if (= (first group_id) 0)
;            (run_ddl_real_time ignite sql_line group_id)
;            (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
;                (run_ddl_real_time ignite sql_code group_id)
;                (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))))


;(defn repace-ast-add [ignite schema_name table_name data]
;    (if-let [ast (.get (.cache ignite "table_ast") (MySchemaTable. schema_name table_name))]
;        (assoc ast :data (concat (-> ast :data) data))
;        ))

(defn repace-ast-add [ignite schema_name table_name data]
    (if-let [ast (.get (.cache ignite "table_ast") (MySchemaTable. schema_name table_name))]
        ;(assoc ast :data (concat (-> ast :data) data))
        (assoc ast :data (merge (-> ast :data) data))
        ))

(defn is-contains [item data]
    (loop [[f & r] data]
        (if (some? f)
            (if (my-lexical/is-eq? (-> f :column_name) (-> item :column_name))
                true
                (recur r))
            false)))

(defn repace-ast-del [ignite schema_name table_name data]
    (if-let [ast (.get (.cache ignite "table_ast") (MySchemaTable. schema_name table_name))]
        (loop [[f & r] (keys data) my-data (-> ast :data)]
            (if (some? f)
                (if (contains? my-data f)
                    (recur r (dissoc my-data f))
                    (recur r my-data))
                (assoc ast :data my-data)))))

(defn alter-table-obj [^Ignite ignite ^String schema_name ^String sql_line]
    (let [{alter_table :alter_table schema_name :schema_name my-table_name :table_name {line :line is_drop :is_drop is_add :is_add is_exists :is_exists is_no_exists :is_no_exists} :add_or_drop {lst_table_item :lst_table_item code_line :code_line} :colums} (re-obj schema_name sql_line)]
        (let [table_name (str/lower-case my-table_name)]
            (if-not (and (my-lexical/is-eq? schema_name "my_meta") (contains? plus-init-sql/my-grid-tables-set table_name))
                (cond (and (true? is_drop) (false? is_add)) {:schema_name schema_name :table_name table_name :sql (format "%s %s.%s %s (%s)" alter_table schema_name table_name line code_line) :pk-data (repace-ast-del ignite schema_name table_name (-> (my-create-table/get_pk_data lst_table_item) :data))}
                      (and (false? is_drop) (true? is_add)) (if (true? is_no_exists)
                                                                {:schema_name schema_name :table_name table_name :sql (format "%s %s.%s %s %s" alter_table schema_name table_name line code_line) :pk-data (repace-ast-add ignite schema_name table_name (-> (my-create-table/get_pk_data lst_table_item) :data))}
                                                                {:schema_name schema_name :table_name table_name :sql (format "%s %s.%s %s (%s)" alter_table schema_name table_name line code_line) :pk-data (repace-ast-add ignite schema_name table_name (-> (my-create-table/get_pk_data lst_table_item) :data))})
                      :else
                      (throw (Exception. "修改表的语句有错误！")))
                (throw (Exception. "MY_META 数据集中的原始表不能被修改！"))))
        ))

(defn alter_table [^Ignite ignite group_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= (first group_id) 0)
            (MyDdlUtilEx/updateCache ignite (alter-table-obj ignite (second group_id) sql_code))
            (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
                (MyDdlUtilEx/updateCache ignite (alter-table-obj ignite (second group_id) sql_code))
                (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))))











































