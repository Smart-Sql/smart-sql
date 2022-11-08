(ns org.gridgain.plus.ddl.my-create-table-0
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyNoSqlCache MyCacheEx MyKeyValue MyLogCache SqlType)
             (cn.plus.model.ddl MySchemaTable MyDataSet MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK)
             (org.gridgain.ddl MyCreateTableUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyCreateTable_0
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [plus_create_table [org.apache.ignite.Ignite Long String String Long String String] void]]
        ))

(defn get-create-table-items
    ([lst] (get-create-table-items lst [] [] [] [] []))
    ([[f & r] create_table schema_name table_name items_line template]
     (if (some? f)
         (cond (empty? create_table) (recur r (conj create_table "create") schema_name table_name items_line template)
               (and (not (empty? create_table)) (empty? schema_name)) (cond (and (= (count create_table) 1) (my-lexical/is-eq? (first create_table) "create") (my-lexical/is-eq? f "table")) (recur r (conj create_table "table") schema_name table_name items_line template)
                                                                            (and (= (count create_table) 2) (my-lexical/is-eq? (first create_table) "create") (my-lexical/is-eq? (second create_table) "table")) (if (my-lexical/is-eq? f "if")
                                                                                                                                                                                                                     (recur r (conj create_table "if") schema_name table_name items_line template)
                                                                                                                                                                                                                     (recur r create_table (conj schema_name f) table_name items_line template))
                                                                            (and (= (count create_table) 3) (my-lexical/is-eq? (first create_table) "create") (my-lexical/is-eq? (nth create_table 1) "table") (my-lexical/is-eq? (nth create_table 2) "if") (my-lexical/is-eq? f "not")) (recur r (conj create_table "not") schema_name table_name items_line template)
                                                                            (and (= (count create_table) 4) (my-lexical/is-eq? (first create_table) "create") (my-lexical/is-eq? (nth create_table 1) "table") (my-lexical/is-eq? (nth create_table 2) "if") (my-lexical/is-eq? (nth create_table 3) "not") (my-lexical/is-eq? f "exists")) (recur r (conj create_table "exists") schema_name table_name items_line template)
                                                                            (and (= (count create_table) 5) (my-lexical/is-eq? (first create_table) "create") (my-lexical/is-eq? (nth create_table 1) "table") (my-lexical/is-eq? (nth create_table 2) "if") (my-lexical/is-eq? (nth create_table 3) "not") (my-lexical/is-eq? (nth create_table 4) "exists")) (recur r create_table (conj schema_name f) table_name items_line template)
                                                                            :else
                                                                            (throw (Exception. "Create Table 语句错误！"))
                                                                            )
               (and (not (empty? create_table)) (not (empty? schema_name)) (empty? table_name)) (cond (and (not (= (first schema_name) "")) (= f ".")) (recur (rest (rest r)) create_table schema_name (conj table_name (first r)) (conj items_line (second r)) template)
                                                                                                      (and (not (= (first schema_name) "")) (= f "(")) (recur r create_table [""] [(first schema_name)] (conj items_line f) template)
                                                                                                      :else
                                                                                                      (throw (Exception. "Create Table 语句错误！"))
                                                                                                      )
               (and (not (empty? create_table)) (not (empty? schema_name)) (not (empty? table_name)) (not (empty? items_line)) (empty? template)) (if (and (my-lexical/is-eq? f "with") (= (count r) 1))
                                                                                                                                                      (recur [] create_table schema_name table_name items_line [(first r)])
                                                                                                                                                      (recur r create_table schema_name table_name (conj items_line f) template))
               :else
               (throw (Exception. "Create Table 语句错误！"))
               )
         {:create_table (str/join " " create_table) :schema_name (first schema_name) :table_name (first table_name) :items_line items_line :template (first template)})))

(defn get_tmp_item [^String item_line]
    (if-let [items (my-lexical/to-back item_line)]
        (if (and (= (count items) 3) (= (nth items 1) "="))
            {:my_left (nth items 0) :my_right (nth items 2)}
            (throw (Exception. (format "创建表的语句中 WITH 语句出错！位置：%s" item_line))))
        (throw (Exception. (format "创建表的语句中 WITH 语句出错！位置：%s" item_line)))))

(defn get_tmp_items
    ([ignite lst_line] (get_tmp_items ignite lst_line [] (StringBuilder.)))
    ([ignite [f & r] lst sb]
     (if (some? f)
         (if (< (count lst) 2)
             (let [{my_left :my_left my_right :my_right} (get_tmp_item f)]
                 (cond (.containsKey (.getTemplateConfiguration (.configuration ignite)) my_right) (recur ignite r (conj lst f) (doto sb (.append (format "%s," (.getTemplateValue (.get (.getTemplateConfiguration (.configuration ignite)) my_right))))))
                       (my-lexical/is-eq? my_left "AFFINITY_KEY") (recur ignite r (conj lst f) (doto sb (.append (format "AFFINITY_KEY=%s," my_right))))
                       :else
                       (throw (Exception. "创建表的语句中 WITH 语句出错！只能是 TEMPLATE=XXX,AFFINITY_KEY=YYY 这种形式"))
                       ))
             (throw (Exception. "创建表的语句中 WITH 语句出错！只能是 TEMPLATE=XXX,AFFINITY_KEY=YYY 这种形式")))
         (.toString sb))))

(defn get_tmp_line [^Ignite ignite ^String template_line]
    (if (re-find #"\"$" template_line)
        (if-let [line (str/replace template_line #"\"$" "")]
            (if-let [lst_line (str/split line #"\s*,\s*")]
                (get_tmp_items ignite lst_line)
                (throw (Exception. "创建表的语句中 WITH 语句出错！")))
            (throw (Exception. "创建表的语句中 WITH 语句出错！")))
        (throw (Exception. "创建表的语句中 WITH 语句出错！"))))

; 事务执行 DDL
; 创建一个recode 记录 (sql un_sql 执行成功)
; 形成这样的列表，当执行中有 false 就执行 un_sql，
; 来回滚事务
;(defrecord ddl [^String sql ^String un_sql ^Boolean is_success])

(defn sql_lst
    ([lst] (sql_lst lst []))
    ([[f & r] rs]
     (if (some? f)
         (if (nil? r) (recur r (concat rs [f]))
                      (recur r (concat rs [f " "])))
         rs)))

(defn get_sql [^String sql]
    (str/join (sql_lst (my-lexical/to-back sql))))

; 获取 items 和 template
(defn get_items_tp [^String line_rs]
    (if-let [items (str/split line_rs #"(?i)\s\)\sWITH\s\"")]
        (if (= (count items) 2)
            {:items_line (get items 0) :template (get items 1)})
        (throw (Exception. "创建表的语句错误！没有 with 关键词！"))))

(defn get_items
    ([lst] (get_items lst [] [] []))
    ([[f & r] lst_stack item_stack lst]
     (if (some? f)
         (cond (= f "(") (recur r (conj lst_stack f) (conj item_stack f) lst)
               (= f ")") (recur r (pop lst_stack) (conj item_stack f) lst)
               (and (= f ",") (= (count lst_stack) 0) (> (count item_stack) 0)) (recur r lst_stack [] (conj lst item_stack))
               :else
               (recur r lst_stack (conj item_stack f) lst)
               )
         (if (> (count item_stack) 0)
             (conj lst item_stack)
             lst))))

(defn get_item_obj
    ([lst] (get_item_obj lst [] [] [] []))
    ([[f & r] pk_stack type_stack lst_type lst]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "comment") (= (first r) "(") (= (second (rest r)) ")")) (recur (rest (rest (rest r))) pk_stack type_stack lst_type (conj lst {:comment (second r)}))
               (and (my-lexical/is-eq? f "PRIMARY") (my-lexical/is-eq? (first r) "KEY")) (if (> (count (rest r)) 0) (recur (rest (rest r)) (conj pk_stack (second r)) type_stack lst_type lst)
                                                                                                    (recur nil nil type_stack lst_type (conj lst {:pk [(first lst)]})))
               (and (> (count pk_stack) 0) (= f ")")) (recur r [] type_stack lst_type (conj lst {:pk (filter #(not= % ",") (rest pk_stack))}))
               (and (> (count pk_stack) 0) (not= f ")")) (recur r (conj pk_stack f) type_stack lst_type lst)
               (and (> (count type_stack) 0) (= f "(")) (recur r pk_stack (conj type_stack f) (conj lst_type f) lst)
               (and (> (count type_stack) 0) (not= f ")")) (recur r pk_stack type_stack (conj lst_type f) lst)
               (and (> (count type_stack) 0) (= f ")")) (if (= (count type_stack) 1) (recur r pk_stack [] [] (conj (pop lst) (assoc (peek lst) :vs lst_type))))
               (and (my-lexical/is-eq? f "NOT") (my-lexical/is-eq? (first r) "NULL") (= (count pk_stack) 0)) (recur (rest r) pk_stack type_stack lst_type (conj lst {:not_null true}))
               (my-lexical/is-eq? f "auto") (recur r pk_stack type_stack lst_type (conj lst {:auto true}))
               (some? (re-find #"^(?i)float$|^(?i)double$|^(?i)long$|^(?i)integer$|^(?i)int$|^(?i)SMALLINT$|^(?i)TINYINT$|^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$|^(?i)BOOLEAN$|^(?i)BIGINT$|^(?i)BINARY$|^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$|^(?i)DECIMAL$|^(?i)REAL$|^(?i)VARBINARY$" f)) (if (= (first r) "(")
                                                                                                                                                                                                                                                                                                             (recur (rest r) pk_stack (conj type_stack (first r)) lst_type (conj lst {:type (my-lexical/convert_to_type f)}))
                                                                                                                                                                                                                                                                                                             (recur r pk_stack [] lst_type (conj lst {:type (my-lexical/convert_to_type f)})))
               (and (my-lexical/is-eq? f "DEFAULT") (some? (first r))) (recur (rest r) pk_stack type_stack lst_type (conj lst {:default (first r)}))
               :else
               (recur r pk_stack type_stack lst_type (conj lst f)))
         lst)))

(defn items_obj [my_items]
    (loop [[f & r] my_items lst_items []]
        (if (some? f)
            (recur r (conj lst_items (get_item_obj f)))
            lst_items)))

(defn to_item
    ([lst] (to_item lst (MyTableItem.) (StringBuilder.) #{}))
    ([[f & r] ^MyTableItem m ^StringBuilder code_line pk_set]
     (if (some? f)
         (cond (and (instance? String f) (= (Strings/isNullOrEmpty (.getColumn_name m)) true)) (let [column_name (str/lower-case f)]
                                                                                                   (.setColumn_name m column_name)
                                                                                                   (.append code_line column_name)
                                                                                                   (recur r m code_line pk_set))
               (and (instance? String f) (= (Strings/isNullOrEmpty (.getColumn_name m)) false)) (throw (Exception. (format "语句错误，位置在：%s" f)))
               (and (map? f) (contains? f :pk) (= (Strings/isNullOrEmpty (.getColumn_name m)) false)) (if (= (count pk_set) 0)
                                                                                                          (do
                                                                                                              (.setPkid m true)
                                                                                                              (recur r m code_line (conj pk_set (.getColumn_name m))))
                                                                                                          (throw (Exception. "组合主键设置错误！")))
               (and (map? f) (contains? f :pk) (= (Strings/isNullOrEmpty (.getColumn_name m)) true)) (recur r m code_line (concat pk_set (-> f :pk)))
               (and (map? f) (contains? f :type)) (let [f-type (my-lexical/to-smart-sql-type (-> f :type))]
                                                      (.setColumn_type m f-type)
                                                      (.append code_line " ")
                                                      (.append code_line f-type)
                                                      (if (contains? f :vs)
                                                          (cond (= (count (-> f :vs)) 1) (let [len (nth (-> f :vs) 0)]
                                                                                             (.setColumn_len m (MyConvertUtil/ConvertToInt len))
                                                                                             (.append code_line "(")
                                                                                             (.append code_line len)
                                                                                             (.append code_line ")"))
                                                                (= (count (-> f :vs)) 3) (let [len (nth (-> f :vs) 0) scale (nth (-> f :vs) 2)]
                                                                                             (.setColumn_len m (MyConvertUtil/ConvertToInt len))
                                                                                             (.setScale m (MyConvertUtil/ConvertToInt scale))
                                                                                             (.append code_line "(")
                                                                                             (.append code_line len)
                                                                                             (.append code_line ",")
                                                                                             (.append code_line scale)
                                                                                             (.append code_line ")"))
                                                                ))
                                                      (recur r m code_line pk_set))
               (and (map? f) (contains? f :not_null)) (do (.setNot_null m (-> f :not_null))
                                                          (.append code_line " not null")
                                                          (recur r m code_line pk_set))
               (and (map? f) (contains? f :default)) (do (.setDefault_value m (-> f :default))
                                                         (.append code_line (.concat " DEFAULT " (-> f :default)))
                                                         (recur r m code_line pk_set))
               (and (map? f) (contains? f :comment)) (do (.setComment m (my-lexical/get_str_value (-> f :comment)))
                                                         (recur r m code_line pk_set))
               (and (map? f) (contains? f :auto)) (do (.setAuto_increment m (-> f :auto))
                                                      (recur r m code_line pk_set))
               )
         (if (and (true? (.getAuto_increment m)) (not (some? (re-find #"^(?i)integer$|^(?i)int$|^(?i)SMALLINT$|^(?i)BIGINT$|^(?i)long$" (.getColumn_type m)))))
             (throw (Exception. "自增长必须是 int 或者是 long 类型的！"))
             {:table_item m :code code_line :pk pk_set}))))

(defn set_pk
    ([lst_table_item column_name] (set_pk lst_table_item column_name []))
    ([[f & r] ^String column_name ^ArrayList lst]
     (if (some? f)
         (if (my-lexical/is-eq? (.getColumn_name f) column_name)
             (recur r column_name (conj lst (doto f (.setPkid true))))
             (recur r column_name (conj lst f)))
         lst)))

(defn set_pk_set [^ArrayList lst_table_item [f & r]]
    (if (some? f)
        (recur (set_pk lst_table_item f) r)
        lst_table_item))

(defn item_obj [[f & r] item_name]
    (if (and (some? f) (my-lexical/is-eq? (.getColumn_name f) item_name))
        f
        (recur r item_name)))

(defn new_pk [[f & r] lst_table_item ^StringBuilder sb]
    (if (some? f)
        (if-let [m (item_obj lst_table_item f)]
            (do
                ;(.append sb (.concat (.getColumn_name m) "_pk"))
                (.append sb (.concat " " (.getColumn_type m)))
                (cond (and (not (nil? (.getColumn_len m))) (not (nil? (.getScale m))) (> (.getColumn_len m) 0) (> (.getScale m) 0)) (.append sb (str/join ["(" (.getColumn_len m) "," (.getScale m) ")"]))
                      (and (not (nil? (.getColumn_len m))) (> (.getColumn_len m) 0) ) (.append sb (str/join ["(" (.getColumn_len m) ")"]))
                      )
                (if (= (Strings/isNullOrEmpty (.getDefault_value m)) false)
                    (.append sb (.concat " default " (.getDefault_value m))))
                (.append sb ",")
                (recur r lst_table_item sb))
            (throw (Exception. "创建表的语句中主键错误！")))
        (.toString sb)))

(defn pk_line
    ([pk_sets] (pk_line pk_sets (StringBuilder.)))
    ([[f & r] ^StringBuilder sb]
     (if (some? f)
         (if (= (count r) 0) (recur r (doto sb (.append (str/lower-case f))))
                             (recur r (doto sb (.append (.concat (str/lower-case f) ",")))))
         (.toString sb))))

(defn get_pk_name_vs [pk_items]
    (loop [[f & r] pk_items name (StringBuilder.) value (StringBuilder.)]
        (if (some? f)
            (if (= (count r) 0)
                (recur r (doto name (.append (str/lower-case f))) (doto value (.append (str/lower-case f))))
                (recur r (doto name (.append (.concat (str/lower-case f) "_"))) (doto value (.append (.concat (str/lower-case f) ",")))))
            {:name (.toString name) :value (.toString value)})))

(defn get_pk_index_no_ds
    ([pk_sets ^String table_name] (if-let [lst_rs (get_pk_index_no_ds pk_sets table_name [])]
                                      (if-let [{name :name value :value} (get_pk_name_vs pk_sets)]
                                          (conj lst_rs {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)" table_name name table_name (str/lower-case value)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_idx" table_name name) :is_success nil})
                                          (throw (Exception. "创建表的语句错误！")))
                                      (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^String table_name lst]
     (if (some? f)
         (recur r table_name (conj lst {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)" table_name f table_name (str/lower-case f)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_idx" table_name f) :is_success nil}))
         lst)))

(defn get_pk_index_ds
    ([pk_sets ^String schema_name ^String table_name ^String schema_name] (if-let [lst_rs (get_pk_index_ds pk_sets schema_name table_name [] schema_name)]
                                                      (if-let [{name :name value :value} (get_pk_name_vs pk_sets)]
                                                          (cond (and (= schema_name "") (not (= schema_name ""))) (conj lst_rs {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_%s_idx ON %s.%s (%s)" schema_name table_name name schema_name table_name (str/lower-case value)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_%s_idx" schema_name table_name name) :is_success nil})
                                                                (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) (conj lst_rs {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_%s_idx ON %s.%s (%s)" schema_name table_name name schema_name table_name (str/lower-case value)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_%s_idx" schema_name table_name name) :is_success nil})
                                                                :else
                                                                (throw (Exception. "没有创建表语句的权限！"))
                                                                )
                                                          (throw (Exception. "创建表的语句错误！")))
                                                      (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^String schema_name ^String table_name lst ^String schema_name]
     (if (some? f)
         (cond (and (= schema_name "") (not (= schema_name ""))) (recur r schema_name table_name (conj lst {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_%s_idx ON %s.%s (%s)" schema_name table_name f schema_name table_name (str/lower-case f)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_%s_idx" schema_name table_name f) :is_success nil}) schema_name)
               (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) (recur r schema_name table_name (conj lst {:sql (format "CREATE INDEX IF NOT EXISTS %s_%s_%s_idx ON %s.%s (%s)" schema_name table_name f schema_name table_name (str/lower-case f)) :un_sql (format "DROP INDEX IF EXISTS %s_%s_%s_idx" schema_name table_name f) :is_success nil}) schema_name)
               :else
               (throw (Exception. "没有创建表语句的权限！"))
               )
         lst)))

(defn get_pk_index [pk_sets ^String schema_name ^String table_name ^String schema_name]
    ;(get_pk_index_ds pk_sets schema_name table_name schema_name)
    nil)

(defn table_items
    ([lst_table_item] (table_items lst_table_item []))
    ([[f & r] lst]
     (if (some? f)
         (if (not (Strings/isNullOrEmpty (.getColumn_name f)))
             (recur r (conj lst f))
             (recur r lst))
         lst)))

(defn get_obj_ds
    ([items ^String schema_name ^String table_name ^String schema_name] (if-let [{lst_table_item :lst_table_item code_sb :code_sb pk_sets :pk_sets} (get_obj_ds items (ArrayList.) (StringBuilder.) #{} schema_name)]
                                                  (cond (= (count pk_sets) 1) {:lst_table_item (set_pk_set lst_table_item pk_sets) :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (nth pk_sets 0))}
                                                        (> (count pk_sets) 1) (if-let [pk_set (set_pk_set lst_table_item pk_sets)]
                                                                                  {:lst_table_item pk_set :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (pk_line pk_sets))
                                                                                   :indexs (get_pk_index pk_sets schema_name table_name schema_name)}
                                                                                  (throw (Exception. "主键设置错误！")))
                                                        :else
                                                        (throw (Exception. "主键设置错误！"))
                                                        )
                                                  (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^ArrayList lst ^StringBuilder sb pk_sets schema_name]
     (if (some? f)
         (if-let [{table_item :table_item code_line :code pk :pk} (to_item f)]
             (do (.add lst table_item)
                 (if (not (nil? (last (.trim (.toString code_line)))))
                     (.append sb (.concat (.trim (.toString code_line)) ",")))
                 (recur r lst sb (concat pk_sets pk) schema_name))
             (throw (Exception. "创建表的语句错误！")))
         {:lst_table_item (table_items lst) :code_sb sb :pk_sets pk_sets})))

;(defn get_obj_no_ds
;    ([items ^String table_name] (if-let [{lst_table_item :lst_table_item code_sb :code_sb pk_sets :pk_sets} (get_obj_no_ds items (ArrayList.) (StringBuilder.) #{})]
;                                                    (cond (= (count pk_sets) 1) {:lst_table_item (set_pk_set lst_table_item pk_sets) :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (nth pk_sets 0))}
;                                                          (> (count pk_sets) 1) (if-let [pk_set (set_pk_set lst_table_item pk_sets)]
;                                                                                    {:lst_table_item pk_set :code_sb (format "%s %s PRIMARY KEY (%s)" (.toString code_sb) (new_pk pk_sets pk_set (StringBuilder.)) (pk_line pk_sets))
;                                                                                     :indexs (get_pk_index pk_sets table_name nil)}
;                                                                                    (throw (Exception. "主键设置错误！")))
;                                                          :else
;                                                          (throw (Exception. "主键设置错误！"))
;                                                          )
;                                                    (throw (Exception. "创建表的语句错误！"))))
;    ([[f & r] ^ArrayList lst ^StringBuilder sb pk_sets]
;     (if (some? f)
;         (if-let [{table_item :table_item code_line :code pk :pk} (to_item f)]
;             (do (.add lst table_item)
;                 (if (not (nil? (last (.trim (.toString code_line)))))
;                     (.append sb (.concat (.trim (.toString code_line)) ",")))
;                 (recur r lst sb (concat pk_sets pk)))
;             (throw (Exception. "创建表的语句错误！")))
;         {:lst_table_item (table_items lst) :code_sb sb :pk_sets pk_sets})))

(defn get_obj [items ^String schema_name ^String table_name ^String schema_name]
    (get_obj_ds items (str/lower-case schema_name) (str/lower-case table_name) (str/lower-case schema_name)))

; items: "( CategoryID INTEGER NOT NULL auto comment ( '产品类型ID' ) , CategoryName VARCHAR ( 15 ) NOT NULL comment ( '产品类型名' ) , Description VARCHAR comment ( '类型说明' ) , Picture VARCHAR comment ( '产品样本' ) , PRIMARY KEY ( CategoryID )"
; items: "( id int PRIMARY KEY , city_id int , name varchar , age int , company varchar"
(defn get_items_obj [items]
    (when-let [my_items (items_obj (get_items (rest (my-lexical/to-back items))))]
        my_items))

(defn get_items_obj_lst [items-lst]
    (when-let [my_items (items_obj (get_items (drop-last 1 (rest items-lst))))]
        my_items))

(defn get_template [^Ignite ignite ^String table_name ^String schema_name ^String schema_name ^String template]
    (cond (and (= schema_name "") (not (= schema_name ""))) (format "%scache_name=f_%s_%s\"" (get_tmp_line ignite template) (str/lower-case schema_name) (str/lower-case table_name))
          (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) (format "%scache_name=f_%s_%s\"" (get_tmp_line ignite template) (str/lower-case schema_name) (str/lower-case table_name))
          :else
          (throw (Exception. "没有创建表语句的权限！"))
          )
    )

(defn get_table_line_obj [^Ignite ignite ^String sql_line ^String schema_name]
    (letfn [(is-no-exists [lst]
                (let [items (take 4 lst)]
                    (if (and (my-lexical/is-eq? (first items) "CREATE") (my-lexical/is-eq? (second items) "TABLE") (= (last items) "(") (my-lexical/is-eq? (first (take-last 2 lst)) "with"))
                        (if (nil? (last lst))
                            (throw (Exception. "创建表必须有 template 的设置！"))
                            (assoc (my-lexical/get-schema (nth items 2)) :create_table "CREATE TABLE" :items_line (drop-last 2 (drop 3 lst)) :template (last lst)))
                        )))
            (is-exists [lst]
                (let [items (take 7 lst)]
                    (if (and (my-lexical/is-eq? (first items) "CREATE") (my-lexical/is-eq? (second items) "TABLE") (my-lexical/is-eq? (nth items 2) "IF") (my-lexical/is-eq? (nth items 3) "NOT") (my-lexical/is-eq? (nth items 4) "EXISTS") (= (last items) "(") (my-lexical/is-eq? (first (take-last 2 lst)) "with"))
                        (assoc (my-lexical/get-schema (nth items 5)) :create_table "CREATE TABLE IF NOT EXISTS" :items_line (drop-last 2 (drop 6 lst)) :template (last lst)))))
            (get-segment [lst]
                (if-let [m (is-no-exists lst)]
                    m
                    (if-let [vs (is-exists lst)]
                        vs)))
            (get-table-name [schema_name table_name]
                (if (Strings/isNullOrEmpty schema_name)
                    table_name
                    (str schema_name "." table_name)))]
        (let [{schema_name :schema_name table_name :table_name create_table :create_table items_line :items_line template :template} (get-segment (my-lexical/to-back sql_line)) schema_table (get-table-name schema_name table_name)]
            (if-let [{lst_table_item :lst_table_item code_sb :code_sb indexs :indexs} (get_obj (get_items_obj_lst items_line) schema_name table_name schema_name)]
                {:create_table create_table
                 :schema_name schema_name
                 :table_name table_name
                 :lst_table_item lst_table_item
                 :code_sb (.toString code_sb)
                 :indexs indexs
                 :template (get_template ignite table_name schema_name schema_name (str/join (rest template)))
                 }
                (throw (Exception. "创建表的语句错误！")))
            ))
    )

;(defn get_table_line_obj_lst [^Ignite ignite lst ^String schema_name]
;    (letfn [(is-no-exists [lst]
;                (let [items (take 4 lst)]
;                    (if (and (my-lexical/is-eq? (first items) "CREATE") (my-lexical/is-eq? (second items) "TABLE") (= (last items) "(") (my-lexical/is-eq? (first (take-last 2 lst)) "with"))
;                        (if (nil? (last lst))
;                            (throw (Exception. "创建表必须有 template 的设置！"))
;                            (assoc (my-lexical/get-schema (nth items 2)) :create_table "CREATE TABLE" :items_line (drop-last 2 (drop 3 lst)) :template (last lst)))
;                        )))
;            (is-exists [lst]
;                (let [items (take 7 lst)]
;                    (if (and (my-lexical/is-eq? (first items) "CREATE") (my-lexical/is-eq? (second items) "TABLE") (my-lexical/is-eq? (nth items 2) "IF") (my-lexical/is-eq? (nth items 3) "NOT") (my-lexical/is-eq? (nth items 4) "EXISTS") (= (last items) "(") (my-lexical/is-eq? (first (take-last 2 lst)) "with"))
;                        (assoc (my-lexical/get-schema (nth items 5)) :create_table "CREATE TABLE IF NOT EXISTS" :items_line (drop-last 2 (drop 6 lst)) :template (last lst)))))
;            (get-segment [lst]
;                (if-let [m (is-no-exists lst)]
;                    m
;                    (if-let [vs (is-exists lst)]
;                        vs)))
;            (get-table-name [schema_name table_name]
;                (if (Strings/isNullOrEmpty schema_name)
;                    table_name
;                    (str schema_name "." table_name)))]
;        (let [{schema_name :schema_name table_name :table_name create_table :create_table items_line :items_line template :template} (get-segment lst) schema_table (get-table-name schema_name table_name)]
;            (if-let [{lst_table_item :lst_table_item code_sb :code_sb indexs :indexs} (get_obj (get_items_obj_lst items_line) schema_name table_name schema_name)]
;                {:create_table create_table
;                 :schema_name schema_name
;                 :table_name table_name
;                 :lst_table_item lst_table_item
;                 :code_sb (.toString code_sb)
;                 :indexs indexs
;                 :template (get_template ignite table_name schema_name schema_name (str/join (rest template)))
;                 }
;                (throw (Exception. "创建表的语句错误！")))
;            ))
;    )

(defn get_table_line_obj_lst [^Ignite ignite lst ^String schema_name]
    (let [{schema_name :schema_name table_name :table_name create_table :create_table items_line :items_line template :template} (get-create-table-items lst)]
        (if-let [{lst_table_item :lst_table_item code_sb :code_sb indexs :indexs} (get_obj (get_items_obj_lst items_line) schema_name table_name schema_name)]
            {:create_table create_table
             :schema_name schema_name
             :table_name table_name
             :lst_table_item lst_table_item
             :code_sb (.toString code_sb)
             :indexs indexs
             :template (get_template ignite table_name schema_name schema_name (str/join (rest template)))
             }
            (throw (Exception. "创建表的语句错误！")))
        )
    )

; json 转换为 ddl 序列
(defn to_ddl_lst [^Ignite ignite ^String sql_line ^String schema_name]
    (if-let [{schema_name :schema_name create_table :create_table table_name :table_name lst_table_item :lst_table_item code_sb :code_sb indexs :indexs template :template} (get_table_line_obj ignite sql_line schema_name)]
        (cond (and (= schema_name "") (not (= schema_name ""))) {:schema_name schema_name :table_name table_name :lst_table_item lst_table_item :lst_ddl (concat (conj [] {:sql (format "%s %s.%s (%s) WITH \"%s" create_table schema_name table_name code_sb template) :un_sql (format "DROP TABLE IF EXISTS %s.%s" schema_name table_name) :is_success nil}) indexs)}
              (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) {:schema_name schema_name :table_name table_name :lst_table_item lst_table_item :lst_ddl (concat (conj [] {:sql (format "%s %s.%s (%s) WITH \"%s" create_table schema_name table_name code_sb template) :un_sql (format "DROP TABLE IF EXISTS %s.%s" schema_name table_name) :is_success nil}) indexs)}
              :else
              (throw (Exception. "没有创建表语句的权限！"))
              )
        (throw (Exception. "创建表的语句错误！"))))

(defn get_pk_data [lst]
    (loop [[f & r] lst dic-pk [] dic-data []]
        (if (some? f)
            (cond (true? (.getPkid f)) (recur r (conj dic-pk {:column_name (.getColumn_name f), :column_type (.getColumn_type f), :pkid true, :auto_increment (.getAuto_increment f)}) dic-data)
                  (false? (.getPkid f)) (recur r dic-pk (conj dic-data {:column_name (.getColumn_name f), :column_type (.getColumn_type f), :pkid false, :auto_increment (.getAuto_increment f)}))
                  )
            {:pk dic-pk :data dic-data})))

(defn to_ddl_lsts [^Ignite ignite lst ^String schema_name]
    (if-let [{schema_name-0 :schema_name create_table :create_table table_name-0 :table_name lst_table_item :lst_table_item code_sb :code_sb indexs :indexs template :template} (get_table_line_obj_lst ignite lst schema_name)]
        (let [schema_name (str/lower-case schema_name-0) table_name (str/lower-case table_name-0)]
            (cond (and (= schema_name "") (not (= schema_name ""))) {:schema_name schema_name :table_name table_name :pk-data (get_pk_data lst_table_item) :lst_table_item lst_table_item :lst_ddl (concat (conj [] {:sql (format "%s %s.%s (%s) WITH \"%s" create_table schema_name table_name code_sb template) :un_sql (format "DROP TABLE IF EXISTS %s.%s" schema_name table_name) :is_success nil}) indexs)}
                  (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) {:schema_name schema_name :table_name table_name :pk-data (get_pk_data lst_table_item) :lst_table_item lst_table_item :lst_ddl (concat (conj [] {:sql (format "%s %s.%s (%s) WITH \"%s" create_table schema_name table_name code_sb template) :un_sql (format "DROP TABLE IF EXISTS %s.%s" schema_name table_name) :is_success nil}) indexs)}
                  :else
                  (throw (Exception. "没有创建表语句的权限！"))
                  ))
        (throw (Exception. "创建表的语句错误！"))))

; 生成 my_meta_tables
(defn get_table_obj [^Ignite ignite ^String table_name ^String descrip ^String code ^Long data_set_id]
    (if-let [id (.incrementAndGet (.atomicSequence ignite "my_meta_tables" 0 true))]
        (MyTable. id table_name descrip code data_set_id)
        (throw (Exception. "数据库异常！"))))

(defn get_table_obj_lst [^Ignite ignite ^String table_name ^String descrip lst ^Long data_set_id]
    (if-let [id (.incrementAndGet (.atomicSequence ignite "my_meta_tables" 0 true))]
        (MyTable. id table_name descrip "" data_set_id)
        (throw (Exception. "数据库异常！"))))

; 生成 MyTable
(defn get_table_items_obj
    ([^Ignite ignite lst_table_item table_id] (get_table_items_obj ignite lst_table_item table_id []))
    ([^Ignite ignite [f & r] table_id lst]
     (if (some? f)
         (if-let [id (.incrementAndGet (.atomicSequence ignite "table_item" 0 true))]
             (recur ignite r table_id (conj lst {:table "table_item" :key (MyTableItemPK. id table_id) :value (doto f (.setId id)
                                                                                                  (.setTable_id table_id))}))
             (throw (Exception. "数据库异常！")))
         lst)))

; 生成 MyCacheEx
(defn get_my_table [^Ignite ignite ^String table_name ^String descrip ^String code lst_table_item ^Long data_set_id]
    (if-let [table (get_table_obj ignite table_name descrip code data_set_id)]
        (if-let [lst_items (get_table_items_obj ignite lst_table_item (.getId table))]
            (cons {:table "my_meta_tables" :key (.getId table) :value table} lst_items)
            (throw (Exception. "数据库异常！")))
        (throw (Exception. "数据库异常！"))))

(defn get_my_table_lst [^Ignite ignite ^String table_name ^String descrip lst lst_table_item ^Long data_set_id]
    (if-let [table (get_table_obj_lst ignite table_name descrip lst data_set_id)]
        (if-let [lst_items (get_table_items_obj ignite lst_table_item (.getId table))]
            (cons {:table "my_meta_tables" :key (.getId table) :value table} lst_items)
            (throw (Exception. "数据库异常！")))
        (throw (Exception. "数据库异常！"))))

(defn to_mycachex
    ([^Ignite ignite lst_dml_table] (to_mycachex ignite lst_dml_table (ArrayList.)))
    ([^Ignite ignite [f & r] lst]
     (if (some? f)
         (if-not (Strings/isNullOrEmpty (.getMyLogCls (.configuration ignite)))
             (recur ignite r (doto lst (.add (MyCacheEx. (.cache ignite (-> f :table)) (-> f :key) (-> f :value) (SqlType/INSERT) (MyLogCache. (-> f :table) "MY_META" (-> f :table) (-> f :key) (-> f :value) (SqlType/INSERT))))))
             (recur ignite r (doto lst (.add (MyCacheEx. (.cache ignite (-> f :table)) (-> f :key) (-> f :value) (SqlType/INSERT) nil)))))
         lst)))

; 先执行 lst_ddl 全部成功后，在执行 lst_dml_table
; 如果 lst_dml_table 执行失败，上面的也要回滚
; lst_ddl： [ddl]
; lst_dml_table: [{:table "表名" :key PK_ID :value 值}]
; 转成 ArrayList 用 java 来执行
(defn run_ddl_dml [^Ignite ignite lst_ddl lst_dml_table no-sql-cache code]
    (MyCreateTableUtil/run_ddl_dml ignite (my-lexical/to_arryList lst_ddl) lst_dml_table no-sql-cache code))

; group_id序列： group_id schema_name group_type dataset_id
(defn my_create_table_lst [^Ignite ignite group_id ^String descrip lst]
    (if (= (first group_id) 0)
        (if-let [{schema_name :schema_name table_name :table_name pk-data :pk-data lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lsts ignite lst (str/lower-case (second group_id)))]
            (if-not (and (my-lexical/is-eq? schema_name "my_meta") (contains? plus-init-sql/my-grid-tables-set table_name))
                (if-let [lst_dml_table (to_mycachex ignite (get_my_table_lst ignite table_name descrip lst lst_table_item 0))]
                    (if (true? (.isMultiUserGroup (.configuration ignite)))
                        (run_ddl_dml ignite lst_ddl lst_dml_table (MyNoSqlCache. "table_ast" schema_name table_name (MySchemaTable. schema_name table_name) pk-data (SqlType/INSERT)) (str/join " " lst)))
                    (throw (Exception. "创建表的语句错误！")))
                (throw (Exception. "MY_META 数据集不能创建新的表！")))
            (throw (Exception. "创建表的语句错误！")))
        (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
            (if-let [{schema_name :schema_name table_name :table_name pk-data :pk-data lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lsts ignite lst (str/lower-case (second group_id)))]
                (if-not (and (my-lexical/is-eq? schema_name "my_meta") (contains? plus-init-sql/my-grid-tables-set table_name))
                    (if (and (not (my-lexical/is-eq? schema_name "my_meta")) (my-lexical/is-eq? schema_name (second group_id)))
                        (if-let [lst_dml_table (to_mycachex ignite (get_my_table_lst ignite table_name descrip lst lst_table_item (last group_id)))]
                            (if (true? (.isMultiUserGroup (.configuration ignite)))
                                (run_ddl_dml ignite lst_ddl lst_dml_table (MyNoSqlCache. "table_ast" schema_name table_name (MySchemaTable. schema_name table_name) pk-data (SqlType/INSERT)) (str/join " " lst)))
                            (throw (Exception. "创建表的语句错误！"))
                            ))
                    (throw (Exception. "MY_META 数据集不能创建新的表！")))
                (throw (Exception. "创建表的语句错误！")))
            (throw (Exception. "该用户组没有创建表的权限！")))))

(defn my_get_obj_ds
    ([items] (if-let [{lst_table_item :lst_table_item code_sb :code_sb pk_sets :pk_sets} (my_get_obj_ds items (ArrayList.) (StringBuilder.) #{})]
                 (cond (= (count pk_sets) 1) {:lst_table_item (set_pk_set lst_table_item pk_sets) :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (nth pk_sets 0))}
                       (> (count pk_sets) 1) (if-let [pk_set (set_pk_set lst_table_item pk_sets)]
                                                 {:lst_table_item pk_set :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (pk_line pk_sets))}
                                                 (throw (Exception. "主键设置错误！")))
                       :else
                       (throw (Exception. "主键设置错误！"))
                       )
                 (throw (Exception. "创建表的语句错误！"))))
    ([[f & r] ^ArrayList lst ^StringBuilder sb pk_sets]
     (if (some? f)
         (if-let [{table_item :table_item code_line :code pk :pk} (to_item f)]
             (do (.add lst table_item)
                 (if (not (nil? (last (.trim (.toString code_line)))))
                     (.append sb (.concat (.trim (.toString code_line)) ",")))
                 (recur r lst sb (concat pk_sets pk)))
             (throw (Exception. "创建表的语句错误！")))
         {:lst_table_item (table_items lst) :code_sb sb :pk_sets pk_sets})))


;(defn my_create_table_lst [^Ignite ignite group_id ^String descrip lst]
;    (if (= (first group_id) 0)
;        (if-let [{schema_name :schema_name table_name :table_name pk-data :pk-data lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lsts ignite lst (str/lower-case (second group_id)))]
;            (if-not (and (my-lexical/is-eq? schema_name "my_meta") (contains? plus-init-sql/my-grid-tables-set table_name))
;                (if-let [lst_dml_table (to_mycachex ignite (get_my_table_lst ignite table_name descrip lst lst_table_item 0))]
;                    (if (true? (.isMultiUserGroup (.configuration ignite)))
;                        (run_ddl_dml ignite lst_ddl lst_dml_table (MyNoSqlCache. "table_ast" schema_name table_name (MySchemaTable. schema_name table_name) pk-data (SqlType/INSERT)) (str/join " " lst)))
;                    (throw (Exception. "创建表的语句错误！")))
;                (throw (Exception. "MY_META 数据集不能创建新的表！")))
;            (throw (Exception. "创建表的语句错误！")))
;        (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
;            (if-let [{schema_name :schema_name table_name :table_name pk-data :pk-data lst_table_item :lst_table_item lst_ddl :lst_ddl} (to_ddl_lsts ignite lst (str/lower-case (second group_id)))]
;                (if-not (and (my-lexical/is-eq? schema_name "my_meta") (contains? plus-init-sql/my-grid-tables-set table_name))
;                    (if (and (not (my-lexical/is-eq? schema_name "my_meta")) (my-lexical/is-eq? schema_name (second group_id)))
;                        (if-let [lst_dml_table (to_mycachex ignite (get_my_table_lst ignite table_name descrip lst lst_table_item (last group_id)))]
;                            (if (true? (.isMultiUserGroup (.configuration ignite)))
;                                (run_ddl_dml ignite lst_ddl lst_dml_table (MyNoSqlCache. "table_ast" schema_name table_name (MySchemaTable. schema_name table_name) pk-data (SqlType/INSERT)) (str/join " " lst)))
;                            (throw (Exception. "创建表的语句错误！"))
;                            ))
;                    (throw (Exception. "MY_META 数据集不能创建新的表！")))
;                (throw (Exception. "创建表的语句错误！")))
;            (throw (Exception. "该用户组没有创建表的权限！")))))

; group_id 列表： group_id schema_name group_type dataset_id
(defn create-table [^Ignite ignite group_id ^String descrip sql-line]
    (my_create_table_lst ignite group_id descrip (my-lexical/to-back sql-line)))

; 用户 meta table 获取 ast
(defn get-meta-pk-data [^String sql]
    (letfn [(meta_table_line_obj_lst [lst ^String schema_name]
                (let [{schema_name :schema_name table_name :table_name create_table :create_table items_line :items_line template :template} (get-create-table-items lst)]
                    (if-let [{lst_table_item :lst_table_item code_sb :code_sb indexs :indexs} (get_obj (get_items_obj_lst items_line) schema_name table_name schema_name)]
                        {:create_table create_table
                         :schema_name schema_name
                         :table_name table_name
                         :lst_table_item lst_table_item
                         }
                        (throw (Exception. "创建表的语句错误！")))
                    )
                )]
        (let [{table_name :table_name lst_table_item :lst_table_item} (meta_table_line_obj_lst (my-lexical/to-back sql) "MY_META")]
            {:schema_name "MY_META" :table_name table_name :value (get_pk_data lst_table_item)})))

; java 中调用
;(defn -plus_create_table [^Ignite ignite ^Long group_id ^String schema_name ^String group_type ^Long dataset_id ^String descrip ^String code]
;    (my_create_table ignite group_id schema_name group_type dataset_id descrip code))







































