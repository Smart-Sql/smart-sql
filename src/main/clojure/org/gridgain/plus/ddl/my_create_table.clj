(ns org.gridgain.plus.ddl.my-create-table
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
             (org.gridgain.ddl MyDdlUtilEx)
             (java.util ArrayList Date Iterator)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyCreateTable
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [plus_create_table [org.apache.ignite.Ignite Long String String Long String String] void]]
        ))

(declare get-create-table-items get_items get_item_obj items_obj get_items_obj_lst set_pk
         set_pk_set pk_line to_item table_items my_get_obj_ds)

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
               (some? (re-find #"^(?i)float$|^(?i)double$|^(?i)long$|^(?i)integer$|^(?i)int$|^(?i)SMALLINT$|^(?i)TINYINT$|^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$|^(?i)BOOLEAN$|^(?i)BIGINT$|^(?i)BINARY$|^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$|^(?i)DECIMAL$|^(?i)REAL$|^(?i)VARBINARY$|^(?i)bit$" f)) (if (= (first r) "(")
                                                                                                                                                                                                                                                                                                                                         (recur (rest r) pk_stack (conj type_stack (first r)) lst_type (conj lst {:type (my-lexical/convert_to_type f)}))
                                                                                                                                                                                                                                                                                                                                         (recur r pk_stack [] lst_type (conj lst {:type (my-lexical/convert_to_type f)})))
               (and (my-lexical/is-eq? f "DEFAULT") (some? (first r))) (recur (rest r) pk_stack type_stack lst_type (conj lst {:default (first r)}))
               :else
               (recur r pk_stack type_stack lst_type (conj lst f)))
         lst)))

(defn re-lst-vs [m]
    (loop [[f & r] m index 0 lst []]
        (if (some? f)
            (if (not (= index 1))
                (recur r (+ index 1) (conj lst f))
                (recur r (+ index 1) (conj lst (dissoc f :vs))))
            lst)))

(defn items_obj [my_items]
    (loop [[f & r] my_items lst_items []]
        (if (some? f)
            (let [m (get_item_obj f)]
                (if (and (string? (first m)) (map? (second m)) (contains? (second m) :type) (contains? (second m) :vs) (not (my-lexical/is-eq? (-> (second m) :type) "DECIMAL")))
                    (recur r (conj lst_items (re-lst-vs m)))
                    (recur r (conj lst_items m))))
            lst_items)))
(defn get_items_obj_lst [items-lst]
    (when-let [my_items (items_obj (get_items (drop-last 1 (rest items-lst))))]
        my_items))
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
(defn pk_line
    ([pk_sets] (pk_line pk_sets (StringBuilder.)))
    ([[f & r] ^StringBuilder sb]
     (if (some? f)
         (if (= (count r) 0) (recur r (doto sb (.append (str/lower-case f))))
                             (recur r (doto sb (.append (.concat (str/lower-case f) ",")))))
         (.toString sb))))
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
               (and (map? f) (contains? f :default)) (do (let [vs-type (my-lexical/vs-type (-> f :default))]
                                                             (cond (= vs-type Integer) (.setDefault_value m (MyConvertUtil/ConvertToInt (-> f :default)))
                                                                   (= vs-type Double) (.setDefault_value m (MyConvertUtil/ConvertToDouble (-> f :default)))
                                                                   (= vs-type String) (.setDefault_value m (MyConvertUtil/ConvertToString (-> f :default)))
                                                                   (= vs-type Long) (.setDefault_value m (MyConvertUtil/ConvertToLong (-> f :default)))
                                                                   (= vs-type Boolean) (.setDefault_value m (MyConvertUtil/ConvertToBoolean (-> f :default)))
                                                                   ))
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
(defn table_items
    ([lst_table_item] (table_items lst_table_item []))
    ([[f & r] lst]
     (if (some? f)
         (if (not (Strings/isNullOrEmpty (.getColumn_name f)))
             (recur r (conj lst f))
             (recur r lst))
         lst)))
(defn my_get_obj_ds
    ([items] (if-let [{lst_table_item :lst_table_item code_sb :code_sb pk_sets :pk_sets} (my_get_obj_ds items (ArrayList.) (StringBuilder.) #{})]
                 (cond (= (count pk_sets) 1) {:lst_table_item (set_pk_set lst_table_item pk_sets) :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (nth pk_sets 0))}
                       (> (count pk_sets) 1) (if-let [pk_set (set_pk_set lst_table_item pk_sets)]
                                                 {:lst_table_item pk_set :code_sb (format "%s PRIMARY KEY (%s)" (.toString code_sb) (pk_line pk_sets))}
                                                 (throw (Exception. "主键设置错误！")))
                       :else
                       (throw (Exception. "主键不存在或设置错误！"))
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

(defn get_template [^Ignite ignite ^String table_name ^String schema_name ^String schema_name ^String template]
    (cond (and (= schema_name "") (not (= schema_name ""))) (format "%scache_name=f_%s_%s\"" (get_tmp_line ignite template) (str/lower-case schema_name) (str/lower-case table_name))
          (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) (format "%scache_name=f_%s_%s\"" (get_tmp_line ignite template) (str/lower-case schema_name) (str/lower-case table_name))
          :else
          (throw (Exception. "没有创建表语句的权限！"))
          )
    )

(defn get-meta-ds [schema_name]
    (if (Strings/isNullOrEmpty schema_name)
        "PUBLIC"
        schema_name))

; group_id序列： group_id schema_name group_type dataset_id
;(defn my_create_table_lst [^Ignite ignite group_id lst]
;    (if (my-lexical/is-eq? (nth lst (- (count lst) 2)) "WITH")
;        (if (= (first group_id) 0)
;            (if-let [{create_table :create_table schema_name :schema_name table_name :table_name items_line :items_line template :template} (get-create-table-items lst)]
;                (let [tmp-line (get_template ignite table_name schema_name (get-meta-ds schema_name) (str/join (rest template)))]
;                    (let [sql (str/join " " [create_table (format "%s.%s" schema_name table_name) (str/join " " items_line) (format " WITH \"%s\"" tmp-line)])]
;                        (if (nil? (.getAll (.query (.cache ignite "my_meta_table") (SqlFieldsQuery. sql))))
;                            ())))))
;        (if (contains? #{"ALL" "DDL"} (str/upper-case (nth group_id 2)))
;            ()
;            (throw (Exception. "该用户组没有创建表的权限！"))))
;    (throw (Exception. "创建表的语句错误！没有 with 关键词！")))

(defn get_table_line_obj [^Ignite ignite lst-sql ^String schema_name]
    (let [{schema_name :schema_name table_name :table_name create_table :create_table items_line :items_line template :template} (get-create-table-items lst-sql)]
        (if-let [{lst_table_item :lst_table_item code_sb :code_sb} (my_get_obj_ds (get_items_obj_lst items_line))]
            {:create_table create_table
             :schema_name schema_name
             :table_name table_name
             :lst_table_item lst_table_item
             :code_sb (.toString code_sb)
             :template (get_template ignite table_name schema_name schema_name (str/join (rest template)))
             }
            (throw (Exception. "创建表的语句错误！")))
        ))

(defn get_table_line_obj_no_tmp [lst-sql]
    (let [{schema_name :schema_name table_name :table_name items_line :items_line} (get-create-table-items lst-sql)]
        (if-let [{lst_table_item :lst_table_item} (my_get_obj_ds (get_items_obj_lst items_line))]
            {:schema_name schema_name
             :table_name table_name
             :lst_table_item lst_table_item
             }
            (throw (Exception. "创建表的语句错误！")))
        ))

(defn get_pk_data [lst]
    (loop [[f & r] lst dic-pk [] dic-data {}]
        (if (some? f)
            (cond (true? (.getPkid f)) (recur r (conj dic-pk {:column_name (.getColumn_name f), :column_type (.getColumn_type f), :pkid true, :auto_increment (.getAuto_increment f)}) dic-data)
                  ;(false? (.getPkid f)) (recur r dic-pk (conj dic-data {:column_name (.getColumn_name f), :column_type (.getColumn_type f), :default_value (.getDefault_value f), :scale (.getScale f), :pkid false, :auto_increment (.getAuto_increment f)}))
                  (false? (.getPkid f)) (recur r dic-pk (assoc dic-data (.getColumn_name f) f)))
            {:pk dic-pk :data dic-data})))

(defn to_ddl_obj [^Ignite ignite lst-sql ^String schema_name]
    (if-let [{schema_name-0 :schema_name create_table :create_table table_name-0 :table_name lst_table_item :lst_table_item code_sb :code_sb template :template} (get_table_line_obj ignite lst-sql schema_name)]
        (let [schema_name (str/lower-case schema_name-0) table_name (str/lower-case table_name-0)]
            (cond (and (= schema_name "") (not (= schema_name "")) (not (my-lexical/is-eq? schema_name "MY_META"))) {:schema_name schema_name :table_name table_name :pk-data (get_pk_data lst_table_item) :sql (format "%s %s.%s (%s) WITH \"%s" create_table schema_name table_name code_sb template)}
                  (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) {:schema_name schema_name :table_name table_name :pk-data (get_pk_data lst_table_item) :sql (format "%s %s.%s (%s) WITH \"%s" create_table schema_name table_name code_sb template)}
                  (and (= schema_name "") (my-lexical/is-eq? schema_name "MY_META")) {:schema_name "public" :table_name table_name :pk-data (get_pk_data lst_table_item) :sql (format "%s %s.%s (%s) WITH \"%s" create_table "public" table_name code_sb template)}
                  :else
                  (throw (Exception. "没有创建表语句的权限！"))
                  ))
        (throw (Exception. "创建表的语句错误！"))))

; group_id序列： group_id schema_name group_type dataset_id
(defn my_create_table_lst [^Ignite ignite group_id lst]
    (if (= (first group_id) 0)
        (MyDdlUtilEx/saveCache ignite (to_ddl_obj ignite lst (second group_id)))
        (if (contains? #{"ALL" "DDL"} (str/upper-case (last group_id)))
            (MyDdlUtilEx/saveCache ignite (to_ddl_obj ignite lst (second group_id)))
            (throw (Exception. "该用户组没有创建表的权限！")))
        ))

(defn to_ddl_obj_no_tmp [lst-sql ^String schema_name]
    (if-let [{schema_name-0 :schema_name table_name-0 :table_name lst_table_item :lst_table_item} (get_table_line_obj_no_tmp lst-sql)]
        (let [schema_name (str/lower-case schema_name-0) table_name (str/lower-case table_name-0)]
            (cond (and (= schema_name "") (not (= schema_name "")) (not (my-lexical/is-eq? schema_name "MY_META"))) {:schema_name schema_name :table_name table_name :pk-data (get_pk_data lst_table_item)}
                  (or (and (not (= schema_name "")) (my-lexical/is-eq? schema_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name schema_name))) {:schema_name schema_name :table_name table_name :pk-data (get_pk_data lst_table_item)}
                  (and (= schema_name "") (my-lexical/is-eq? schema_name "MY_META")) {:schema_name "public" :table_name table_name :pk-data (get_pk_data lst_table_item)}
                  :else
                  (throw (Exception. "没有创建表语句的权限！"))
                  ))
        (throw (Exception. "创建表的语句错误！"))))



































