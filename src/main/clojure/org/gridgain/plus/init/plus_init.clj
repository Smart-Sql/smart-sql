(ns org.gridgain.plus.init.plus-init
    (:require
        [org.gridgain.plus.init.plus-init-sql :as plus-init]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (cn.plus.model.ddl MySchemaTable MyDataSet MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             org.tools.MyTools)
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.init.PlusInit
        ; 是否生成 class 的 main 方法
        :main false
        :state state
        ; init 构造函数
        :init init
        ; 构造函数
        :constructors {[org.apache.ignite.Ignite] []}
        ; 生成 java 调用的方法
        :methods [[initialization [] void]]
        ))

; 构造函数
(defn -init [^Ignite ignite]
    [[] (atom {:ignite ignite})])

(defn meta-ast [^Ignite ignite ^String sql]
    (if (some? (re-find #"^(?i)\s*CREATE\s+TABLE\s+" sql))
        (let [{schema_name :schema_name table_name :table_name pk-data :pk-data} (my-create-table/to_ddl_obj_no_tmp (my-lexical/to-back sql) "MY_META")]
            (let [table-ast-cache (.cache ignite "table_ast") my-pk (MySchemaTable. schema_name table_name)]
                (if-not (.containsKey table-ast-cache my-pk)
                    (.put table-ast-cache my-pk pk-data)))
            )))

; 获取 code 序列
(defn get-code-lst
    [code]
    (str/split (MyTools/eliminate_comment code) #";"))

(defn run-sql [ignite [f & rs] cache]
    (if (some? f)
        (do (.getAll (.query cache (SqlFieldsQuery. f)))
            (meta-ast ignite f)
            (recur ignite rs cache)
            )))

;; 添加自定义 template
;(defn add_template [^Ignite ignite]
;    (doto ignite
;        (.addCacheConfiguration (doto (CacheConfiguration. "MyMeta_template*")
;                                    (.setCacheMode (CacheMode/REPLICATED))
;                                    (.setReadFromBackup true)
;                                    (.setSqlSchema "MY_META")))
;        ))

; 添加自定义  template
(defn add_template [^Ignite ignite]
    (doto ignite
        (.addCacheConfiguration (doto (CacheConfiguration. "MyMeta_template*")
                                    (.setCacheMode (CacheMode/REPLICATED))
                                    (.setReadFromBackup true)
                                    (.setSqlSchema "MY_META")))
        ))

; 获取 meta CacheConfiguration
(defn get_meta_cache [^Ignite ignite]
    (.getOrCreateCache (add_template ignite) (doto (CacheConfiguration. "my_meta_table")
                                                 (.setSqlSchema "MY_META")))
    )

; 获取 public
(defn get_public_cache [^Ignite ignite]
    (.getOrCreateCache ignite (doto (CacheConfiguration. "public_meta")
                                  (.setSqlSchema "PUBLIC")))
    )

; 创建所有的元表和索引
(defn init-meta-table [^Ignite ignite]
    (run-sql ignite (get-code-lst plus-init/my-grid-tables) (get_meta_cache ignite)))

; 获取 cache
;(defn get-cache
;    [this]
;    (let [ignite (@(.state this) :ignite) template_cfg (CacheConfiguration. "MyMeta_template*")
;          cacheCfg (CacheConfiguration. "my_meta_table")]
;        (.setCacheMode template_cfg (CacheMode/REPLICATED))
;        (.setReadFromBackup template_cfg true)
;        (.setSqlSchema template_cfg "MY_META")
;        (.addCacheConfiguration ignite template_cfg)
;        (.setSqlSchema cacheCfg "MY_META")
;        (.getOrCreateCache ignite cacheCfg)
;        ))

; 初始化元数据表
(defn my-initialization
    [^Ignite ignite]
    (let [cache (get_meta_cache ignite) lst (get-code-lst plus-init/my-grid-tables)]
        (get_public_cache ignite)
        (run-sql ignite lst cache)))

(defn -initialization
    [this]
    (my-initialization (@(.state this) :ignite)))

;(def func-smart #{"println" "first" "rest" "next" "second" "last" "query_sql" "noSqlCreate" "noSqlGet" "noSqlInsertTran" "noSqlUpdateTran" "noSqlDeleteTran"
;                  "noSqlDrop" "noSqlInsert" "noSqlUpdate" "noSqlDelete" "auto_id" "trans" "my_view" "rm_view" "add_scenes_to" "rm_scenes_from"
;                  "add_job" "remove_job" "job_snapshot" "add_func" "remove_func" "recovery_to_cluster" "create_train_matrix" "has_train_matrix"
;                  "drop_train_matrix" "train_matrix" "fit" "predict" "train_matrix_single" "loadCsv" "loadCode"})
;
;(def func-set #{"add" "set" "take" "drop" "nth" "concat" "concat_ws" "contains?" "put" "get" "remove" "pop" "peek"
;                "takeLast" "dropLast" "null?" "notNull?" "empty?" "notEmpty?" "nullOrEmpty?" "notNullOrEmpty?"
;                "str_replace" "str_split" "str_find" "format" "regular" "range" "length" "ucase" "day_name" "to_number"
;                "add_months" "to_date" "last_day" "trunc" "substr" "instrb" "chr" "months_between" "replace" "show_msg"
;                "months_between"
;                "substrb" "lengthb" "tolowercase" "lowercase" "toUpperCase" "uppercase" "show_msg"
;                })
;
;(def db-func-set #{"avg" "count" "max" "min" "sum" "abs" "acos" "asin" "atan" "cos" "cosh" "cot" "sin" "sinh" "tan" "tanh" "atan2"
;                   "bitand" "bitget" "bitor" "bitor" "mod" "pow" "sqrt"
;                   "ceiling" "degrees" "exp"
;                   "floor" "ln" "log" "log10" "pi" "radians" "round" "random_uuid" "roundMagic" "sign" "zero"
;                   "ascii" "char" "concat" "concat_ws" "instr" "lower" "upper" "left" "right" "locate" "position" "soundex" "space" "lpad" "rpad"
;                   "ltrim" "rtrim" "trim" "stringencode" "stringtoutf8" "to_char" "current_date" "current_time" "current_timestamp"
;                   "add_year" "add_quarter" "add_month" "add_date" "add_hour" "add_second" "add_ms"
;                   "diff_year" "diff_quarter" "diff_month" "diff_date" "diff_hour" "diff_second" "diff_ms" "dayname"
;                   "day_of_month" "day_of_week" "day_of_year" "hour" "minute" "monthname" "quarter" "year" "month" "rand" "translate"
;                   "decode" "greatest" "ifnull" "least" "ifnull" "nullif" "nvl2"})
;
;
;(def db-func-no-set #{"SECURE_RAND" "SECURE_RAND" "ENCRYPT" "DECRYPT" "TRUNCATE" "COMPRESS" "EXPAND" "BIT_LENGTH"
;                      "LENGTH" "OCTET_LENGTH" "DIFFERENCE" "HEXTORAW" "RAWTOHEX" "REGEXP_REPLACE" "REGEXP_LIKE" "DATEADD" "DATEDIFF"
;                      "EXTRACT" "FORMATDATETIME" "PARSEDATETIME" "CASEWHEN" "CAST" "CONVERT" "TABLE"})


