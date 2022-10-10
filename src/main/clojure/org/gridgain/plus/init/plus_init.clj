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


