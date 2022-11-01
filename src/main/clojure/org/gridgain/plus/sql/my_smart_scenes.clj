(ns org.gridgain.plus.sql.my-smart-scenes
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar)
             (org.tools MyPlusUtil)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartScenes
        ; 是否生成 class 的 main 方法
        :main false
        ; init 构造函数
        :init init
        ; 生成 java 静态的方法
        ;:methods [^:static [invokeScenes [org.apache.ignite.Ignite Long String java.util.List] Object]
        ;          ^:static [myInvokeScenes [org.apache.ignite.Ignite Long Long] Object]
        ;          ^:static [myInit [] void]
        ;          ^:static [myShowCode [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [invokeScenesLink [org.apache.ignite.Ignite Long String java.util.List] Object]]
        :methods [[invokeScenes [org.apache.ignite.Ignite Object String java.util.List] Object]
                  [myInvokeScenes [org.apache.ignite.Ignite Long Long] Object]
                  [myShowCode [org.apache.ignite.Ignite Long String] String]
                  [invokeScenesLink [org.apache.ignite.Ignite Object String java.util.List] Object]]
        ))

; 调用 func
(defn my-invoke-func [^Ignite ignite ^String method-name ps]
    (MyPlusUtil/invokeFuncObj ignite (str/lower-case method-name) (to-array ps)))

(defn my-invoke-func-no-ps [^Ignite ignite ^String method-name]
    (MyPlusUtil/invokeFuncNoPs ignite (str/lower-case method-name)))

; 获取输入参数和处理后的参数
(defn get-params [params]
    (if (> (count params) 0)
        (loop [[f & r] (MyPlusUtil/getParams params) ps-line [] ps-line-ex []]
            (if (some? f)
                (let [ps-name (str (gensym "c"))]
                    (recur r (conj ps-line ps-name) (conj ps-line-ex (MyPlusUtil/getValue ps-name (.getPs_type f)))))
                [(str/join " " ps-line) (str/join " " ps-line-ex)])
            )
        ))

; 获取真实的 clj
(defn get-code-by-scenes [m]
    (let [sql-code (.getSql_code m) scenes_name (.getScenes_name m) [ps-line ps-line-ex] (get-params (.getParams m))]
        [sql-code (format "(defn %s-ex [^Ignite ignite group_id %s]\n    (%s ignite group_id %s))" scenes_name ps-line scenes_name ps-line-ex)]
        ))

(defn scenes-run [^Ignite ignite group_id ^String my-method-name ps]
    (let [m (.get (.cache ignite "my_scenes") (MyScenesCachePk. (first group_id) my-method-name))]
        (if-not (nil? m)
            (let [sql-code (.getSql_code m)]
                (eval (read-string sql-code))
                (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps)))
            )
        (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps))))

(defn get-call-group-id [^Ignite ignite group_id my-method-name]
    (.get (.cache ignite "call_scenes") (MyCallScenesPk. group_id my-method-name)))

; 调用 scenes
(defn my-invoke-scenes [^Ignite ignite group_id ^String method-name ps]
    (let [my-method-name (str/lower-case method-name)]
        (try
            (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps))
            (catch Exception e
                (if-let [my-group (get-call-group-id ignite (first group_id) my-method-name)]
                    (my-invoke-scenes ignite (.getGroup_id_obj my-group) method-name ps)
                    (scenes-run ignite group_id my-method-name ps))
                ))))

(defn my-invoke-scenes-no-ps [^Ignite ignite group_id ^String method-name]
    (my-invoke-scenes ignite group_id method-name []))

;(defn my-invoke-scenes [^Ignite ignite ^Long group_id ^String method-name ps]
;    (let [my-method-name (str/lower-case method-name)]
;        (try
;            (my-lexical/get-value (apply (eval (read-string (format "%s-ex" my-method-name))) ignite group_id ps))
;            (catch Exception e
;                (let [m (.get (.cache ignite "my_scenes") (MyScenesCachePk. group_id my-method-name))]
;                    (let [[sql-code sql-code-ex] (get-code-by-scenes m)]
;                        (eval (read-string sql-code))
;                        (my-lexical/get-value (apply (eval (read-string sql-code-ex)) ignite group_id ps)))
;                    (my-lexical/get-value (apply (eval (read-string (format "%s-ex" my-method-name))) ignite group_id ps)))))))

; 调用 scenes
;(defn my-invoke-scenes-link [^Ignite ignite ^Long group_id ^String method-name & ps]
;    (let [my-method-name (str/lower-case method-name)]
;        (my-lexical/get-value (apply (eval (read-string my-method-name)) ignite group_id ps))))

(defn my-invoke-scenes-link [^Ignite ignite group_id ^String method-code ps]
    (let [[code args] (my-smart-token-clj/func-link-clj ignite group_id (my-select-plus/sql-to-ast (my-lexical/to-back method-code)))]
        (apply (eval (read-string code)) ignite group_id ps)))

; 首先调用方法，如果不存在，在从 cache 中读取数据在执行
(defn -invokeScenes [^Ignite ignite group_id ^String method-name ^List ps]
    (my-invoke-scenes ignite group_id method-name ps))

(defn -invokeScenesLink [^Ignite ignite group_id ^String method-name ^List ps]
    (my-invoke-scenes-link ignite group_id method-name ps))

(defn -myInvokeScenes [this ^Ignite ignite ^Long a ^Long b]
    (my-lexical/get-value (apply (eval (read-string "(defn add [ignite a b]\n    (+ a b))")) [nil a b])))

(defn -myShowCode [this ^Ignite ignite ^Long group_id ^String method-name]
    (do
        ;(import (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk))
        (my-lexical/get-value (apply (eval (read-string "(defn get-code [ignite group_id my-method-name]\n    (let [m (.get (.cache ignite \"my_scenes\") (MyScenesCachePk. group_id my-method-name))]\n        (.getSql_code m)))")) [ignite group_id method-name]))))

(defn -init []
    (do
        (import (org.apache.ignite Ignite IgniteCache)
                (org.apache.ignite.internal IgnitionEx)
                (org.gridgain.smart MyVar)
                (com.google.common.base Strings)
                (org.tools MyConvertUtil MyPlusUtil KvSql MyDbUtil MyGson MyTools MyFunction)
                (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
                (cn.plus.model.ddl MyDataSet MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK MyUpdateViews)
                (org.gridgain.dml.util MyCacheExUtil)
                (cn.plus.model.db MyScenesCache MyScenesParams MyScenesParamsPk MyScenesCachePk)
                (org.apache.ignite.configuration CacheConfiguration)
                (org.apache.ignite.cache CacheMode CacheAtomicityMode)
                (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
                (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
                (java.util List ArrayList Date Iterator Hashtable)
                (java.sql Timestamp)
                (java.math BigDecimal)
                )
        (require
            '[org.gridgain.plus.ddl.my-create-table :as my-create-table]
            '[org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
            '[org.gridgain.plus.ddl.my-create-index :as my-create-index]
            '[org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
            '[org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
            '[org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
            '[org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
            '[org.gridgain.plus.dml.my-delete :as my-delete]
            '[org.gridgain.plus.dml.my-insert :as my-insert]
            '[org.gridgain.plus.dml.my-load-smart-sql :as my-load-smart-sql]
            '[org.gridgain.plus.dml.my-select-plus :as my-select-plus]
            '[org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
            '[org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
            '[org.gridgain.plus.dml.my-smart-db :as my-smart-db]
            '[org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
            '[org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
            '[org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
            '[org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
            '[org.gridgain.plus.dml.my-update :as my-update]
            '[org.gridgain.plus.dml.select-lexical :as my-lexical]
            '[org.gridgain.plus.init.plus-init :as plus-init]
            '[org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
            '[org.gridgain.plus.sql.my-super-sql :as my-super-sql]
            '[org.gridgain.plus.tools.my-cache :as my-cache]
            '[org.gridgain.plus.tools.my-date-util :as my-date-util]
            '[org.gridgain.plus.tools.my-java-util :as my-java-util]
            '[org.gridgain.plus.tools.my-util :as my-util]
            '[org.gridgain.plus.user.my-user :as my-user]
            '[org.gridgain.plus.smart-func :as smart-func]
            '[org.gridgain.plus.ml.my-ml-train-data :as my-ml-train-data]
            '[org.gridgain.plus.ml.my-ml-func :as my-ml-func]
            '[clojure.core.reducers :as r]
            '[clojure.string :as str]
            '[clojure.walk :as w]
            )
        ))











































