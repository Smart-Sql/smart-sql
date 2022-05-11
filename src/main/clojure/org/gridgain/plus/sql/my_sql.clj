(ns org.gridgain.plus.sql.my-sql
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.context.my-context :as my-context]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [org.gridgain.plus.init.plus-init :as plus-init]
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
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MySql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 输入 sql
(defn my_sql [^Ignite ignite ^Long group_id ^String sql]
    (if-not (Strings/isNullOrEmpty sql)
        (let [sql_line (str/trim sql)]
            (cond (re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)insert\s+" sql_line) (my-insert/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ;(re-find #"^(?i)select\s+" sql_line) (my-select/my_plus_sql ignite group_id sql_line)
                  ))
        (throw (Exception. "sql 语句不能为空！"))))



























































