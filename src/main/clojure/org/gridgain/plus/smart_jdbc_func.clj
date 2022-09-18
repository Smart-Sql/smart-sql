(ns org.gridgain.plus.smart-jdbc-func
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-load-smart-sql :as my-load-smart-sql]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.init.plus-init :as plus-init]
        [org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
        [org.gridgain.plus.sql.my-super-sql :as my-super-sql]
        [org.gridgain.plus.tools.my-cache :as my-cache]
        [org.gridgain.plus.tools.my-date-util :as my-date-util]
        [org.gridgain.plus.tools.my-java-util :as my-java-util]
        [org.gridgain.plus.tools.my-util :as my-util]
        [org.gridgain.plus.user.my-user :as my-user]
        [org.gridgain.plus.smart-func :as smart-func]
        [org.gridgain.plus.ml.my-ml-train-data :as my-ml-train-data]
        [org.gridgain.plus.ml.my-ml-func :as my-ml-func]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.sql.jdbc.SmartJdbcFunc
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [invokeAllFuncScenes [org.apache.ignite.Ignite Object String java.util.List] Object]]
        ))

; PreparedStatement 调用所有方法 参数用 ? 来传递，
(defn invoke-all-func-scenes [^Ignite ignite group_id ^String func-name ^List ps]
    (if-not (Strings/isNullOrEmpty func-name)
        (if-let [my-lexical-func (my-lexical/smart-func func-name)]
            (apply (eval (read-string my-lexical-func)) ps)
            (cond (my-lexical/is-eq? "println" func-name) (apply (eval (read-string "my-lexical/my-show-msg")) [(my-lexical/gson (first ps))])
                  (contains? #{"first" "rest" "next" "second" "last"} (str/lower-case func-name)) (apply (eval (read-string (str/lower-case func-name))) ps)
                  (my-lexical/is-eq? func-name "query_sql") (apply (eval (read-string "query_sql")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "empty?") (apply (eval (read-string "empty?")) ps)
                  (my-lexical/is-eq? func-name "noSqlCreate") (apply (eval (read-string "my-lexical/no-sql-create")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlGet") (apply (eval (read-string "my-lexical/no-sql-get-vs")) (concat [ignite group_id] ps))

                  (my-lexical/is-eq? func-name "noSqlInsertTran") (apply (eval (read-string "my-lexical/no-sql-insert-tran")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlUpdateTran") (apply (eval (read-string "my-lexical/no-sql-update-tran")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlDeleteTran") (apply (eval (read-string "my-lexical/no-sql-delete-tran")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlDrop") (apply (eval (read-string "my-lexical/no-sql-drop")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlInsert") (apply (eval (read-string "my-lexical/no-sql-insert")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlUpdate") (apply (eval (read-string "my-lexical/no-sql-update")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "noSqlDelete") (apply (eval (read-string "my-lexical/no-sql-delete")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "auto_id") (apply (eval (read-string "my-lexical/auto_id")) (concat [ignite] ps))

                  (my-lexical/is-eq? func-name "trans") (apply (eval (read-string "my-smart-db/trans")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "my_view") (apply (eval (read-string "smart-func/smart-view")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "rm_view") (apply (eval (read-string "smart-func/rm-smart-view")) (concat [ignite group_id] ps))

                  (my-lexical/is-eq? func-name "add_scenes_to") (apply (eval (read-string "smart-func/add-scenes-to")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "rm_scenes_from") (apply (eval (read-string "smart-func/rm-scenes-from")) (concat [ignite group_id] ps))

                  (my-lexical/is-eq? func-name "add_job") (apply (eval (read-string "smart-func/add-job")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "remove_job") (apply (eval (read-string "smart-func/remove-job")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "job_snapshot") (apply (eval (read-string "smart-func/get-job-snapshot")) (concat [ignite group_id] ps))

                  (my-lexical/is-eq? func-name "add_func") (apply (eval (read-string "smart-func/add_func")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "remove_func") (apply (eval (read-string "smart-func/remove_func")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "recovery_to_cluster") (apply (eval (read-string "smart-func/recovery-to-cluster")) (concat [ignite] ps))

                  ; 机器学习
                  (my-lexical/is-eq? func-name "create_train_matrix") (apply (eval (read-string "my-ml-train-data/create-train-matrix")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "has_train_matrix") (apply (eval (read-string "my-ml-train-data/has-train-matrix")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "drop_train_matrix") (apply (eval (read-string "my-ml-train-data/drop-train-matrix")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "train_matrix") (apply (eval (read-string "my-ml-train-data/train-matrix")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "train_matrix_single") (apply (eval (read-string "my-ml-train-data/train-matrix-single")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "fit") (apply (eval (read-string "my-ml-func/ml-fit")) (concat [ignite group_id] ps))
                  (my-lexical/is-eq? func-name "predict") (apply (eval (read-string "my-ml-func/ml-predict")) (concat [ignite group_id] ps))

                  (my-lexical/is-func? ignite func-name) (if-not (empty? ps)
                                                             (apply (eval (read-string "my-smart-scenes/my-invoke-func")) (concat [ignite (format "%s" func-name)] [ps]))
                                                             (apply (eval (read-string "my-smart-scenes/my-invoke-func-no-ps")) [ignite (format "%s" func-name)]))

                  (my-lexical/is-scenes? ignite group_id func-name) (if-not (empty? ps)
                                                                        (apply (eval (read-string "my-smart-scenes/my-invoke-scenes")) (concat [ignite group_id (format "%s" func-name)] [ps]))
                                                                        (apply (eval (read-string "my-smart-scenes/my-invoke-scenes-no-ps")) [ignite group_id (format "%s" func-name)]))
                  (my-lexical/is-call-scenes? ignite group_id func-name) (if-not (empty? ps)
                                                                             (apply (eval (read-string "my-smart-scenes/my-invoke-scenes")) (concat [ignite group_id (format "%s" func-name)] [ps]))
                                                                             (apply (eval (read-string "my-smart-scenes/my-invoke-scenes-no-ps")) [ignite group_id (format "%s" func-name)]))
                  (my-lexical/is-eq? func-name "loadCode") (apply (eval (read-string "my-load-smart-sql/load-smart-sql")) (concat [ignite group_id] ps))
                  :else
                  (throw (Exception. (format "%s 不存在，或没有权限！" func-name)))
                  ))))

(defn -invokeAllFuncScenes [^Ignite ignite group_id ^String func-name ^List ps]
    (invoke-all-func-scenes ignite group_id func-name ps))











































