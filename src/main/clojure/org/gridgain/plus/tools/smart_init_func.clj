(ns org.gridgain.plus.tools.smart-init-func
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.my-smart-clj :as my-smart-clj]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-db-line :as my-smart-db-line]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.tools.my-user-group :as my-user-group]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
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
             (java.util ArrayList List Hashtable Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MySmartInitFunc
        ; 是否生成 class 的 main 方法
        :main false
        ))

(defn has_user_token_type [ignite group_id c_user_token_type_f1700]
    (let [user_token_type (MyVar. (MyConvertUtil/ConvertToString c_user_token_type_f1700)) flag (MyVar. false) lst (MyVar. (my-lexical/to_arryList ["ddl" "dml" "all"]))]
        (do
            (cond (my-lexical/is-contains? (my-lexical/get-value lst) (str/lower-case (my-lexical/get-value user_token_type))) (.setVar flag true))
            (my-lexical/get-value flag))))


(defn get_user_group [ignite group_id c_user_token_f1722]
    (let [user_token (MyVar. (MyConvertUtil/ConvertToString c_user_token_f1722))]
        (let [vs (MyVar. (my-lexical/no-sql-get-vs ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (my-lexical/get-value user_token)))))]
            (cond (my-lexical/not-empty? (my-lexical/get-value vs)) (my-lexical/get-value vs)
                  :else (let [rs (MyVar. (my-smart-db/query_sql ignite group_id "select g.id, g.data_set_name, g.group_type from my_users_group as g where g.user_token = ?" [(my-lexical/to_arryList [(my-lexical/get-value user_token)])])) result (MyVar. )]
                            (do
                                (cond (my-lexical/my-is-iter? rs) (try
                                                                      (loop [M-F-v1725-I-Q1726-c-Y (my-lexical/get-my-iter rs)]
                                                                          (if (.hasNext M-F-v1725-I-Q1726-c-Y)
                                                                              (let [r (.next M-F-v1725-I-Q1726-c-Y)]
                                                                                  (my-lexical/no-sql-insert ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (my-lexical/get-value user_token)) (.put "value" r)))
                                                                                  (.setVar result r)
                                                                                  (recur M-F-v1725-I-Q1726-c-Y))))
                                                                      (catch Exception e
                                                                          (if-not (= (.getMessage e) "my-break")
                                                                              (throw e))))
                                      (my-lexical/my-is-seq? rs) (try
                                                                     (doseq [r (my-lexical/get-my-seq rs)]
                                                                         (my-lexical/no-sql-insert ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (my-lexical/get-value user_token)) (.put "value" r)))
                                                                         (.setVar result r))
                                                                     (catch Exception e
                                                                         (if-not (= (.getMessage e) "my-break")
                                                                             (throw e))))
                                      :else
                                      (throw (Exception. "for 循环只能处理列表或者是执行数据库的结果")))
                                result))))))


(defn get_user_token [ignite group_id c_group_name_f1759]
    (let [group_name (MyVar. (MyConvertUtil/ConvertToString c_group_name_f1759)) group_token (MyVar. )]
        (let [rs (MyVar. (my-smart-db/query_sql ignite group_id "select m.id, m.user_token from my_users_group m where m.group_name = ?" [(my-lexical/to_arryList [(my-lexical/get-value group_name)])]))]
            (do
                (cond (my-lexical/my-is-iter? rs) (try
                                                      (loop [M-F-v1762-I-Q1763-c-Y (my-lexical/get-my-iter rs)]
                                                          (if (.hasNext M-F-v1762-I-Q1763-c-Y)
                                                              (let [r (.next M-F-v1762-I-Q1763-c-Y)]
                                                                  (.setVar group_token r)
                                                                  (recur M-F-v1762-I-Q1763-c-Y))))
                                                      (catch Exception e
                                                          (if-not (= (.getMessage e) "my-break")
                                                              (throw e))))
                      (my-lexical/my-is-seq? rs) (try
                                                     (doseq [r (my-lexical/get-my-seq rs)]
                                                         (.setVar group_token r))
                                                     (catch Exception e
                                                         (if-not (= (.getMessage e) "my-break")
                                                             (throw e))))
                      :else
                      (throw (Exception. "for 循环只能处理列表或者是执行数据库的结果")))
                group_token))))

(defn add_user_group [ignite group_id c_group_name_f1766 c_user_token_f1767 c_group_type_f1768 c_data_set_name_f1769]
    (let [group_name (MyVar. (MyConvertUtil/ConvertToString c_group_name_f1766)) user_token (MyVar. (MyConvertUtil/ConvertToString c_user_token_f1767)) group_type (MyVar. (MyConvertUtil/ConvertToString c_group_type_f1768)) data_set_name (MyVar. (MyConvertUtil/ConvertToString c_data_set_name_f1769))]
        (cond (my-smart-scenes/my-invoke-scenes ignite group_id "has_user_token_type" [(my-lexical/get-value group_type)]) (cond (my-lexical/not-null-or-empty? (my-smart-scenes/my-invoke-scenes ignite group_id "get_user_group" [(my-lexical/get-value user_token)])) "已经存在了该 user_token 不能重复插入！"
                                                                                                                                 :else (let [user_group_id (MyVar. (my-lexical/auto_id ignite group_id "my_users_group"))]
                                                                                                                                           (let [lst (MyVar. (my-lexical/to_arryList [(my-lexical/to_arryList ["insert into my_users_group (id, group_name, data_set_name, user_token, group_type) values (?, ?, ?, ?, ?)" (my-lexical/to_arryList [(my-lexical/get-value user_group_id) (my-lexical/get-value group_name) (my-lexical/get-value data_set_name) (my-lexical/get-value user_token) (my-lexical/get-value group_type)])])]))]
                                                                                                                                               (do
                                                                                                                                                   (my-lexical/list-add (my-lexical/get-value lst) (my-lexical/to_arryList ["insert into call_scenes (group_id, to_group_id, scenes_name) values (?, ?, ?)" (my-lexical/to_arryList [0 (my-lexical/get-value user_group_id) "get_user_group"])]))
                                                                                                                                                   (my-smart-db/trans ignite group_id (my-lexical/get-value lst))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.my_caches where dataset_name = '%s'" (my-lexical/get-value data_set_name)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.call_scenes where to_group_id = %s" (my-lexical/get-value user_group_id)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.my_cron where group_id = '%s'" (my-lexical/get-value user_group_id)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.my_delete_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.my_insert_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.my_update_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) (format "select * from my_meta.my_select_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                                                                                                                                   (smart-func/smart-view ignite group_id (my-lexical/get-value group_name) "select id, group_name, data_set_name, group_type from my_meta.my_users_group"))))))))