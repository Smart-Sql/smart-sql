(ns org.gridgain.plus.tools.smart-init-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.smart-func :as smart-func]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar)
             (java.util ArrayList List Hashtable Date Iterator))
    (:gen-class
        :implements [org.gridgain.superservice.ISmartFuncInit]
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MySmartInitFunc
        ; 是否生成 class 的 main 方法
        :main false
        ))

; 判断是否存在 DDL, DML, ALL 这三个字符
(defn has_user_token_type [user_token_type]
    (let [flag (MyVar. false) lst (MyVar. (my-lexical/to_arryList ["ddl" "dml" "all"]))]
        (do
            (cond (my-lexical/is-contains? (my-lexical/get-value lst) (str/lower-case (my-lexical/get-value user_token_type))) (.setVar flag true))
            (my-lexical/get-value flag))))

(defn -hasUserTokenType [this ^String user_token_type]
    (has_user_token_type user_token_type))

; 创建查询用户组的方法 get_user_group
; 首先、查询缓存，如果缓存中存在，则返回
; 如果没有查询到，就查询数据库并将查询的结果保存到，缓存中
(defn get_user_group [ignite group_id user_token]
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
                            result)))))

(defn -getUserGroup [this ^Ignite ignite ^Object group_id ^String user_token]
    (get_user_group ignite group_id user_token))

(defn get_user_token [ignite group_id group_name]
    (let [group_token (MyVar. )]
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

(defn -getUserToken [this ^Ignite ignite ^Object group_id ^String group_name]
    (get_user_token ignite group_id group_name))

(defn add_user_group [ignite group_id group_name user_token group_type data_set_name]
    (cond (has_user_token_type group_type)
          (cond (my-lexical/not-null-or-empty? (get_user_group ignite group_id (my-lexical/get-value user_token))) "已经存在了该 user_token 不能重复插入！"
                :else (let [user_group_id (MyVar. (my-lexical/auto_id ignite group_id "my_users_group"))]
                          (let [lst (MyVar. (my-lexical/to_arryList [(my-lexical/to_arryList ["insert into my_users_group (id, group_name, data_set_name, user_token, group_type) values (?, ?, ?, ?, ?)" (my-lexical/to_arryList [(my-lexical/get-value user_group_id) group_name (my-lexical/get-value data_set_name) (my-lexical/get-value user_token) (my-lexical/get-value group_type)])])]))]
                              (do
                                  (my-lexical/list-add (my-lexical/get-value lst) (my-lexical/to_arryList ["insert into call_scenes (group_id, to_group_id, scenes_name) values (?, ?, ?)" (my-lexical/to_arryList [0 (my-lexical/get-value user_group_id) "get_user_group"])]))
                                  (my-smart-db/trans ignite group_id (my-lexical/get-value lst))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.my_caches where dataset_name = '%s'" (my-lexical/get-value data_set_name)))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.call_scenes where to_group_id = %s" (my-lexical/get-value user_group_id)))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.my_cron where group_id = '%s'" (my-lexical/get-value user_group_id)))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.my_delete_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.my_insert_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.my_update_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                  (smart-func/smart-view ignite group_id group_name (format "select * from my_meta.my_select_views where group_id = %s" (my-lexical/get-value user_group_id)))
                                  (smart-func/smart-view ignite group_id group_name "select id, group_name, data_set_name, group_type from my_meta.my_users_group")
                                  ; sys 的权限
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.BASELINE_NODES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name (format "SELECT convert_to(cache_name, show_cache_name(cache_name)), CACHE_ID, CACHE_TYPE, CACHE_MODE, ATOMICITY_MODE, CACHE_GROUP_NAME, AFFINITY, AFFINITY_MAPPER,BACKUPS, CACHE_GROUP_ID, CACHE_LOADER_FACTORY, CACHE_STORE_FACTORY, CACHE_WRITER_FACTORY, DATA_REGION_NAME, DEFAULT_LOCK_TIMEOUT, EVICTION_FILTER,EVICTION_POLICY_FACTORY, EXPIRY_POLICY_FACTORY, INTERCEPTOR, IS_COPY_ON_READ, IS_EAGER_TTL, IS_ENCRYPTION_ENABLED, IS_EVENTS_DISABLED, IS_INVALIDATE,IS_LOAD_PREVIOUS_VALUE, IS_MANAGEMENT_ENABLED, IS_NEAR_CACHE_ENABLED, IS_ONHEAP_CACHE_ENABLED, IS_READ_FROM_BACKUP, IS_READ_THROUGH, IS_SQL_ESCAPE_ALL,IS_SQL_ONHEAP_CACHE_ENABLED, IS_STATISTICS_ENABLED, IS_STORE_KEEP_BINARY, IS_WRITE_BEHIND_ENABLED, IS_WRITE_THROUGH, MAX_QUERY_ITERATORS_COUNT, NEAR_CACHE_EVICTION_POLICY_FACTORY,NEAR_CACHE_START_SIZE, NODE_FILTER, PARTITION_LOSS_POLICY, QUERY_DETAIL_METRICS_SIZE, QUERY_PARALLELISM, REBALANCE_BATCH_SIZE, REBALANCE_BATCHES_PREFETCH_COUNT, REBALANCE_DELAY,REBALANCE_MODE, REBALANCE_ORDER, REBALANCE_THROTTLE, REBALANCE_TIMEOUT, SQL_INDEX_MAX_INLINE_SIZE, SQL_ONHEAP_CACHE_MAX_SIZE, SQL_SCHEMA, TOPOLOGY_VALIDATOR, WRITE_BEHIND_BATCH_SIZE, WRITE_BEHIND_COALESCING, WRITE_BEHIND_FLUSH_FREQUENCY, WRITE_BEHIND_FLUSH_SIZE, WRITE_BEHIND_FLUSH_THREAD_COUNT, WRITE_SYNCHRONIZATION_MODE FROM sys.CACHES WHERE SQL_SCHEMA IN ('%s', 'PUBLIC') AND cache_name <> '%s_meta'" (str/upper-case data_set_name) (str/lower-case data_set_name)))
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.CACHE_GROUPS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.CACHE_GROUP_PAGE_LISTS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.CLIENT_CONNECTIONS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.CONTINUOUS_QUERIES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DATASTREAM_THREADPOOL_QUEUE WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DATA_REGION_PAGE_LISTS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_ATOMICLONGS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.ds_atomicreferences WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.ds_atomicsequences WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_ATOMICSTAMPED WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_COUNTDOWNLATCHES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_QUEUES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_REENTRANTLOCKS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_SEMAPHORES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.DS_SETS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.INDEXES WHERE SCHEMA_NAME in ('MYY', 'PUBLIC')")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.JOBS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.LOCAL_CACHE_GROUPS_IO WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.METRICS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.NODES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.NODE_ATTRIBUTES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.NODE_METRICS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.SCAN_QUERIES WHERE FALSE")

                                  ;(smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.SCHEMAS WHERE true")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.SQL_QUERIES WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.SQL_QUERIES_HISTORY WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.STATISTICS_CONFIGURATION WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.STATISTICS_LOCAL_DATA WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.STATISTICS_PARTITION_DATA WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.STRIPED_THREADPOOL_QUEUE WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name (format "SELECT CACHE_GROUP_ID, convert_to(CACHE_GROUP_NAME, show_cache_name(CACHE_GROUP_NAME)), CACHE_ID, convert_to(CACHE_NAME, show_cache_name(CACHE_NAME)), SCHEMA_NAME, TABLE_NAME, AFFINITY_KEY_COLUMN, KEY_ALIAS, VALUE_ALIAS, KEY_TYPE_NAME, VALUE_TYPE_NAME, IS_INDEX_REBUILD_IN_PROGRESS FROM sys.TABLES WHERE SCHEMA_NAME in ('%s', 'PUBLIC')" (str/upper-case data_set_name)))
                                  (smart-func/smart-view ignite group_id group_name (format "SELECT * FROM sys.TABLE_COLUMNS WHERE SCHEMA_NAME in ('%s', 'PUBLIC')" (str/upper-case data_set_name)))
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.TASKS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.TRANSACTIONS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.VIEWS WHERE FALSE")
                                  (smart-func/smart-view ignite group_id group_name "SELECT * FROM sys.VIEW_COLUMNS WHERE FALSE")
                                  ))))))

(defn -addUserGroup [this ^Ignite ignite ^Object group_id ^String group_name ^String user_token ^String group_type ^String data_set_name]
    (add_user_group ignite group_id group_name user_token group_type data_set_name))

(defn update_user_group [ignite group_id group_name group_type]
    (let [user_token (MyVar. (get_user_token ignite group_id (my-lexical/get-value group_name)))]
        (let [vs (MyVar. (my-lexical/no-sql-get-vs ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (last (my-lexical/get-value user_token))))))]
            (cond (nil? (my-lexical/get-value vs)) (my-smart-db/query_sql ignite group_id "update my_users_group set group_type = ? where group_name = ?" [(my-lexical/to_arryList [(my-lexical/get-value group_type) (my-lexical/get-value group_name)])])
                  :else (let [new_vs (MyVar. (my-lexical/list-set (my-lexical/get-value vs) 2 (my-lexical/get-value group_type))) lst (MyVar. (my-lexical/to_arryList [(my-lexical/to_arryList ["update my_users_group set group_type = ? where group_name = ?" (my-lexical/to_arryList [(my-lexical/get-value group_type) (my-lexical/get-value group_name)])])]))]
                            (do
                                (my-lexical/list-add (my-lexical/get-value lst) (my-lexical/to_arryList [(my-lexical/no-sql-update-tran ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (last (my-lexical/get-value user_token))) (.put "value" (my-lexical/get-value new_vs))))]))
                                (my-smart-db/trans ignite group_id (my-lexical/get-value lst))))))))

(defn -updateUserGroup [this ^Ignite ignite ^Object group_id ^String group_name ^String group_type]
    (update_user_group ignite group_id group_name group_type))

(defn delete_user_group [ignite group_id group_name]
    (let [user_token (MyVar. (get_user_token ignite group_id (my-lexical/get-value group_name)))]
        (do
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_caches" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.call_scenes" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_cron" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_delete_views" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_insert_views" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_update_views" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_select_views" "select")
            (smart-func/rm-smart-view ignite group_id group_name "my_meta.my_users_group" "select")

            ; sys
            (smart-func/rm-smart-view ignite group_id group_name "sys.BASELINE_NODES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.CACHES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.CACHE_GROUPS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.CACHE_GROUP_PAGE_LISTS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.CLIENT_CONNECTIONS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.CONTINUOUS_QUERIES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DATASTREAM_THREADPOOL_QUEUE" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DATA_REGION_PAGE_LISTS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_ATOMICLONGS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_ATOMICREFERENCES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_ATOMICSEQUENCES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_ATOMICSTAMPED" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_COUNTDOWNLATCHES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_QUEUES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_REENTRANTLOCKS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_SEMAPHORES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.DS_SETS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.INDEXES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.JOBS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.LOCAL_CACHE_GROUPS_IO" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.METRICS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.NODES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.NODE_ATTRIBUTES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.NODE_METRICS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.SCAN_QUERIES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.SCHEMAS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.SQL_QUERIES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.SQL_QUERIES_HISTORY" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.STATISTICS_CONFIGURATION" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.STATISTICS_LOCAL_DATA" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.STATISTICS_PARTITION_DATA" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.STRIPED_THREADPOOL_QUEUE" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.TABLES" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.TABLE_COLUMNS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.TASKS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.TRANSACTIONS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.VIEWS" "select")
            (smart-func/rm-smart-view ignite group_id group_name "sys.VIEW_COLUMNS" "select")

            (let [lst (MyVar. (my-lexical/to_arryList [(my-lexical/to_arryList ["delete from my_users_group where group_name = ?" (my-lexical/to_arryList [(my-lexical/get-value group_name)])])]))]
                (my-lexical/list-add (my-lexical/get-value lst) (my-lexical/to_arryList ["delete from call_scenes where to_group_id = ?" (my-lexical/to_arryList [(first (my-lexical/get-value user_token))])]))
                (my-lexical/list-add (my-lexical/get-value lst) (my-lexical/no-sql-delete-tran ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (last (my-lexical/get-value user_token))))))
                (my-smart-db/trans ignite group_id (my-lexical/get-value lst))))))

(defn -deleteUserGroup [this ^Ignite ignite ^Object group_id ^String group_name]
    (delete_user_group ignite group_id group_name))