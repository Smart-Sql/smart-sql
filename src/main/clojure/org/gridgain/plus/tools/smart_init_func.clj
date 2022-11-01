(ns org.gridgain.plus.tools.smart-init-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.smart-func :as smart-func]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.gridgain.smart MyVar)
             (cn.plus.model.db MyCallScenesPk MyCallScenes)
             (cn.plus.model MyUsersGroup MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
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
              :else (let [rs (MyVar. (my-smart-db/query_sql ignite group_id "select g.id, g.schema_name, g.group_type from my_users_group as g where g.user_token = ?" [(my-lexical/to_arryList [(my-lexical/get-value user_token)])])) result (MyVar. )]
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
    (my-lexical/get-value (get_user_group ignite group_id user_token)))

(defn get_user_token [ignite group_name]
    (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.id, m.user_token from my_users_group m where m.group_name = ?") (to-array [group_name]))))))

(defn -getUserToken [this ^Ignite ignite ^String group_name]
    (get_user_token ignite group_name))

(defn to-lst [lst]
    (loop [[f & r] lst rs (ArrayList.)]
        (if (some? f)
            (recur r (doto rs (.add f)))
            rs)))

(defn has-schema-name [ignite schema_name]
    (if (= "SYS" (str/upper-case schema_name))
        (throw (Exception. (format "不能在 %s 中添加用户组！" schema_name)))
        (let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "SELECT m.SCHEMA_NAME FROM sys.SCHEMAS m WHERE m.SCHEMA_NAME = ?") (to-array [(str/upper-case schema_name)])))))]
            (if (and (not (nil? m)) (not (empty? m)))
                true
                (throw (Exception. (format "不存在 schema %s ! 请仔细检查是否拼写正确！" schema_name)))))))

(defn add_user_group [ignite group_id group_name user_token group_type schema_name]
    (cond (has_user_token_type group_type)
          (if (true? (has-schema-name ignite schema_name))
              (cond (my-lexical/not-null-or-empty? (my-lexical/get-value (get_user_group ignite group_id (my-lexical/get-value user_token)))) "已经存在了该 user_token 不能重复插入！"
                    :else (let [user_group_id (my-lexical/auto_id ignite group_id "my_users_group")]
                              (let [my-call-scenes-pk (doto (MyCallScenesPk. user_group_id "get_user_group")) my-call-scenes (doto (MyCallScenes. 0 user_group_id "get_user_group")) my-users-group (doto (MyUsersGroup.) (.setId user_group_id) (.setGroup_name group_name) (.setSchema_name schema_name) (.setUser_token user_token) (.setGroup_type group_type))]
                                  (let [lst (doto (ArrayList.) (.add (MyLogCache. "my_users_group" user_group_id my-users-group (SqlType/INSERT))) (.add (MyLogCache. "call_scenes" my-call-scenes-pk my-call-scenes (SqlType/INSERT))))]
                                      ;(MyCacheExUtil/transLogCache ignite lst)
                                      (doto lst (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.my_caches where schema_name = '%s'" (my-lexical/get-value schema_name)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.call_scenes where to_group_id = %s" (my-lexical/get-value user_group_id)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.my_cron where group_id = %s" (my-lexical/get-value user_group_id)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.my_delete_views where group_id = %s" (my-lexical/get-value user_group_id)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.my_insert_views where group_id = %s" (my-lexical/get-value user_group_id)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.my_update_views where group_id = %s" (my-lexical/get-value user_group_id)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "select * from my_meta.my_select_views where group_id = %s" (my-lexical/get-value user_group_id)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "select id, group_name, schema_name, group_type from my_meta.my_users_group")))

                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.BASELINE_NODES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "SELECT convert_to(cache_name, show_cache_name(cache_name)), CACHE_ID, CACHE_TYPE, CACHE_MODE, ATOMICITY_MODE, CACHE_GROUP_NAME, AFFINITY, AFFINITY_MAPPER,BACKUPS, CACHE_GROUP_ID, CACHE_LOADER_FACTORY, CACHE_STORE_FACTORY, CACHE_WRITER_FACTORY, DATA_REGION_NAME, DEFAULT_LOCK_TIMEOUT, EVICTION_FILTER,EVICTION_POLICY_FACTORY, EXPIRY_POLICY_FACTORY, INTERCEPTOR, IS_COPY_ON_READ, IS_EAGER_TTL, IS_ENCRYPTION_ENABLED, IS_EVENTS_DISABLED, IS_INVALIDATE,IS_LOAD_PREVIOUS_VALUE, IS_MANAGEMENT_ENABLED, IS_NEAR_CACHE_ENABLED, IS_ONHEAP_CACHE_ENABLED, IS_READ_FROM_BACKUP, IS_READ_THROUGH, IS_SQL_ESCAPE_ALL,IS_SQL_ONHEAP_CACHE_ENABLED, IS_STATISTICS_ENABLED, IS_STORE_KEEP_BINARY, IS_WRITE_BEHIND_ENABLED, IS_WRITE_THROUGH, MAX_QUERY_ITERATORS_COUNT, NEAR_CACHE_EVICTION_POLICY_FACTORY,NEAR_CACHE_START_SIZE, NODE_FILTER, PARTITION_LOSS_POLICY, QUERY_DETAIL_METRICS_SIZE, QUERY_PARALLELISM, REBALANCE_BATCH_SIZE, REBALANCE_BATCHES_PREFETCH_COUNT, REBALANCE_DELAY,REBALANCE_MODE, REBALANCE_ORDER, REBALANCE_THROTTLE, REBALANCE_TIMEOUT, SQL_INDEX_MAX_INLINE_SIZE, SQL_ONHEAP_CACHE_MAX_SIZE, SQL_SCHEMA, TOPOLOGY_VALIDATOR, WRITE_BEHIND_BATCH_SIZE, WRITE_BEHIND_COALESCING, WRITE_BEHIND_FLUSH_FREQUENCY, WRITE_BEHIND_FLUSH_SIZE, WRITE_BEHIND_FLUSH_THREAD_COUNT, WRITE_SYNCHRONIZATION_MODE FROM sys.CACHES WHERE SQL_SCHEMA IN ('%s', 'PUBLIC') AND cache_name <> '%s_meta' and cache_name <> 'public_meta'" (str/upper-case schema_name) (str/lower-case schema_name)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.CACHE_GROUPS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.CACHE_GROUP_PAGE_LISTS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.CLIENT_CONNECTIONS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.CONTINUOUS_QUERIES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DATASTREAM_THREADPOOL_QUEUE WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DATA_REGION_PAGE_LISTS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_ATOMICLONGS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.ds_atomicreferences WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.ds_atomicsequences WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_ATOMICSTAMPED WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_COUNTDOWNLATCHES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_QUEUES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_REENTRANTLOCKS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_SEMAPHORES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.DS_SETS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "SELECT * FROM sys.INDEXES WHERE SCHEMA_NAME in ('%s', 'PUBLIC')" (str/upper-case schema_name)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.JOBS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.LOCAL_CACHE_GROUPS_IO WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.METRICS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.NODES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.NODE_ATTRIBUTES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.NODE_METRICS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.SCAN_QUERIES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.SCHEMAS WHERE true")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.SQL_QUERIES WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.SQL_QUERIES_HISTORY WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.STATISTICS_CONFIGURATION WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.STATISTICS_LOCAL_DATA WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.STATISTICS_PARTITION_DATA WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.STRIPED_THREADPOOL_QUEUE WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "SELECT CACHE_GROUP_ID, convert_to(CACHE_GROUP_NAME, show_cache_name(CACHE_GROUP_NAME)), CACHE_ID, convert_to(CACHE_NAME, show_cache_name(CACHE_NAME)), SCHEMA_NAME, TABLE_NAME, AFFINITY_KEY_COLUMN, KEY_ALIAS, VALUE_ALIAS, KEY_TYPE_NAME, VALUE_TYPE_NAME, IS_INDEX_REBUILD_IN_PROGRESS FROM sys.TABLES WHERE SCHEMA_NAME in ('%s', 'PUBLIC')" (str/upper-case schema_name)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id (format "SELECT * FROM sys.TABLE_COLUMNS WHERE SCHEMA_NAME in ('%s', 'PUBLIC')" (str/upper-case schema_name)))))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.TASKS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.TRANSACTIONS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.VIEWS WHERE FALSE")))
                                                (.addAll (to-lst (smart-func/smart-view-tran ignite group_id group_name user_group_id "SELECT * FROM sys.VIEW_COLUMNS WHERE FALSE")))
                                                )
                                      (MyCacheExUtil/transLogCache ignite lst)))
                              )))))

(defn -addUserGroup [this ^Ignite ignite ^Object group_id ^String group_name ^String user_token ^String group_type ^String schema_name]
    (add_user_group ignite group_id group_name user_token group_type schema_name))

(defn update-cache [ignite group_id group_name group_type]
    (if-let [ids (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.id from my_users_group m where m.group_name = ?") (to-array [group_name])))))]
        (let [vs (.get (.cache ignite "my_users_group") (first ids))]
            (MyLogCache. "my_users_group" (first ids) (doto vs (.setGroup_type group_type)) (SqlType/UPDATE)))))

(defn update_user_group [ignite group_id group_name group_type]
    (let [user_token (MyVar. (get_user_token ignite (my-lexical/get-value group_name)))]
        (let [vs (MyVar. (my-lexical/no-sql-get-vs ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (last (my-lexical/get-value user_token))))))]
            (cond (nil? (my-lexical/get-value vs)) (if-let [ids (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.id from my_users_group m where m.group_name = ?") (to-array [group_name])))))]
                                                       (let [vs (.get (.cache ignite "my_users_group") (first ids))]
                                                           (.replace (.cache ignite "my_users_group") (first ids) (doto vs (.setGroup_type group_type)))))
                  :else (let [new_vs (MyVar. (my-lexical/list-set (my-lexical/get-value vs) 2 (my-lexical/get-value group_type))) lst (MyVar. (my-lexical/to_arryList [(update-cache ignite group_id group_name group_type)]))]
                            (do
                                (my-lexical/list-add (my-lexical/get-value lst) (my-lexical/no-sql-update-tran ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (last (my-lexical/get-value user_token))) (.put "value" (my-lexical/get-value new_vs)))))
                                (MyCacheExUtil/transLogCache ignite (my-lexical/get-value lst))))))))

(defn -updateUserGroup [this ^Ignite ignite ^Object group_id ^String group_name ^String group_type]
    (update_user_group ignite group_id group_name group_type))

(defn update-cache-group-name [ignite group_name]
    (if-let [ids (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.id from my_users_group m where m.group_name = ?") (to-array [group_name])))))]
        (MyLogCache. "my_users_group" (first ids) nil (SqlType/DELETE))))

(defn call_scenes-cache [ignite to_group_id]
    (if-let [ids (first (.getAll (.query (.cache ignite "call_scenes") (.setArgs (SqlFieldsQuery. "select m.group_id, m.scenes_name from call_scenes m where m.to_group_id = ?") (to-array [to_group_id])))))]
        (MyLogCache. "call_scenes" (MyCallScenesPk. (first ids) (second ids)) nil (SqlType/DELETE))))

(defn delete_user_group [ignite group_id group_name]
    (let [user_token (MyVar. (get_user_token ignite (my-lexical/get-value group_name)))]
        (let [my-user-group-cache (update-cache-group-name ignite group_name) call-cache (call_scenes-cache ignite (first (my-lexical/get-value user_token)))]
            (MyCacheExUtil/transLogCache ignite (doto (ArrayList.)
                                                   (.add my-user-group-cache)
                                                   (.add call-cache)
                                                   (.add (my-lexical/no-sql-delete-tran ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (last (my-lexical/get-value user_token))))))
                                                   (.addAll (doto (ArrayList.)
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_caches" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.call_scenes" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_cron" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_delete_views" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_insert_views" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_update_views" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_select_views" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "my_meta.my_users_group" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.BASELINE_NODES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.CACHES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.CACHE_GROUPS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.CACHE_GROUP_PAGE_LISTS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.CLIENT_CONNECTIONS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.CONTINUOUS_QUERIES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DATASTREAM_THREADPOOL_QUEUE" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DATA_REGION_PAGE_LISTS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_ATOMICLONGS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_ATOMICREFERENCES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_ATOMICSEQUENCES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_ATOMICSTAMPED" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_COUNTDOWNLATCHES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_QUEUES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_REENTRANTLOCKS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_SEMAPHORES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.DS_SETS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.INDEXES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.JOBS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.LOCAL_CACHE_GROUPS_IO" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.METRICS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.NODES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.NODE_ATTRIBUTES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.NODE_METRICS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.SCAN_QUERIES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.SCHEMAS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.SQL_QUERIES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.SQL_QUERIES_HISTORY" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.STATISTICS_CONFIGURATION" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.STATISTICS_LOCAL_DATA" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.STATISTICS_PARTITION_DATA" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.STRIPED_THREADPOOL_QUEUE" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.TABLES" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.TABLE_COLUMNS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.TASKS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.TRANSACTIONS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.VIEWS" "select")))
                                                                (.addAll (to-lst (smart-func/rm-smart-view-tran ignite group_id group_name "sys.VIEW_COLUMNS" "select")))
                                                                )))))))

(defn -deleteUserGroup [this ^Ignite ignite ^Object group_id ^String group_name]
    (delete_user_group ignite group_id group_name))