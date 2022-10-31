(ns org.gridgain.plus.tools.my-user-group
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.apache.ignite.transactions Transaction)
             (com.google.gson Gson GsonBuilder)
             (org.tools MyConvertUtil KvSql)
             (cn.plus.model MyLogCache SqlType)
             (cn.mysuper.model MyUrlToken)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache MyScenesCachePk MyScenesParams)
             (org.gridgain.myservice MyNoSqlFunService)
             (org.gridgain.jdbc MyJdbc)
             (org.gridgain.smart.view MyViewAstPK)
             (java.util ArrayList Date Iterator Hashtable)
             (java.sql Timestamp)
             (org.tools MyTools MyFunction)
             (java.math BigDecimal)
             (com.google.common.base Strings))
    (:gen-class
        ;:implements [org.gridgain.superservice.INoSqlFun]
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyUserGroup
        ; 是否生成 class 的 main 方法
        :main false
        :methods [^:static [getUserGroup [org.apache.ignite.Ignite String] Object]
                  ^:static [getUserGroupById [org.apache.ignite.Ignite Long] Object]]
        ))


; 通过 user_token 获取 user-group 的列表对象
;(defn get_user_group [ignite user_token]
;    (if (my-lexical/is-eq? user_token (.getRoot_token (.configuration ignite)))
;        [0 "MY_META" "all"]
;        (let [group_id [0 "MY_META" "all"]]
;            (let [vs (my-lexical/no-sql-get-vs ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache")(.put "key" user_token)))]
;                (cond (my-lexical/not-empty? vs) vs
;                      :else (let [rs (my-smart-db/query_sql ignite group_id "select g.id, g.schema_name, g.group_type from my_users_group as g where g.user_token = ?" [(my-lexical/to_arryList [user_token])])]
;                                (loop [M-F-v156-I-Q157-c-Y (my-lexical/get-my-iter rs)]
;                                    (if (.hasNext M-F-v156-I-Q157-c-Y)
;                                        (let [r (.next M-F-v156-I-Q157-c-Y)]
;                                            (my-lexical/no-sql-insert ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache")(.put "key" user_token)(.put "value" r)))
;                                            r
;                                            (recur M-F-v156-I-Q157-c-Y))))))))))

(defn get_user_group [ignite user_token]
    (if (my-lexical/is-eq? user_token (.getRoot_token (.configuration ignite)))
        [0 "MY_META" "all"]
        (let [group_id [0 "MY_META" "all"]]
            (let [vs (MyVar. (my-lexical/no-sql-get-vs ignite group_id (doto (Hashtable.)
                                                                           (.put "table_name" "user_group_cache")
                                                                           (.put "key" (my-lexical/get-value user_token)))))]
                (cond (my-lexical/not-empty? (my-lexical/get-value vs)) (my-lexical/get-value vs)
                      :else (let [rs (MyVar. (my-smart-db/query_sql ignite group_id "select g.id, g.schema_name, g.group_type from my_users_group as g where g.user_token = ?" [(my-lexical/to_arryList [(my-lexical/get-value user_token)])])) result (MyVar. )]
                                (do
                                    (cond (my-lexical/my-is-iter? rs) (try
                                                                          (loop [M-F-v1625-I-Q1626-c-Y (my-lexical/get-my-iter rs)]
                                                                              (if (.hasNext M-F-v1625-I-Q1626-c-Y)
                                                                                  (let [r (.next M-F-v1625-I-Q1626-c-Y)]
                                                                                      (my-lexical/no-sql-insert ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache") (.put "key" (my-lexical/get-value user_token)) (.put "value" r)))
                                                                                      (.setVar result r)
                                                                                      (recur M-F-v1625-I-Q1626-c-Y))))
                                                                          (catch Exception e
                                                                              (if-not (= (.getMessage e) "my-break")
                                                                                  (throw e))))
                                          (my-lexical/my-is-seq? rs) (try
                                                                         (doseq [r (my-lexical/get-my-seq rs)]
                                                                             (my-lexical/no-sql-insert ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache")(.put "key" (my-lexical/get-value user_token))(.put "value" r)))
                                                                             (.setVar result r)
                                                                             )
                                                                         (catch Exception e
                                                                             (if-not (= (.getMessage e) "my-break")
                                                                                 (throw e))))
                                          :else
                                          (throw (Exception. "for 循环只能处理列表或者是执行数据库的结果"))
                                          )
                                    (my-lexical/get-value result))))))))

; 通过 group-id 获取 user-group 的列表对象
(defn get-user-group-by-id [ignite user-group-id]
    (if (= user-group-id 0)
        [0 "MY_META" "all"]
        (if-let [m (.get (.cache ignite "my_users_group") user-group-id)]
            [(.getId m) (.getSchema_name m) (.getGroup_type m)])
        ;(if-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.schema_name, g.group_type, m.id from my_users_group as g, my_dataset as m where g.data_set_id = m.id and g.id = ?") (to-array [user-group-id])))))]
        ;    (cons user-group-id m))
        ))


(defn -getUserGroup [ignite user_token]
    (get_user_group ignite user_token))

(defn -getUserGroupById [ignite user-group-id]
    (get-user-group-by-id ignite user-group-id))
































