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
        ))


; 通过 user_token 获取 user-group 的列表对象
(defn get_user_group [ignite user_token]
    (if (my-lexical/is-eq? user_token (.getRoot_token (.configuration ignite)))
        [0 "MY_META" "all" -1]
        (let [group_id [0 "MY_META" "all" -1]]
            (let [vs (my-lexical/no-sql-get-vs ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache")(.put "key" user_token)))]
                (cond (my-lexical/not-empty? vs) vs
                      :else (let [rs (my-smart-db/query_sql ignite group_id "select g.id, m.dataset_name, g.group_type, m.id from my_users_group as g, my_dataset as m where g.data_set_id = m.id and g.user_token = ?" [(my-lexical/to_arryList [user_token])])]
                                (loop [M-F-v156-I-Q157-c-Y (my-lexical/get-my-iter rs)]
                                    (if (.hasNext M-F-v156-I-Q157-c-Y)
                                        (let [r (.next M-F-v156-I-Q157-c-Y)]
                                            (my-lexical/no-sql-insert ignite group_id (doto (Hashtable.) (.put "table_name" "user_group_cache")(.put "key" user_token)(.put "value" r)))
                                            r
                                            (recur M-F-v156-I-Q157-c-Y))))))))))

; 通过 group-id 获取 user-group 的列表对象
(defn get-user-group-by-id [ignite user-group-id]
    (if (= user-group-id 0)
        [0 "MY_META" "all" -1]
        (if-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.dataset_name, g.group_type, m.id from my_users_group as g, my_dataset as m where g.data_set_id = m.id and g.id = ?") (to-array [user-group-id])))))]
            (cons user-group-id m))))