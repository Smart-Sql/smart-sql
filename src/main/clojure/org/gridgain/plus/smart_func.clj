(ns org.gridgain.plus.smart-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.tools MyConvertUtil MyPlusUtil KvSql MyDbUtil)
             (com.google.common.base Strings)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (cn.plus.model MyNoSqlCache MyCacheEx MyKeyValue MyLogCache MCron SqlType)
             (cn.plus.model.ddl MyViewsPk MyInsertViews MySelectViews MyUpdateViews MyDeleteViews)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (org.gridgain.smart.view MyViewAstPK)
             (org.gridgain.nosql MyNoSqlUtil)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.SmartFunc
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [smart_view [org.apache.ignite.Ignite Object String String] String]]
        ))

(defn cron-to-str
    ([lst] (cron-to-str lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (= "*" (first r))
             (recur r (concat lst [f " "]))
             (recur r (concat lst [f])))
         (str/join lst))))

(defn get-data-set-id-by-group-id [^Ignite ignite ^String group_name]
    (let [rs (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.id, g.id from my_users_group as g JOIN my_dataset as m ON m.id = g.data_set_id where g.group_name = ?") (to-array [group_name])))))]
        [(first rs) (second rs)]))

(defn get-data-set-id-by-ds-name [^Ignite ignite ^String schema_name]
    (if (my-lexical/is-eq? schema_name "public")
        0
        (let [rs (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_dataset as m where m.dataset_name = ?") (to-array [schema_name])))))]
            (first rs)))
    )

;(defn smart-view-select [^Ignite ignite ^String group_name lst code]
;    (let [ast (my-select-plus/sql-to-ast lst)]
;        (if (= (count ast) 1)
;            (let [{table-items :table-items} (-> (first ast) :table-items) [data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name)]
;                (if (= (count table-items) 1)
;                    (let [schema_name (str/lower-case (-> (first table-items) :schema_name)) table_name (str/lower-case (-> (first table-items) :table_name))]
;                        (if-not (Strings/isNullOrEmpty schema_name)
;                            (if-let [my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
;                                (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) (MyInsertViews. user_group_id table_name my_data_set_id code) (SqlType/INSERT)) (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]))
;                            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名"))))
;                    )))))

(defn smart-view-select [^Ignite ignite ^String group_name lst code]
    (let [ast (my-select-plus/sql-to-ast lst)]
        (if (= (count ast) 1)
            (let [table-items (-> (first ast) :sql_obj :table-items) [data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name)]
                (if (= (count table-items) 1)
                    (let [schema_name (str/lower-case (-> (first table-items) :schema_name)) table_name (str/lower-case (-> (first table-items) :table_name))]
                        (if-not (Strings/isNullOrEmpty schema_name)
                            (if-let [my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
                                (let [select-view (MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) (MySelectViews. user_group_id table_name my_data_set_id code) (SqlType/INSERT)) select-view-ast (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) ast (SqlType/INSERT))]
                                    (MyCacheExUtil/transLogCache ignite [select-view select-view-ast])
                                    )
                                )
                            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名"))))

                    )))))

;(defn smart-view-insert [^Ignite ignite ^String group_name lst code]
;    (let [{schema_name :schema_name table_name :table_name vs-line :vs-line} (my-insert/insert-body (rest (rest lst))) [data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name)]
;        (println "***************")
;        (println group_name)
;        (println code)
;        (println "***************")
;        (if (Strings/isNullOrEmpty schema_name)
;            (let [id (.incrementAndGet (.atomicSequence ignite "my_insert_views" 0 true))]
;                (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_insert_views" schema_name table_name id (MyInsertViews. id table_name data_set_id code) (SqlType/INSERT)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]))
;            (let [id (.incrementAndGet (.atomicSequence ignite "my_insert_views" 0 true)) my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
;                (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_insert_views" schema_name table_name id (MyInsertViews. id table_name my_data_set_id code) (SqlType/INSERT)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]))
;            )))

(defn smart-view-insert [^Ignite ignite ^String group_name lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-insert/insert-body (rest (rest lst))) [data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (let [my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
                    (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_insert_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) (MyInsertViews. user_group_id table_name my_data_set_id code) (SqlType/INSERT)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))])))
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

(defn smart-view-update [^Ignite ignite ^String group_name lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-update/get_table_name lst) [data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (let [my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
                    (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_update_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) (MyUpdateViews. user_group_id table_name my_data_set_id code) (SqlType/INSERT)) (MyNoSqlCache. "my_update_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))])))
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

(defn smart-view-delete [^Ignite ignite ^String group_name lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-delete/get_table_name lst) [data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (let [my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
                    (let [delete-view (MyNoSqlCache. "my_delete_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) (MyDeleteViews. user_group_id table_name my_data_set_id code) (SqlType/INSERT)) delete-view-ast (MyNoSqlCache. "my_delete_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]
                        (MyCacheExUtil/transLogCache ignite [delete-view delete-view-ast]))
                    ))
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

; 输入用户组和 code 名称添加 权限视图
(defn smart-view [^Ignite ignite group_id ^String group_name ^String code]
    (if (= (first group_id) 0)
        (let [lst (my-lexical/to-back code)]
            (cond (my-lexical/is-eq? (first lst) "insert") (smart-view-insert ignite group_name lst code)
                  (my-lexical/is-eq? (first lst) "update") (smart-view-update ignite group_name lst code)
                  (my-lexical/is-eq? (first lst) "delete") (smart-view-delete ignite group_name lst code)
                  (my-lexical/is-eq? (first lst) "select") (smart-view-select ignite group_name lst code)
                  ))
        (throw (Exception. "只有 root 用户才能对用户组设置权限！")))
    )

(defn rm-smart-view-insert [ignite group_name schema_name table_name]
    (let [[data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name) my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
        (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_insert_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) nil (SqlType/DELETE)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))])))

(defn rm-smart-view-update [ignite group_name schema_name table_name]
    (let [[data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name) my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
        (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_update_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) nil (SqlType/DELETE)) (MyNoSqlCache. "my_update_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))])))

(defn rm-smart-view-delete [ignite group_name schema_name table_name]
    (let [[data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name) my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
        (let [delete-view (MyNoSqlCache. "my_delete_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) nil (SqlType/DELETE)) delete-view-ast (MyNoSqlCache. "my_delete_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]
            (MyCacheExUtil/transLogCache ignite [delete-view delete-view-ast]))))

(defn rm-smart-view-select [ignite group_name schema_name table_name]
    (let [[data_set_id user_group_id] (get-data-set-id-by-group-id ignite group_name) my_data_set_id (get-data-set-id-by-ds-name ignite schema_name)]
        (let [select-view (MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name my_data_set_id) nil (SqlType/DELETE)) select-view-ast (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]
            (MyCacheExUtil/transLogCache ignite [select-view select-view-ast])
            )))

(defn rm-smart-view [^Ignite ignite group_id ^String group_name ^String schema_name-table_name ^String rm-type]
    (if (= (first group_id) 0)
        (let [lst (my-lexical/to-back schema_name-table_name)]
            (if (and (= (count lst) 3) (= (second lst) "."))
                (let [schema_name (first lst) table_name (last lst)]
                    (cond (my-lexical/is-eq? rm-type "insert") (rm-smart-view-insert ignite group_name schema_name table_name)
                          (my-lexical/is-eq? rm-type "update") (rm-smart-view-update ignite group_name schema_name table_name)
                          (my-lexical/is-eq? rm-type "delete") (rm-smart-view-delete ignite group_name schema_name table_name)
                          (my-lexical/is-eq? rm-type "select") (rm-smart-view-select ignite group_name schema_name table_name)
                          ))))
        (throw (Exception. "只有 root 用户才能对删除用户组权限！")))
    )

; 通过 userToken 获取 group_id
(defn get_group_id [^Ignite ignite ^Long group_id]
    (if (= group_id 0)
        [0 "MY_META" "ALL" -1]
        (when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.dataset_name, g.group_type, m.id from my_users_group as g, my_dataset as m where g.data_set_id = m.id and g.id = ?") (to-array [group_id])))))]
            (cons group_id m)))
    )

; 添加场景到用户组
(defn add-scenes-to [^Ignite ignite group_id belong-group-id method-name to-group-id]
    (cond (= (first group_id) belong-group-id) (MyNoSqlUtil/runCache ignite (MyNoSqlCache. "call_scenes" nil nil (MyCallScenesPk. to-group-id method-name) (MyCallScenes. belong-group-id to-group-id method-name (get_group_id ignite belong-group-id)) (SqlType/INSERT)))
          (= (first group_id) 0) (MyNoSqlUtil/runCache ignite (MyNoSqlCache. "call_scenes" nil nil (MyCallScenesPk. to-group-id method-name) (MyCallScenes. belong-group-id to-group-id method-name (get_group_id ignite belong-group-id)) (SqlType/INSERT)))
          :else
          (throw (Exception. (format "没有为用户组 id：%s 添加 %s 的权限" to-group-id method-name)))
          ))

; 删除场景到用户组
(defn rm-scenes-from [^Ignite ignite group_id belong-group-id method-name to-group-id]
    (cond (= (first group_id) belong-group-id) (MyNoSqlUtil/runCache ignite (MyNoSqlCache. "call_scenes" nil nil (MyCallScenesPk. to-group-id method-name) nil (SqlType/DELETE)))
          (= (first group_id) 0) (MyNoSqlUtil/runCache ignite (MyNoSqlCache. "call_scenes" nil nil (MyCallScenesPk. to-group-id method-name) nil (SqlType/DELETE)))
          :else
          (throw (Exception. (format "没有为用户组 id：%s 添加 %s 的权限" to-group-id method-name)))
          ))

; 添加 job
(defn add-job [^Ignite ignite group_id ^String job-name ^Object ps ^String cron]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if (.containsKey scheduledFutures job-name)
                (throw (Exception. (format "已存在任务 %s 不能添加相同的任务名！" job-name)))
                (try
                    (let [cron-line (cron-to-str (my-lexical/to-back cron)) my-cron-cache (.cache ignite "my_cron")]
                        (if-not (nil? (.scheduleLocal (.scheduler ignite) job-name (proxy [Object Runnable] []
                                                                                       (run []
                                                                                           (my-smart-scenes/my-invoke-scenes ignite group_id job-name ps)))
                                                      cron-line))
                            (.put my-cron-cache job-name (MCron. job-name cron-line (MyCacheExUtil/objToBytes ps)))))
                    (catch Exception ex
                        (.remove scheduledFutures job-name))
                    )
                ))))

; 删除 job
(defn remove-job [^Ignite ignite group_id ^String job-name]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (let [job-cache (.cache ignite "my_cron")]
                (if-let [job-obj (.get job-cache job-name)]
                    (if (.containsKey scheduledFutures job-name)
                        (try
                            (.remove scheduledFutures job-name)
                            (.remove job-cache job-name)
                            (catch Exception ex
                                (add-job ignite group_id job-name (MyCacheExUtil/restore (.getPs job-obj)) (.getCron job-obj)))
                            )
                        (throw (Exception. (format "任务 %s 不存在！" job-name)))))
                )
            )))

(defn -smart_view [^Ignite ignite group_id ^String group_name ^String code]
    (smart-view ignite group_id group_name code))































































