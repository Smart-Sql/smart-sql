(ns org.gridgain.plus.smart-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
        [org.gridgain.plus.tools.my-user-group :as my-user-group]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.tools MyConvertUtil MyPlusUtil MyGson KvSql MyDbUtil)
             (com.google.common.base Strings)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (cn.plus.model MySmartDll MyNoSqlCache MyCacheEx MyKeyValue MyLogCache MCron SqlType)
             (cn.plus.model.ddl MyUserFunc MyFunc MyViewsPk MyInsertViews MySelectViews MyUpdateViews MyDeleteViews)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (org.gridgain.smart.view MyViewAstPK)
             (org.gridgain.nosql MyNoSqlUtil)
             (org.gridgain.myservice MySmartSqlService)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.SmartFunc
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [smart_view [org.apache.ignite.Ignite Object String String] String]
                  ^:static [initJob [org.apache.ignite.Ignite] void]
                  ^:static [recoveryToCluster [org.apache.ignite.Ignite Object] void]]
        ))

(defn cron-to-str
    ([lst] (cron-to-str lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (contains? #{"*" "?"} (first r))
             (recur r (concat lst [f " "]))
             (recur r (concat lst [f])))
         (str/join lst))))

(defn get-data-set-id-by-group-id [^Ignite ignite ^String group_name]
    (let [rs (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select g.id from my_users_group as g where g.group_name = ?") (to-array [group_name])))))]
        (first rs)))

;(defn get-data-set-id-by-ds-name [^Ignite ignite ^String schema_name]
;    (cond (my-lexical/is-eq? schema_name "public") 0
;          (my-lexical/is-eq? schema_name "my_meta") -1
;          :else (let [rs (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_dataset as m where m.schema_name = ?") (to-array [schema_name])))))]
;                    (first rs)))
;    )

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
            (let [table-items (-> (first ast) :sql_obj :table-items) user_group_id (get-data-set-id-by-group-id ignite group_name)]
                (if (= (count table-items) 1)
                    (let [schema_name (str/lower-case (-> (first table-items) :schema_name)) table_name (str/lower-case (-> (first table-items) :table_name))]
                        (if-not (Strings/isNullOrEmpty schema_name)
                            (let [select-view (MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MySelectViews. user_group_id table_name schema_name code) (SqlType/INSERT)) select-view-ast (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) ast (SqlType/INSERT))]
                                (MyCacheExUtil/transLogCache ignite [select-view select-view-ast])
                                )
                            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名"))))

                    )))))

(defn smart-view-select-tran [^Ignite ignite ^String group_name user_group_id lst code]
    (let [ast (my-select-plus/sql-to-ast lst)]
        (if (= (count ast) 1)
            (let [table-items (-> (first ast) :sql_obj :table-items)]
                (if (= (count table-items) 1)
                    (let [schema_name (str/lower-case (-> (first table-items) :schema_name)) table_name (str/lower-case (-> (first table-items) :table_name))]
                        (if-not (Strings/isNullOrEmpty schema_name)
                            (let [select-view (MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MySelectViews. user_group_id table_name schema_name code) (SqlType/INSERT)) select-view-ast (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) ast (SqlType/INSERT))]
                                [select-view select-view-ast])
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
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-insert/insert-body (rest (rest lst))) user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_insert_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MyInsertViews. user_group_id table_name schema_name code) (SqlType/INSERT)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]))
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

(defn smart-view-insert-tran [^Ignite ignite ^String group_name user_group_id lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-insert/insert-body (rest (rest lst)))]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                [(MyNoSqlCache. "my_insert_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MyInsertViews. user_group_id table_name schema_name code) (SqlType/INSERT)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))])
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

(defn smart-view-update [^Ignite ignite ^String group_name lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-update/get_table_name lst) user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_update_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MyUpdateViews. user_group_id table_name schema_name code) (SqlType/INSERT)) (MyNoSqlCache. "my_update_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]))
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

(defn smart-view-update-tran [^Ignite ignite ^String group_name user_group_id lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-update/get_table_name lst)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                [(MyNoSqlCache. "my_update_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MyUpdateViews. user_group_id table_name schema_name code) (SqlType/INSERT)) (MyNoSqlCache. "my_update_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))])
            (throw (Exception. "在设置权限视图中，必须有数据集的名字！必须是：数据集.表名")))
        ))

(defn smart-view-delete [^Ignite ignite ^String group_name lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-delete/get_table_name lst) user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (let [delete-view (MyNoSqlCache. "my_delete_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MyDeleteViews. user_group_id table_name schema_name code) (SqlType/INSERT)) delete-view-ast (MyNoSqlCache. "my_delete_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]
                    (MyCacheExUtil/transLogCache ignite [delete-view delete-view-ast])))
            (throw (Exception. "在设置权限视图中，delete 语言有误！")))
        ))

(defn smart-view-delete-tran [^Ignite ignite ^String group_name user_group_id lst code]
    (let [{schema_name_v :schema_name table_name_v :table_name} (my-delete/get_table_name lst)]
        (if-not (Strings/isNullOrEmpty schema_name_v)
            (let [schema_name (str/lower-case schema_name_v) table_name (str/lower-case table_name_v)]
                (let [delete-view (MyNoSqlCache. "my_delete_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) (MyDeleteViews. user_group_id table_name schema_name code) (SqlType/INSERT)) delete-view-ast (MyNoSqlCache. "my_delete_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) (my-lexical/to-back code) (SqlType/INSERT))]
                    [delete-view delete-view-ast]))
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

(defn smart-view-tran [^Ignite ignite group_id ^String group_name user_group_id ^String code]
    (if (= (first group_id) 0)
        (let [lst (my-lexical/to-back code)]
            (cond (my-lexical/is-eq? (first lst) "insert") (smart-view-insert-tran ignite group_name user_group_id lst code)
                  (my-lexical/is-eq? (first lst) "update") (smart-view-update-tran ignite group_name user_group_id lst code)
                  (my-lexical/is-eq? (first lst) "delete") (smart-view-delete-tran ignite group_name user_group_id lst code)
                  (my-lexical/is-eq? (first lst) "select") (smart-view-select-tran ignite group_name user_group_id lst code)
                  ))
        (throw (Exception. "只有 root 用户才能对用户组设置权限！")))
    )

(defn rm-smart-view-insert [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_insert_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))])))

(defn rm-smart-view-update [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (MyCacheExUtil/transLogCache ignite [(MyNoSqlCache. "my_update_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) (MyNoSqlCache. "my_update_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))])))

(defn rm-smart-view-delete [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (let [delete-view (MyNoSqlCache. "my_delete_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) delete-view-ast (MyNoSqlCache. "my_delete_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]
            (MyCacheExUtil/transLogCache ignite [delete-view delete-view-ast]))))

(defn rm-smart-view-select [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (let [select-view (MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) select-view-ast (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]
            (MyCacheExUtil/transLogCache ignite [select-view select-view-ast])
            )))

(defn rm-smart-view-insert-tran [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        [(MyNoSqlCache. "my_insert_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) (MyNoSqlCache. "my_insert_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]))

(defn rm-smart-view-update-tran [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        [(MyNoSqlCache. "my_update_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) (MyNoSqlCache. "my_update_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]))

(defn rm-smart-view-delete-tran [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (let [delete-view (MyNoSqlCache. "my_delete_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) delete-view-ast (MyNoSqlCache. "my_delete_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]
            [delete-view delete-view-ast])))

(defn rm-smart-view-select-tran [ignite group_name schema_name table_name]
    (let [user_group_id (get-data-set-id-by-group-id ignite group_name)]
        (let [select-view (MyNoSqlCache. "my_select_views" schema_name table_name (MyViewsPk. user_group_id table_name schema_name) nil (SqlType/DELETE)) select-view-ast (MyNoSqlCache. "my_select_view_ast" schema_name table_name (MyViewAstPK. schema_name table_name user_group_id) nil (SqlType/DELETE))]
            [select-view select-view-ast]
            )))

; rm_view('wudafu_group', 'public.Categories', 'update');
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

(defn rm-smart-view-tran [^Ignite ignite group_id ^String group_name ^String schema_name-table_name ^String rm-type]
    (if (= (first group_id) 0)
        (let [lst (my-lexical/to-back schema_name-table_name)]
            (if (and (= (count lst) 3) (= (second lst) "."))
                (let [schema_name (first lst) table_name (last lst)]
                    (cond (my-lexical/is-eq? rm-type "insert") (rm-smart-view-insert-tran ignite group_name schema_name table_name)
                          (my-lexical/is-eq? rm-type "update") (rm-smart-view-update-tran ignite group_name schema_name table_name)
                          (my-lexical/is-eq? rm-type "delete") (rm-smart-view-delete-tran ignite group_name schema_name table_name)
                          (my-lexical/is-eq? rm-type "select") (rm-smart-view-select-tran ignite group_name schema_name table_name)
                          ))))
        (throw (Exception. "只有 root 用户才能对删除用户组权限！")))
    )

; 通过 group_id 获取 group_id 的对象
(defn get_group_id [^Ignite ignite ^Long group_id]
    (if (= group_id 0)
        [0 "MY_META" "ALL"]
        (if-let [m (.get (.cache ignite "my_users_group") group_id)]
            [group_id (.getSchema_name m) (.getGroup_type m)]))
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

(defn run-job [ignite group_id job-name ps]
    (cond (my-lexical/is-scenes? ignite group_id job-name) (if-not (empty? ps)
                                                               (my-smart-scenes/my-invoke-scenes ignite group_id job-name ps)
                                                               (my-smart-scenes/my-invoke-scenes-no-ps ignite group_id job-name))
          (my-lexical/is-func? ignite job-name) (if-not (empty? ps)
                                                    (apply my-smart-scenes/my-invoke-func ignite job-name ps)
                                                    (my-smart-scenes/my-invoke-func-no-ps ignite job-name))
          (my-lexical/is-call-scenes? ignite group_id job-name) (if-not (empty? ps)
                                                                    (my-smart-scenes/my-invoke-scenes ignite group_id job-name ps)
                                                                    (my-smart-scenes/my-invoke-scenes-no-ps ignite group_id job-name))
          ))

(defn init-add-job [^Ignite ignite ^Long my-group-id ^String job-name ^Object ps ^String cron]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if-not (.containsKey scheduledFutures job-name)
                (try
                    (let [cron-line (cron-to-str (my-lexical/to-back cron))]
                        (.scheduleLocal (.scheduler ignite) job-name (proxy [Object Runnable] []
                                                                         (run []
                                                                             ;(my-smart-scenes/my-invoke-scenes ignite group_id job-name ps)
                                                                             (run-job ignite (my-user-group/get-user-group-by-id ignite my-group-id) job-name ps)
                                                                             ))
                                        cron-line))
                    (catch Exception ex
                        (.remove scheduledFutures job-name))
                    )
                ))))

(defn init-job [^Ignite ignite]
    (let [group_id [0 "MY_META" "all"]]
        (let [rs (my-smart-db/query_sql ignite group_id "select m.job_name, m.group_id, m.cron, m.ps from my_cron m" nil)]
            (loop [M-F-v156-I-Q157-c-Y (my-lexical/get-my-iter rs)]
                (if (.hasNext M-F-v156-I-Q157-c-Y)
                    (let [r (.next M-F-v156-I-Q157-c-Y)]
                        (init-add-job ignite (nth r 1) (nth r 0) (MyConvertUtil/ConvertToList (nth r 3)) (nth r 2))
                        (recur M-F-v156-I-Q157-c-Y)))))))

(defn -initJob [^Ignite ignite]
    (init-job ignite))

; 添加 job
; job-name: 任务名，场景名，函数名
; ps：参数，列表类型，例如： ["吴大富", 100], 如果是空 []
; cron: 0/5 * * * * ?   表示每5秒 执行任务
(defn add-job [^Ignite ignite group_id ^String job-name ^Object ps ^String cron]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if (.containsKey scheduledFutures job-name)
                (throw (Exception. (format "已存在任务 %s 不能添加相同的任务名！" job-name)))
                (try
                    (let [cron-line (cron-to-str (my-lexical/to-back cron)) my-cron-cache (.cache ignite "my_cron")]
                        (if-not (nil? (.scheduleLocal (.scheduler ignite) job-name (proxy [Object Runnable] []
                                                                                       (run []
                                                                                           ;(my-smart-scenes/my-invoke-scenes ignite group_id job-name ps)
                                                                                           (run-job ignite group_id job-name ps)
                                                                                           ))
                                                      cron-line))
                            (.put my-cron-cache job-name (MCron. job-name (first group_id) cron-line (my-lexical/gson ps)))))
                    (catch Exception ex
                        (.remove scheduledFutures job-name))
                    )
                ))))

; 删除 job
; 除超级用户外，只有同一个用户组的，才可以删除
(defn remove-job [^Ignite ignite group_id ^String job-name]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (let [job-cache (.cache ignite "my_cron")]
            (if-let [m-job (.get job-cache job-name)]
                (cond (or (= (.getGroup_id m-job) (first group_id)) (= (first group_id) 0)) (if (.containsKey (.getScheduledFutures scheduleProcessor) job-name)
                                                                                                (try
                                                                                                    ;(.onDescheduled scheduleProcessor job-name)
                                                                                                    (if (true? (.cancel (.get (.getScheduledFutures scheduleProcessor) job-name)))
                                                                                                        (.remove job-cache job-name))
                                                                                                    (catch Exception ex
                                                                                                        (throw ex))
                                                                                                    )
                                                                                                (throw (Exception. (format "任务 %s 不存在！" job-name))))
                      :else (throw (Exception. (format "没有删除 %s 任务的权限！" job-name)))
                      ))
            )))

; 获取 job 快照
(defn get-job-snapshot [^Ignite ignite group_id ^String job-name]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (let [job-cache (.cache ignite "my_cron") scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if-let [m-job (.get job-cache job-name)]
                (cond (or (= (.getGroup_id m-job) (first group_id)) (= (first group_id) 0)) (if (.containsKey scheduledFutures job-name)
                                                                                                (try
                                                                                                    ;(.onDescheduled scheduleProcessor job-name)
                                                                                                    (let [sm (.get scheduledFutures job-name)]
                                                                                                        (doto (StringBuilder.)
                                                                                                            (.append (format "cron: %s " (.pattern sm)))
                                                                                                            (.append (format "是否运行: %s " (.isRunning sm)))
                                                                                                            (.append (format "开始时间: %s " (.lastStartTime sm)))
                                                                                                            (.append (format "平均执行时间: %s " (.averageExecutionTime sm)))
                                                                                                            (.append (format "结束时间: %s" (.lastFinishTime sm)))
                                                                                                            ))
                                                                                                    (catch Exception ex
                                                                                                        (throw ex))
                                                                                                    )
                                                                                                (throw (Exception. (format "任务 %s 不存在！" job-name))))
                      :else (throw (Exception. (format "没有参考 %s 任务快照的权限！" job-name)))
                      ))
            )))

;(defn remove-job [^Ignite ignite group_id ^String job-name]
;    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
;        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
;            (let [job-cache (.cache ignite "my_cron")]
;                (if-let [m-job (.get job-cache job-name)]
;                    (cond (or (= (.getGroup_id m-job) (first group_id)) (= (first group_id) 0)) (if (.containsKey scheduledFutures job-name)
;                                                                                                    (try
;                                                                                                        (.remove scheduledFutures job-name)
;                                                                                                        (.remove job-cache job-name)
;                                                                                                        (catch Exception ex
;                                                                                                            (throw ex))
;                                                                                                        )
;                                                                                                    (throw (Exception. (format "任务 %s 不存在！" job-name))))
;                          :else (throw (Exception. (format "没有删除 %s 任务的权限！" job-name)))
;                          ))
;                )
;            )))

; add_func: 添加扩展方法
(defn add_func [^Ignite ignite group_id func-define]
    (if (= (first group_id) 0)
        (if-let [m (MyGson/getUserFunc func-define)]
            (let [cache (.cache ignite "my_func")]
                (if (.containsKey cache (.getMethod_name m))
                    (throw (Exception. "扩展方法已经存在，如果要添加，请将旧方法先删除！"))
                    (.put cache (.getMethod_name m) (MyFunc/fromUserFunc m))))
            (throw (Exception. "参数输入错误！输入的参数需要符合 MyUserFunc 的 json 对象！")))
        (throw (Exception. "只有 root 用户才能添加扩展方法！"))))

; add_func: 添加扩展方法
(defn remove_func [^Ignite ignite group_id func-name-0]
    (if (= (first group_id) 0)
        (let [cache (.cache ignite "my_func") func-name (str/lower-case func-name-0)]
            (if (.containsKey cache func-name)
                (.remove cache func-name)
                (throw (Exception. "扩展方法不存在，所以不能删除！"))))
        (throw (Exception. "只有 root 用户才能删除扩展方法！"))))

(defn -smart_view [^Ignite ignite group_id ^String group_name ^String code]
    (smart-view ignite group_id group_name code))

; 恢复数据
(defn recovery-to-cluster [^Ignite ignite data]
    (if-let [m (MyCacheExUtil/restore data)]
        (cond (instance? MySmartDll m) (.recovery_ddl (.getInstance MySmartSqlService) ignite (.getSql m))
              (instance? MyNoSqlCache m) (MyCacheExUtil/restoreNoSqlCache ignite m)
              (instance? MyLogCache m) (MyCacheExUtil/restoreLogCache ignite m)
              (my-lexical/is-seq? m) (MyCacheExUtil/restoreListData ignite m)
              )))

; 恢复数据 java 调用
(defn -recoveryToCluster [^Ignite ignite data]
    (recovery-to-cluster ignite data))

































































