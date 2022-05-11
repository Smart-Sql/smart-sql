(ns org.gridgain.plus.dml.my-update
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyUpdate
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [my_update_run_log [org.apache.ignite.Ignite Long String] java.util.ArrayList]]
        ))

; 获取名字
(defn get_table_name [[f & r]]
    (if (and (some? f) (my-lexical/is-eq? f "update") (my-lexical/is-eq? (second r) "set"))
        {:table_name (first r) :rs_lst (rest (rest r))}))

(defn get_items
    ([rs_lst] (get_items rs_lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (my-lexical/is-eq? f "where")
             {:items_line lst :where_line r}
             (recur r (conj lst f)))
         {:items_line lst :where_line r})))

(defn get_item_lst
    ([lst_tokens] (get_item_lst lst_tokens [] [] []))
    ([[f & r] stack lst lst_result]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0) (> (count lst) 0)) (recur r [] [] (conj lst_result lst))
               (= f "(") (recur r (conj stack f) (conj lst f) lst_result)
               (= f ")") (recur r (pop stack) (conj lst f) lst_result)
               :else
               (recur r stack (conj lst f) lst_result))
         (if (> (count lst) 0) (conj lst_result lst) lst_result))))

(defn to_item_obj [lst]
    (if (= (second lst) "=")
        {:item_name (first lst) :item_obj (my-select/sql-to-ast (rest (rest lst)))}))

(defn item_jsons
    ([lst] (item_jsons lst []))
    ([[f & r] lst]
     (if (some? f)
         (recur r (conj lst (to_item_obj f)))
         lst)))

(defn get_json [lst]
    (if-let [{table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
        (if-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
            (if-let [items (get_item_lst items_line)]
                ;{:table_name table_name :items (item_jsons items) :where_line where_line}
                (assoc (my-lexical/get-schema (str/lower-case table_name)) :items (item_jsons items) :where_line where_line)
                )
            (throw (Exception. "更新数据的语句错误！")))
        (throw (Exception. "更新数据的语句错误！"))))

(defn get_json_lst [^clojure.lang.PersistentVector lst]
    (if-not (empty? lst)
        (if-let [{table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
            (if-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
                (if-let [items (get_item_lst items_line)]
                    {:table_name table_name :items (item_jsons items) :where_line where_line})
                (throw (Exception. "更新数据的语句错误！")))
            (throw (Exception. "更新数据的语句错误！")))
        (throw (Exception. "更新数据的语句不能为空！"))))

; 获取 update view obj
(defn get_view_obj [lst]
    (when-let [{table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
        (when-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
            {:table_name table_name :items (filter #(not= % ",") (map #(str/lower-case %) items_line)) :where_line (my-lexical/double-to-signal where_line)})))

; UPDATE categories set categoryname, description where categoryname = '白酒'
(defn get_view_db [^Ignite ignite ^Long group_id ^String table_name]
    (when-let [lst_rs (first (.getAll (.query (.cache ignite "my_update_views") (.setArgs (SqlFieldsQuery. "select m.code from my_update_views as m join my_group_view as v on m.id = v.view_id where m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [table_name group_id "改"])))))]
        (if (> (count lst_rs) 0) (get_view_obj (my-lexical/to-back (nth lst_rs 0))))))

(defn my-contains [[f & r] item_name]
    (if (my-lexical/is-eq? f item_name)
        true
        (recur r item_name)))

(defn has_authority_item [[f & r] v_items]
    (if (some? f)
        (if (my-contains v_items (str/lower-case (-> f :item_name)))
            (recur r v_items)
            (throw (Exception. (format "%s列没有修改的权限！" (-> f :item_name)))))))

; 合并 where
(defn merge_where [where_line v_where_line]
    (cond (and (some? where_line) (not (some? v_where_line))) where_line
          (and (some? v_where_line) (not (some? where_line))) v_where_line
          (and (some? where_line) (some? v_where_line)) (concat ["("] where_line [") and ("] v_where_line [")"])
          ))

; 判断权限
(defn get-authority [^Ignite ignite ^Long group_id lst-sql]
    (when-let [{schema_name :schema_name table_name :table_name items :items where_line :where_line} (get_json lst-sql)]
        (if-let [{v_items :items v_where_line :where_line} (get_view_db ignite group_id table_name)]
            (if (nil? (has_authority_item items v_items))
                {:schema_name schema_name :table_name table_name :items items :where_line (merge_where where_line v_where_line)}
                {:schema_name schema_name :table_name table_name :items items :where_line where_line})
            {:schema_name schema_name :table_name table_name :items items :where_line where_line}
            )))

(defn get-authority-lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector sql_lst]
    (when-let [{table_name :table_name items :items where_line :where_line} (get_json_lst sql_lst)]
        (if-let [{v_items :items v_where_line :where_line} (get_view_db ignite group_id table_name)]
            (if (nil? (has_authority_item items v_items))
                {:table_name table_name :items items :where_line (merge_where where_line v_where_line)}
                {:table_name table_name :items items :where_line where_line})
            {:table_name table_name :items items :where_line where_line}
            )))

; 直接在实时中执行
; 过程如下：
; 1、获取表的 PK 定义
; 2、生成 select sql 通过 PK 查询 cache
; 3，修改 cache 的值并生成 MyLogCache
; 4、对 cache 和 MyLogCache 执行事务

(defn get-rows [^Ignite ignite ^String schema_name ^String table_name]
    (cond (my-lexical/is-eq? schema_name "public") (.getAll (.query (.cache ignite "my_meta_tables") (doto (SqlFieldsQuery. "SELECT m.column_name, m.pkid, m.column_type FROM table_item AS m INNER JOIN my_meta_tables AS o ON m.table_id = o.id WHERE o.table_name = ?")
                                                                                                         (.setArgs (to-array [table_name])))))
          (my-lexical/is-eq? schema_name "my_meta") (throw (Exception. "没有修改权限！"))
          :else
          (.getAll (.query (.cache ignite "my_meta_tables") (doto (SqlFieldsQuery. "SELECT m.column_name, m.pkid, m.column_type FROM table_item AS m, my_meta_tables AS o, my_dataset as ds WHERE m.table_id = o.id and ds.id = o.data_set_id and o.table_name = ? and ds.dataset_name = ?")
                                                                (.setArgs (to-array [table_name schema_name])))))
          ))

; 1、获取表的 PK 定义
(defn get_pk_def [^Ignite ignite ^String schema_name ^String table_name]
    (when-let [rows (get-rows ignite schema_name table_name)]
        (loop [[f & r] rows lst [] lst_pk [] dic {}]
            (if (some? f)
                (if (= (.get f 1) true) (recur r lst (conj lst_pk (.get f 0)) (assoc dic (.get f 0) (.get f 2)))
                                        (recur r (conj lst (.get f 0)) lst_pk (assoc dic (.get f 0) (.get f 2))))
                {:lst lst :lst_pk lst_pk :dic dic}))))

(defn get_pk_def_map [^Ignite ignite ^String schema_name ^String table_name]
    (when-let [{lst :lst lst_pk :lst_pk dic :dic} (get_pk_def ignite schema_name table_name)]
        (if (> (count lst_pk) 1)
            (loop [[f & r] lst_pk sb (StringBuilder.)]
                (if (some? f)
                    (if (some? r)
                        (recur r (doto sb (.append (format "%s_pk" f)) (.append ",")))
                        (recur r (doto sb (.append (format "%s_pk" f)))))
                    {:line (.toString sb) :lst lst :lst_pk lst_pk :dic dic}))
            {:line (first lst_pk) :lst lst :lst_pk lst_pk :dic dic}
            )
        ))

; 2、生成 select sql 通过 PK 查询 cache
(defn get_update_query_sql [^Ignite ignite obj]
    (when-let [{pk_line :line lst :lst lst_pk :lst_pk dic :dic} (get_pk_def_map ignite (-> obj :schema_name) (-> obj :table_name))]
        (letfn [(get_items_type [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic (str/lower-case (-> f :item_name)))
                            (recur r dic (conj lst (assoc f :type (get dic (str/lower-case (-> f :item_name))))))
                            (recur r dic lst))
                        lst))
                (get_pk_lst [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic f)
                            (recur r dic (conj lst {:item_name f :item_type (get dic f)}))
                            (recur r dic lst))
                        lst))]
            {:schema_name (-> obj :schema_name) :table_name (-> obj :table_name) :sql (format "select %s from %s.%s where %s" pk_line (-> obj :schema_name) (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_line))) :items (get_items_type (-> obj :items) dic []) :pk_lst (get_pk_lst lst_pk dic []) :lst lst :dic dic})
        ))

; update 转换为对象
(defn get_update_obj [^Ignite ignite ^Long group_id lst-sql]
    (if-let [m (get-authority ignite group_id lst-sql)]
        (if-let [us (get_update_query_sql ignite m)]
            us
            (throw (Exception. "更新语句字符串错误！")))
        (throw (Exception. "更新语句字符串错误！"))))

(defn update_run_super_admin [^Ignite ignite lst-sql]
    (if-let [{table_name :table_name} (get_table_name (my-lexical/to-back lst-sql))]
        (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name))
            (.getAll (.query (.cache ignite (str/lower-case table_name)) (SqlFieldsQuery. (str/join " " lst-sql))))
            (throw (Exception. "超级管理员不能修改具体的业务数据！")))
        (throw (Exception. "更新语句字符串错误！"))))

(defn update_run_log [^Ignite ignite ^Long group_id lst-sql]
    )

(defn update_run_no_log [^Ignite ignite ^Long group_id ^String sql]
    )


; 1、判断用户组在实时数据集，还是非实时数据
; 如果是非实时数据集,
; 获取表名后，查一下，表名是否在 对应的 my_dataset_table 中，如果在就不能添加，否则直接执行 insert sql
; 2、如果是在实时数据集是否需要 log
(defn update_run [^Ignite ignite ^Long group_id lst-sql sql]
    (if (= group_id 0)
        ; 超级用户
        (.getAll (.query (.cache ignite "my_meta_table") (SqlFieldsQuery. sql)))
        (if (true? (.isDataSetEnabled (.configuration ignite)))
            (my-lexical/trans ignite (update_run_log ignite group_id lst-sql))
            (my-lexical/trans ignite (update_run_no_log ignite group_id lst-sql)))
        )
    )























































