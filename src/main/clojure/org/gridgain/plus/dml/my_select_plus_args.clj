(ns org.gridgain.plus.dml.my-select-plus-args
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.tools.my-cache :as my-cache]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (com.google.common.base Strings)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (org.tools MyGson)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySelectPlusArgs
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

; 重新获取新的 ast 将 权限添加进去
; 1、扫描 :sql_obj --> :table-items --> :table_name
; 2、判断 :sql_obj --> :query-items 中的 :item_name 是否存在于权限中
; 3、添加 :sql_obj --> :where-items
(defn re-select-ast [ignite group_id ast]
    (letfn [(get-table-items [ignite group_id sql-obj]
                (letfn [
                        ; 获取 table_select_view 的 ast
                        ; 重新生成新的 ast
                        ; 新的 ast = {query_item = {'item_name': '转换的函数'}}
                        (get_select_view [ignite group_id schema_name talbe_name]
                            (cond (and (or (my-lexical/is-eq? schema_name "my_meta") (my-lexical/is-empty? schema_name)) (= (first group_id) 0)) nil
                                  (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "用户不存在或者没有权限！删除数据！"))
                                  (and (my-lexical/is-empty? schema_name) (my-lexical/is-not-empty? (second group_id))) (if-let [sql_objs (my-lexical/get-select-code ignite (second group_id) talbe_name group_id)]
                                                                                                                            (if (= (count sql_objs) 1)
                                                                                                                                (if-let [{query-items :query-items where-items :where-items} (get (first sql_objs) :sql_obj)]
                                                                                                                                    (if (and (= (count query-items) 1) (contains? (first query-items) :operation_symbol))
                                                                                                                                        {:query-items nil :where-items where-items}
                                                                                                                                        {:query-items (get_query_view query-items) :where-items where-items})
                                                                                                                                    )))
                                  (and (my-lexical/is-eq? schema_name (second group_id)) (my-lexical/is-not-empty? (second group_id))) (if-let [sql_objs (my-lexical/get-select-code ignite schema_name talbe_name group_id)]
                                                                                                                                           (if (= (count sql_objs) 1)
                                                                                                                                               (if-let [{query-items :query-items where-items :where-items} (get (first sql_objs) :sql_obj)]
                                                                                                                                                   (if (and (= (count query-items) 1) (contains? (first query-items) :operation_symbol))
                                                                                                                                                       {:query-items nil :where-items where-items}
                                                                                                                                                       {:query-items (get_query_view query-items) :where-items where-items})
                                                                                                                                                   )))
                                  (and (not (my-lexical/is-eq? schema_name (second group_id))) (my-lexical/is-not-empty? schema_name) (my-lexical/is-not-empty? (second group_id))) (if-let [sql_objs (my-lexical/get-select-code ignite schema_name talbe_name group_id)]
                                                                                                                                                                                        (if (= (count sql_objs) 1)
                                                                                                                                                                                            (if-let [{query-items :query-items where-items :where-items} (get (first sql_objs) :sql_obj)]
                                                                                                                                                                                                (if (and (= (count query-items) 1) (contains? (first query-items) :operation_symbol))
                                                                                                                                                                                                    {:query-items nil :where-items where-items}
                                                                                                                                                                                                    {:query-items (get_query_view query-items) :where-items where-items})
                                                                                                                                                                                                ))
                                                                                                                                                                                        (throw (Exception. "用户不存在或者没有权限！查询数据！")))
                                  ))
                        (get_query_view
                            ([query-items] (get_query_view query-items {}))
                            ([[f & r] dic]
                             (if (some? f)
                                 (cond (contains? f :item_name) (recur r (assoc dic (get f :item_name) nil))
                                       (and (contains? f :func-name) (my-lexical/is-eq? (get f :func-name) "convert_to") (= (count (get f :lst_ps)) 3)) (recur r (assoc dic (get (first (get f :lst_ps)) :item_name) (last (get f :lst_ps))))
                                       (contains? f :comma_symbol) (recur r dic)
                                       :else
                                       (throw (Exception. "select 权限视图中只能是字段或者是转换函数！")))
                                 dic)))]
                    (loop [[f & r] (-> sql-obj :table-items) dic {}]
                        (if (some? f)
                            (if (contains? f :table_name)
                                (if-let [table_ast (get_select_view ignite group_id (get f :schema_name) (get f :table_name))]
                                    (if-not (Strings/isNullOrEmpty (-> f :table_alias))
                                        (recur r (assoc dic (-> f :table_alias) table_ast))
                                        (if-not (nil? r)
                                            (throw (Exception. "两个表以上要给表取别名"))
                                            (recur r (assoc dic "" table_ast))))
                                    (recur r dic))
                                (recur r dic))
                            (if (= dic {})
                                nil dic))))
                )
            (replace-alias [table_alias m]
                (cond (my-lexical/is-seq? m) (map (partial replace-alias table_alias) m)
                      (map? m) (if (and (contains? m :item_name) (false? (-> m :const)))
                                   (assoc m :table_alias table_alias)
                                   (loop [[f & r] (keys m) rs m]
                                       (if (some? f)
                                           (let [vs (get m f)]
                                               (cond (my-lexical/is-seq? vs) (recur r (assoc rs f (replace-alias table_alias vs)))
                                                     (map? vs) (recur r (assoc rs f (replace-alias table_alias vs)))
                                                     :else
                                                     (recur r rs)
                                                     ))
                                           rs))
                                   )
                      ))
            (my-table-items [ignite group_id sql-obj]
                (let [table-ast (get-table-items ignite group_id sql-obj)]
                    (loop [[f & r] (keys table-ast) rs table-ast]
                        (if (some? f)
                            (let [new-where-items (replace-alias f (-> (get table-ast f) :where-items))]
                                (let [my-vs (assoc (get table-ast f) :where-items new-where-items)]
                                    (recur r (assoc rs f my-vs)))
                                )
                            rs))))
            (my-query-items [authority-ast m]
                (cond (my-lexical/is-seq? m) (map (partial my-query-items authority-ast) m)
                      (map? m) (if (and (contains? m :item_name) (false? (-> m :const)))
                                   (if-let [ar-query-items (-> (get authority-ast (-> m :table_alias)) :query-items)]
                                       (if (my-lexical/is-seq-contains? (keys ar-query-items) (-> m :item_name))
                                           (if (nil? (get ar-query-items (-> m :item_name)))
                                               m
                                               (get ar-query-items (-> m :item_name)))
                                           (throw (Exception. (format "没有访问字段 %s 的权限" (-> m :item_name)))))
                                       m)
                                   (loop [[f & r] (keys m) rs m]
                                       (if (some? f)
                                           (let [vs (get m f)]
                                               (cond (my-lexical/is-seq? vs) (recur r (assoc rs f (my-query-items authority-ast vs)))
                                                     (map? vs) (recur r (assoc rs f (my-query-items authority-ast vs)))
                                                     :else
                                                     (recur r rs)
                                                     ))
                                           rs))
                                   )
                      ))
            (my-where-items
                ([authority-ast m] (my-where-items authority-ast m []))
                ([authority-ast m lst]
                 (if (and (not (nil? (-> m :where-items))) (not (empty? (-> m :where-items))))
                     (loop [[f & r] (keys authority-ast) lst-rs []]
                         (if (some? f)
                             (if-not (nil? (-> (get authority-ast f) :where-items))
                                 (recur r (concat lst-rs [{:parenthesis (-> (get authority-ast f) :where-items)} {:and_or_symbol "and"}]))
                                 (recur r lst-rs))
                             (concat lst lst-rs [{:parenthesis (-> m :where-items)}])))
                     (-> m :where-items))))
            (re-all-sql-obj [ignite group_id ast]
                (cond (my-lexical/is-seq? ast) (map (partial re-all-sql-obj ignite group_id) ast)
                      (map? ast) (if (contains? ast :sql_obj)
                                     (if-let [authority-ast (my-table-items ignite group_id (-> ast :sql_obj))]
                                         (let [new-sql-obj (assoc (-> ast :sql_obj) :query-items (my-query-items authority-ast (-> ast :sql_obj :query-items)) :where-items (my-where-items authority-ast (-> ast :sql_obj)))]
                                             (assoc ast :sql_obj new-sql-obj))
                                         ast)
                                     (loop [[f & r] (keys ast) rs ast]
                                         (if (some? f)
                                             (let [vs (get ast f)]
                                                 (cond (my-lexical/is-seq? vs) (recur r (assoc rs f (re-all-sql-obj ignite group_id vs)))
                                                       (map? vs) (recur r (assoc rs f (re-all-sql-obj ignite group_id vs)))
                                                       :else
                                                       (recur r rs)
                                                       ))
                                             rs)))
                      ))
            (re-ast [ignite group_id ast]
                (cond (my-lexical/is-seq? ast) (map (partial re-ast ignite group_id) ast)
                      (map? ast) (if (contains? ast :sql_obj)
                                     (let [new-ast (re-all-sql-obj ignite group_id ast)]
                                         (loop [[f & r] (keys (-> new-ast :sql_obj)) rs (-> new-ast :sql_obj)]
                                             (if (some? f)
                                                 (let [vs (get (-> new-ast :sql_obj) f)]
                                                     (cond (my-lexical/is-seq? vs) (recur r (assoc rs f (re-ast ignite group_id vs)))
                                                           (map? vs) (recur r (assoc rs f (re-ast ignite group_id vs)))
                                                           :else
                                                           (recur r rs)
                                                           ))
                                                 (assoc new-ast :sql_obj rs))))
                                     (loop [[f & r] (keys ast) rs ast]
                                         (if (some? f)
                                             (let [vs (get ast f)]
                                                 (cond (my-lexical/is-seq? vs) (recur r (assoc rs f (re-ast ignite group_id vs)))
                                                       (map? vs) (recur r (assoc rs f (re-ast ignite group_id vs)))
                                                       :else
                                                       (recur r rs)
                                                       ))
                                             rs))
                                     )))]
        (re-ast ignite group_id ast)))

; ast to sql
(defn ast_to_sql [ignite group_id dic-args ast]
    (letfn [(select_to_sql_single [ignite group_id dic-args ast]
                (if (and (some? ast) (map? ast))
                    (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} ast]
                        (cond (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items)]
                                                                                                                                                                        {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items)]
                                                                                                                                                                       {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by)]
                                                                                                                                                                      {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                    {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql) "order by" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args order-by) tk-6 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                   {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql) "order by" (-> tk-5 :sql) "limit" (-> tk-6 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args) (-> tk-6 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                    {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql) "limit" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "order by" (-> tk-4 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "limit" (-> tk-4 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-5 (lst-token-to-line ignite group_id dic-args order-by) tk-6 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                    {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "order by" (-> tk-5 :sql) "limit" (-> tk-6 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-5 :args) (-> tk-6 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-5 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                      {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "order by" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                      {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "limit" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-5 (lst-token-to-line ignite group_id dic-args order-by) tk-6 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "order by" (-> tk-5 :sql) "limit" (-> tk-6 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args) (-> tk-6 :args)))})

                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by)]
                                                                                                                                                                      {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having)]
                                                                                                                                                                      {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having) tk-4 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql) "order by" (-> tk-4 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having) tk-4 (lst-token-to-line ignite group_id dic-args order-by) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                    {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql) "order by" (-> tk-4 :sql) "limit" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "limit" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql) "limit" (-> tk-5 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                       {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "order by" (-> tk-2 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args order-by) tk-3 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                      {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "order by" (-> tk-2 :sql) "limit" (-> tk-3 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-3 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                       {:sql (str/join " " ["select" (-> tk :sql) "from" (-> tk-1 :sql) "limit" (-> tk-3 :sql)]) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-3 :args)))})

                              ))))
            (get-map-token-to-sql [m]
                (loop [[f & r] m lst-sql [] lst-args []]
                    (if (some? f)
                        (let [{sql :sql args :args} f]
                            (recur r (concat lst-sql [sql]) (concat lst-args args))
                            )
                        {:sql lst-sql :args lst-args})))
            (lst-token-to-line
                ([ignite group_id dic-args lst_token] (cond (string? lst_token) lst_token
                                                            (map? lst_token) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args lst_token)]
                                                                                 {:sql (my-select-plus/my-array-to-sql sql) :args args})
                                                            :else
                                                            (let [{sql :sql args :args} (lst-token-to-line ignite group_id dic-args lst_token [] [])]
                                                                {:sql (my-select-plus/my-array-to-sql sql) :args args})
                                                            ))
                ([ignite group_id dic-args [f & rs] lst lst-args]
                 (if (some? f)
                     (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args f)]
                         (recur ignite group_id dic-args rs (conj lst (my-select-plus/my-array-to-sql sql)) (concat lst-args args)))
                     {:sql lst :args lst-args})))
            (token-to-sql [ignite group_id dic-args m]
                (if (some? m)
                    (cond (my-lexical/is-seq? m) (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) m))
                          (map? m) (map-token-to-sql ignite group_id dic-args m))))
            (map-token-to-sql
                [ignite group_id dic-args m]
                (if (some? m)
                    (cond
                        (contains? m :sql_obj) (select-to-sql ignite group_id dic-args m)
                        (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-line ignite group_id dic-args m)
                        (contains? m :func-link) (func-link-to-line ignite group_id dic-args m)
                        (contains? m :and_or_symbol) {:sql (get m :and_or_symbol) :args nil} ;(get m :and_or_symbol)
                        (contains? m :keyword) {:sql (get m :keyword) :args nil} ;(get m :keyword)
                        (contains? m :operation) (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) (get m :operation)))
                        (contains? m :comparison_symbol) {:sql (get m :comparison_symbol) :args nil} ; (get m :comparison_symbol)
                        (contains? m :in_symbol) {:sql (get m :in_symbol) :args nil} ; (get m :in_symbol)
                        (contains? m :operation_symbol) {:sql (get m :operation_symbol) :args nil} ; (get m :operation_symbol)
                        (contains? m :join) {:sql (get m :join) :args nil} ;(get m :join)
                        (contains? m :on) (on-to-line ignite group_id dic-args m)
                        (contains? m :comma_symbol) {:sql (get m :comma_symbol) :args nil} ;(get m :comma_symbol)
                        (contains? m :order-item) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (-> m :order-item))]
                                                      {:sql (concat sql [(-> m :order)]) :args args})
                        (contains? m :item_name) (item-to-line dic-args m)
                        (contains? m :table_name) (table-to-line ignite group_id dic-args m)
                        (contains? m :exists) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (get (get m :select_sql) :parenthesis))]
                                                  {:sql (concat [(get m :exists) "("] sql [")"]) :args args})
                        (contains? m :parenthesis) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (get m :parenthesis))]
                                                       (if (contains? m :alias)
                                                           {:sql (concat ["("] sql [")" " " (-> m :alias)]) :args args}
                                                           {:sql (concat ["("] sql [")"]) :args args}))
                        :else
                        (throw (Exception. "select 语句错误！请仔细检查！"))
                        )))
            (on-to-line [ignite group_id dic-args m]
                (if (some? m)
                    (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (get m :on))]
                        {:sql (str/join ["on " (str/join " " sql)]) :args args})
                    ))
            (func-to-line [ignite group_id dic-args m]
                (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                    (let [{sql :sql args :args} (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) (-> m :lst_ps)))]
                        (cond (my-cache/is-func? ignite (str/lower-case (-> m :func-name))) (if-not (empty? (-> m :lst_ps))
                                                                                                {:sql (concat ["my_fun(" (format "'%s'," (-> m :func-name))] sql [")" " as"] [(-> m :alias)]) :args args}
                                                                                                {:sql (concat ["my_fun(" (format "'%s'" (-> m :func-name))] sql [")" " as"] [(-> m :alias)]) :args args})
                              (my-cache/is-scenes? ignite group_id (str/lower-case (-> m :func-name))) (if-not (empty? (-> m :lst_ps))
                                                                                                           {:sql (concat ["my_invoke(" (format "'%s', %s," (-> m :func-name) (MyGson/groupObjToLine group_id))] sql [")" " as"] [(-> m :alias)]) :args args}
                                                                                                           {:sql (concat ["my_invoke(" (format "'%s', %s" (-> m :func-name) (MyGson/groupObjToLine group_id))] sql [")" " as"] [(-> m :alias)]) :args args})
                              :else
                              {:sql (concat [(-> m :func-name) "("] sql [")" " as"] [(-> m :alias)]) :args args}
                              )
                        )
                    (let [{sql :sql args :args} (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) (-> m :lst_ps)))]
                        (cond (my-cache/is-func? ignite (str/lower-case (-> m :func-name))) (if-not (empty? (-> m :lst_ps))
                                                                                                {:sql (concat ["my_fun(" (format "'%s'," (-> m :func-name))] sql [")"] [(-> m :alias)]) :args args}
                                                                                                {:sql (concat ["my_fun(" (format "'%s'" (-> m :func-name))] sql [")"] [(-> m :alias)]) :args args})
                              (my-cache/is-scenes? ignite group_id (str/lower-case (-> m :func-name))) (if-not (empty? (-> m :lst_ps))
                                                                                                           {:sql (concat ["my_invoke(" (format "'%s', %s," (-> m :func-name) (MyGson/groupObjToLine group_id))] sql [")"] [(-> m :alias)]) :args args}
                                                                                                           {:sql (concat ["my_invoke(" (format "'%s', %s" (-> m :func-name) (MyGson/groupObjToLine group_id))] sql [")"] [(-> m :alias)]) :args args})
                              :else
                              {:sql (concat [(-> m :func-name) "("] sql [")"]) :args args}
                              )
                        )
                    ))
            (func-link-to-line [ignite group_id dic-args m]
                (let [{sql :sql args :args} (my-lexical/my-func-line-code m)]
                    (loop [[f & r] args lst-ps [(MyGson/groupObjToLine group_id)] lst-args []]
                        (if (some? f)
                            (if (contains? (-> dic-args :dic) f)
                                (recur r (conj lst-ps "?") (conj lst-args (first (get (-> dic-args :dic) f))))
                                (recur r (conj lst-ps f) lst-args))
                            {:sql (str/join ["my_invoke_link('" sql "'," (str/join "," lst-ps) ")"]) :args lst-args}))))
            (item-to-line [dic-args m]
                (let [{table_alias :table_alias item_name :item_name alias :alias} m]
                    (cond
                        (and (not (Strings/isNullOrEmpty table_alias)) (not (nil? alias)) (not (Strings/isNullOrEmpty alias))) {:sql (str/join [table_alias "." item_name " as " alias]) :args nil}
                        (and (not (Strings/isNullOrEmpty table_alias)) (Strings/isNullOrEmpty alias)) {:sql (str/join [table_alias "." item_name]) :args nil}
                        (and (Strings/isNullOrEmpty table_alias) (Strings/isNullOrEmpty alias)) (if (contains? (-> dic-args :dic) item_name)
                                                                                                    {:sql "?" :args [(first (get (-> dic-args :dic) item_name))]}
                                                                                                    {:sql item_name :args nil}
                                                                                                    )
                        )))
            (table-to-line [ignite group_id dic-args m]
                (if (some? m)
                    (if-let [{schema_name :schema_name table_name :table_name table_alias :table_alias} m]
                        (if-not (Strings/isNullOrEmpty schema_name)
                            (if (Strings/isNullOrEmpty table_alias)
                                {:sql (format "%s.%s" schema_name table_name) :args nil}
                                {:sql (str/join [(format "%s.%s" schema_name table_name) " " table_alias]) :args nil})
                            (if (= (first group_id) 0)
                                (if (Strings/isNullOrEmpty table_alias)
                                    {:sql (format "MY_META.%s" table_name) :args nil}
                                    {:sql (str/join [(format "MY_META.%s" table_name) " " table_alias]) :args nil})
                                (let [schema_name (get_data_set_name ignite group_id)]
                                    (if (Strings/isNullOrEmpty table_alias)
                                        {:sql (format "%s.%s" schema_name table_name) :args nil}
                                        {:sql (str/join [(format "%s.%s" schema_name table_name) " " table_alias]) :args nil}))))
                        )))
            ; 获取 data_set 的名字和对应的表
            (get_data_set_name [^Ignite ignite ^Long group_id]
                ;(when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.dataset_name from my_users_group as g JOIN my_dataset as m ON m.id = g.data_set_id where g.id = ?") (to-array [group_id])))))]
                ;    (first m))
                (second group_id)
                )
            (select-to-sql
                ([ignite group_id dic-args ast]
                 (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (let [{sql :sql args :args} (select-to-sql ignite group_id dic-args ast [] [])]
                                                                                  {:sql sql :args args})
                       (contains? ast :sql_obj) (select_to_sql_single ignite group_id dic-args (get ast :sql_obj))
                       :else
                       (throw (Exception. "select 语句错误！"))))
                ([ignite group_id dic-args [f & rs] lst_rs lst-args]
                 (if (some? f)
                     (if (map? f)
                         (cond (contains? f :sql_obj) (let [{sql :sql args :args} (select_to_sql_single ignite group_id dic-args (get f :sql_obj))]
                                                          (recur ignite group_id dic-args rs (conj lst_rs sql) (filter #(not (nil? %)) (concat lst-args args))))
                               (contains? f :keyword) (recur ignite group_id dic-args rs (conj lst_rs (get f :keyword)) lst-args)
                               :else
                               (throw (Exception. "select 语句错误！"))) (throw (Exception. "select 语句错误！")))
                     {:sql (str/join " " lst_rs) :args (filter #(not (nil? %)) lst-args)})))]
        (select-to-sql ignite group_id dic-args ast)))

; {:dic {"?$p_s_5_c_f_n$#c1894" ["myy" java.lang.String]}, :keys ["?$p_s_5_c_f_n$#c1894"]}
(defn my-ast-to-sql [ignite group_id dic-args ast]
    (let [my-ast (re-select-ast ignite group_id ast)]
        (ast_to_sql ignite group_id dic-args my-ast)))

(defn my-ast-to-sql-no-authority [ignite group_id dic-args ast]
    (ast_to_sql ignite group_id dic-args ast))







































































