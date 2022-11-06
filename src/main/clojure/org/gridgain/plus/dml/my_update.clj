(ns org.gridgain.plus.dml.my-update
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (cn.plus.model.ddl MySchemaTable MyDataSet)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
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

(defn my-where-items [ps]
    (letfn [(even-items [ps]
                (let [my-count (count ps)]
                    (loop [i 0 flag true]
                        (if (< i (- my-count 1))
                            (if (even? (+ i 1))
                                (if-not (and (contains? (nth ps i) :and_or_symbol) (= (-> (nth ps i) :and_or_symbol) "and"))
                                    (recur my-count false)
                                    (recur (+ i 1) flag))
                                (recur (+ i 1) flag))
                            flag)))
                )
            (get-where-items [ps]
                (cond (and (my-lexical/is-seq? ps) (= (count ps) 3) (contains? (second ps) :comparison_symbol) (= (-> (second ps) :comparison_symbol) "=") (contains? (first ps) :const) (false? (-> (first ps) :const))) [{:key (first ps) :value (last ps)}]
                      (and (my-lexical/is-seq? ps) (my-lexical/is-seq? (first ps)) (even-items ps)) (loop [[f & r] ps lst []]
                                                                                                        (if (some? f)
                                                                                                            (if (my-lexical/is-seq? f)
                                                                                                                (let [m-item (get-where-items f)]
                                                                                                                    (recur r (conj lst m-item)))
                                                                                                                (recur r lst)
                                                                                                                )
                                                                                                            lst))
                      (and (map? ps) (contains? ps :parenthesis)) (get-where-items (-> ps :parenthesis)))
                )]
        (let [lst-ps (get-where-items ps)]
            (if (empty? (filter #(nil? %) lst-ps))
                lst-ps
                nil))))

(defn is-column_name [column_name where-items]
    (loop [[f & r] where-items flag nil]
        (if (some? f)
            (cond (map? f) (if (my-lexical/is-eq? (-> f :key :item_name) column_name)
                               (recur nil f)
                               (recur r flag))
                  (my-lexical/is-seq? f) (if (my-lexical/is-eq? (-> (first f) :key :item_name) column_name)
                                             (recur nil (first f))
                                             (recur r flag)))
            flag)))

(defn get-k-v [pk where-items]
    (loop [[f & r] pk lst []]
        (if (some? f)
            (if-let [m (is-column_name (-> f :column_name) where-items)]
                (recur r (conj lst (assoc m :column_type (-> f :column_type))))
                (recur nil nil))
            (if (and (not (nil? lst)) (= (count lst) (count where-items)))
                lst))))

(defn update-table
    ([lst] (update-table lst nil [] []))
    ([[f & r] my-set stack lst]
     (if (some? f)
         (cond (and (nil? my-set) (not (my-lexical/is-eq? f "set"))) (recur r my-set (conj stack f) lst)
               (and (nil? my-set) (my-lexical/is-eq? f "set")) (recur r "set" stack lst)
               (not (nil? my-set)) (recur r my-set stack (conj lst f))
               )
         (cond (and (= (count stack) 3) (= (second stack) ".")) {:schema_name (str/lower-case (first stack)) :table_name (str/lower-case (last stack)) :rs_lst lst}
               (= (count stack) 1) {:schema_name "" :table_name (str/lower-case (first stack)) :rs_lst lst}
               :else
               (throw (Exception. "update 语句错误，要么是dataSetName.tableName 或者是 tableName"))
               ))))

; 获取名字
(defn get_table_name [lst]
    (if (and (not (empty? lst)) (my-lexical/is-eq? (first lst) "update"))
        (update-table (rest lst))))

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
    (if-let [{schema_name :schema_name table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
        (if-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
            (if-let [items (get_item_lst items_line)]
                {:schema_name schema_name :table_name table_name :items (item_jsons items) :where_line where_line}
                ;(assoc (my-lexical/get-schema (str/lower-case table_name)) :items (item_jsons items) :where_line where_line)
                )
            (throw (Exception. "更新数据的语句错误！")))
        (throw (Exception. "更新数据的语句错误！"))))

(defn my-where-line [where-line-lst args-dic]
    (loop [[f & r] where-line-lst where-lst [] args []]
        (if (some? f)
            (if (contains? args-dic f)
                (recur r (conj where-lst "?") (conj args (first (get args-dic f))))
                (recur r (conj where-lst f) args))
            [where-lst args])))

(defn my-view-db [^Ignite ignite group_id ^String schema_name ^String table_name]
    (if-let [code (my-lexical/get-update-code ignite schema_name table_name group_id)]
        (let [rs-lst (-> (update-table (rest code)) :rs_lst)]
            (let [{items_line :items_line where_line :where_line} (get_items rs-lst)]
                (loop [[f & r] (my-select/my-get-items items_line) lst-rs #{}]
                    (if (some? f)
                        (if (= (count f) 1)
                            (recur r (conj lst-rs (str/lower-case (first f))))
                            (recur r lst-rs))
                        {:items lst-rs :where_line where_line}))))))

(defn my-has-authority-item [[f & r] v_items]
    (if (some? f)
        (if (contains? v_items (str/lower-case (-> f :item_name)))
            (recur r v_items)
            (throw (Exception. (format "%s列没有修改的权限！" (-> f :item_name)))))))

; 合并 where
(defn merge_where [where_line v_where_line]
    (cond (and (some? where_line) (not (some? v_where_line))) where_line
          (and (some? v_where_line) (not (some? where_line))) v_where_line
          (and (some? where_line) (some? v_where_line)) (concat ["("] where_line [") and ("] v_where_line [")"])
          ))

(defn my-authority [^Ignite ignite group_id lst-sql args-dic]
    (when-let [{schema_name :schema_name table_name :table_name items :items where_line :where_line} (get_json lst-sql)]
        (let [[where-lst args] (my-where-line where_line args-dic)]
            (cond (and (my-lexical/is-eq? schema_name "my_meta") (= (first group_id) 0)) (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name))
                                                                                             (throw (Exception. (format "%s 没有修改数据的权限！" table_name)))
                                                                                             {:schema_name schema_name :table_name table_name :items items :where_line where-lst :args args})
                  (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "用户不存在或者没有权限！修改数据！"))
                  (= (first group_id) 0) (if (and (or (my-lexical/is-eq? schema_name "my_meta") (my-lexical/is-str-empty? schema_name)) (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name)))
                                             (throw (Exception. (format "%s 没有修改数据的权限！" table_name)))
                                             (if (my-lexical/is-str-not-empty? schema_name)
                                                 {:schema_name schema_name :table_name table_name :items items :where_line where-lst :args args}
                                                 {:schema_name (second group_id) :table_name table_name :items items :where_line where-lst :args args}))
                  (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "用户不存在或者没有权限！添加数据！"))
                  (and (my-lexical/is-str-empty? schema_name) (my-lexical/is-str-not-empty? (second group_id))) (if-let [{v_items :items v_where_line :where_line} (my-view-db ignite group_id (second group_id) table_name)]
                                                                                                                    (if (nil? (my-has-authority-item items v_items))
                                                                                                                        (let [where-lst (merge_where where-lst v_where_line)]
                                                                                                                            {:schema_name (second group_id) :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args})
                                                                                                                        {:schema_name (second group_id) :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args})
                                                                                                                    {:schema_name (second group_id) :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args}
                                                                                                                    )
                  (and (my-lexical/is-eq? schema_name (second group_id)) (my-lexical/is-str-not-empty? (second group_id))) (if-let [{v_items :items v_where_line :where_line} (my-view-db ignite group_id schema_name table_name)]
                                                                                                                               (if (nil? (my-has-authority-item items v_items))
                                                                                                                                   (let [where-lst (merge_where where-lst v_where_line)]
                                                                                                                                       {:schema_name schema_name :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args})
                                                                                                                                   {:schema_name schema_name :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args})
                                                                                                                               {:schema_name schema_name :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args}
                                                                                                                               )
                  (and (not (my-lexical/is-eq? schema_name (second group_id))) (my-lexical/is-str-not-empty? schema_name) (my-lexical/is-str-not-empty? (second group_id))) (if-let [{v_items :items v_where_line :where_line} (my-view-db ignite group_id schema_name table_name)]
                                                                                                                                                                                (if (nil? (my-has-authority-item items v_items))
                                                                                                                                                                                    (let [where-lst (merge_where where-lst v_where_line)]
                                                                                                                                                                                        {:schema_name schema_name :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args})
                                                                                                                                                                                    {:schema_name schema_name :table_name table_name :items items :where_line where-lst :where-objs (my-select/sql-to-ast where-lst) :args args})
                                                                                                                                                                                (if (not (my-lexical/is-eq? schema_name "public"))
                                                                                                                                                                                    (throw (Exception. "用户不存在或者没有权限！修改数据！"))
                                                                                                                                                                                    {:schema_name "public" :table_name table_name :items items :where_line where-lst :args args})
                                                                                                                                                                                )
                  ))
        ))

(defn my-no-authority [^Ignite ignite group_id lst-sql args-dic]
    (when-let [{schema_name :schema_name table_name :table_name items :items where_line :where_line} (get_json lst-sql)]
        (let [[where-lst args] (my-where-line where_line args-dic)]
            {:schema_name schema_name :table_name table_name :items items :where_line where-lst :args args})
        ))

(defn my-items
    ([lst] (loop [[f & r] (my-items lst []) rs []]
               (if (some? f)
                   (recur r (conj rs (str/lower-case (-> f :item_name))))
                   rs)))
    ([[f & r] lst]
     (if (some? f)
         (if-let [items-lst (my-lexical/my-ast-items (-> f :item_obj))]
             (recur r (concat lst items-lst))
             (recur r lst))
         lst)))

(defn get-rows [^Ignite ignite group_id ^String schema_name ^String table_name]
    (cond (my-lexical/is-eq? schema_name "public") (.getAll (.query (.cache ignite "my_meta_tables") (doto (SqlFieldsQuery. "SELECT m.column_name, m.pkid, m.column_type FROM table_item AS m INNER JOIN my_meta_tables AS o ON m.table_id = o.id WHERE o.table_name = ?")
                                                                                                         (.setArgs (to-array [table_name])))))
          (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "没有修改权限！"))
          :else
          (.getAll (.query (.cache ignite "my_meta_tables") (doto (SqlFieldsQuery. "SELECT m.column_name, m.pkid, m.column_type FROM table_item AS m, my_meta_tables AS o, my_dataset as ds WHERE m.table_id = o.id and ds.id = o.data_set_id and o.table_name = ? and ds.schema_name = ?")
                                                                (.setArgs (to-array [table_name schema_name])))))
          ))

; 1、获取表的 PK 定义
(defn get_pk_def [^Ignite ignite group_id ^String schema_name ^String table_name]
    (when-let [rows (get-rows ignite group_id schema_name table_name)]
        (loop [[f & r] rows lst [] lst_pk [] dic {}]
            (if (some? f)
                (if (= (.get f 1) true) (recur r lst (conj lst_pk (.get f 0)) (assoc dic (.get f 0) (.get f 2)))
                                        (recur r (conj lst (.get f 0)) lst_pk (assoc dic (.get f 0) (.get f 2))))
                {:lst lst :lst_pk lst_pk :dic dic}))))

(defn get_pk_def_map [^Ignite ignite group_id ^String schema_name ^String table_name]
    (when-let [{lst :lst lst_pk :lst_pk dic :dic} (get_pk_def ignite group_id schema_name table_name)]
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

(defn query-lst [dic lst-items is-pk]
    (loop [[f & r] lst-items rs []]
        (if (some? f)
            (if (contains? dic f)
                (recur r (conj rs {:column_name f :column_type (get dic f) :is-pk is-pk}))
                (recur r rs))
            rs)))

(defn my-query-lst [my-obj lst-items]
    (loop [[f & r] (concat (query-lst (-> my-obj :dic) (-> my-obj :lst_pk) true) (query-lst (-> my-obj :dic) lst-items false)) index 0 rs []]
        (if (some? f)
            (recur r (+ index 1) (conj rs (assoc f :index index)))
            rs)))

(defn my-query-line [query-lst]
    (loop [[f & r] query-lst rs []]
        (if (some? f)
            (recur r (conj rs (-> f :column_name)))
            (str/join "," rs))))

(defn my-pk-def-map [^Ignite ignite group_id ^String schema_name ^String table_name update-obj]
    (let [my-obj (get_pk_def ignite group_id schema_name table_name) lst-items (my-items (-> update-obj :items))]
        (let [query-lst (my-query-lst my-obj lst-items)]
            {:query-line (my-query-line query-lst) :query-lst query-lst :dic (-> my-obj :dic)})))

(defn my_update_query_sql-0 [^Ignite ignite ^Long group_id obj]
    (letfn [(get_items_type
                ([items dic] (get_items_type items dic []))
                ([[f & r] dic lst]
                 (if (some? f)
                     (if (contains? dic (str/lower-case (-> f :item_name)))
                         (recur r dic (conj lst (assoc f :type (get dic (str/lower-case (-> f :item_name))))))
                         (recur r dic lst))
                     lst)))]
        (let [{query-line :query-line query-lst :query-lst dic :dic} (my-pk-def-map ignite group_id (-> obj :schema_name) (-> obj :table_name) obj)]
            {:schema_name (-> obj :schema_name) :table_name (-> obj :table_name) :query-lst query-lst :sql (format "select %s from %s.%s where %s" query-line (-> obj :schema_name) (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_line))) :args (-> obj :args) :items (get_items_type (-> obj :items) dic [])})))

(declare get-column get-column_type merge-pk-where-items get_pk_line pk-where get-column-type get_items_type)

(defn get-column [[f & r] column_name]
    (if (some? f)
        (if (my-lexical/is-eq? (-> f :column_name) column_name)
            (-> f :column_type)
            (recur r column_name))
        nil))
(defn get-column_type [pk data column_name]
    (if-let [ct (get-column pk column_name)]
        ct
        (if-let [m (get data (str/lower-case column_name))]
            (.getColumn_type m))
        ;(get-column data column_name)
        ))

(defn merge-pk-where-items [pk data obj args-dic]
    (if-let [lst-items (my-items (-> obj :items))]
        (loop [[f & r] (concat pk lst-items) index 0 rs [] ht #{}]
            (if (some? f)
                (cond (and (map? f) (contains? f :column_name) (not (contains? ht (-> f :column_name)))) (recur r (+ index 1) (conj rs {:column_name (-> f :column_name) :column_type (-> f :column_type) :index index :is-pk true}) (conj ht (-> f :column_name)))
                      (and (string? f) (not (contains? args-dic f)) (not (contains? ht f))) (recur r (+ index 1) (conj rs {:column_name f :column_type (get-column_type pk data f) :index index :is-pk false}) (conj ht f))
                      :else
                      (recur r (+ index 1) rs ht))
                rs))
        (loop [[f & r] pk index 0 rs [] ht #{}]
            (if (some? f)
                (if-not (contains? ht (-> f :column_name))
                    (recur r (+ index 1) (conj rs {:column_name (-> f :column_name) :column_type (-> f :column_type) :index index :is-pk true}) (conj ht (-> f :column_name)))
                    (recur r (+ index 1) rs ht))
                rs))))

(defn get_pk_line [[f & r] lst]
    (if (some? f)
        (recur r (conj lst (-> f :column_name)))
        (if-not (empty? lst)
            (str/join "," lst)
            "")))
(defn pk-where [pk where-objs]
    (get-k-v pk (my-where-items where-objs)))
(defn get-column-type [item_name data]
    (loop [[f & r] data column-type nil]
        (if (some? f)
            (if (my-lexical/is-eq? (-> f :column_name) item_name)
                (recur nil (-> f :column_type))
                (recur r column-type))
            column-type)))

(defn get-column-type-ex [item_name data]
    (if-let [m (get data (str/lower-case item_name))]
        (.getColumn_type m)))

(defn get_items_type [items data]
    (loop [[f & r] items lst []]
        (if (some? f)
            (recur r (conj lst (assoc f :type (get-column-type-ex (-> f :item_name) data))))
            lst)))

(defn my_update_query_sql [^Ignite ignite group_id obj args-dic]
    (if-let [{pk :pk data :data} (.get (.cache ignite "table_ast") (MySchemaTable. (-> obj :schema_name) (-> obj :table_name)))]
        (let [pk-where-item (merge-pk-where-items pk data obj args-dic)]
            (if-let [k-v (pk-where pk (-> obj :where-objs))]
                {:schema_name (-> obj :schema_name) :table_name (-> obj :table_name) :k-v k-v :args (-> obj :args) :items (get_items_type (-> obj :items) data)}
                {:schema_name (-> obj :schema_name) :table_name (-> obj :table_name) :query-lst pk-where-item :sql (format "select %s from %s.%s where %s" (get_pk_line pk-where-item []) (-> obj :schema_name) (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_line))) :args (-> obj :args) :items (get_items_type (-> obj :items) data)})))
    )

(defn my_update_obj-0 [^Ignite ignite group_id lst-sql args-dic]
    (if-let [m (my-authority ignite group_id lst-sql args-dic)]
        (if (and (boolean? m) (true? m))
            true
            (if-let [us (my_update_query_sql ignite group_id m args-dic)]
                us
                (throw (Exception. "更新语句字符串错误！"))))
        (throw (Exception. "更新语句字符串错误！"))))

(defn my_update_obj [^Ignite ignite group_id lst-sql args-dic]
    (if-let [m (my-authority ignite group_id lst-sql args-dic)]
        (if-let [us (my_update_query_sql ignite group_id m args-dic)]
            us
            (throw (Exception. "更新语句字符串错误！")))
        (throw (Exception. "更新语句字符串错误！"))))

(defn my_update_obj-authority-0 [^Ignite ignite group_id lst-sql args-dic]
    (if-let [m (my-no-authority ignite group_id lst-sql args-dic)]
        (if (and (boolean? m) (true? m))
            true
            (if-let [us (my_update_query_sql ignite group_id m args-dic)]
                us
                (throw (Exception. "更新语句字符串错误！"))))
        (throw (Exception. "更新语句字符串错误！"))))

(defn my_update_obj-authority [^Ignite ignite group_id lst-sql args-dic]
    (if-let [m (my-no-authority ignite group_id lst-sql args-dic)]
        (if-let [us (my_update_query_sql ignite group_id m args-dic)]
            us
            (throw (Exception. "更新语句字符串错误！")))
        (throw (Exception. "更新语句字符串错误！"))))























































