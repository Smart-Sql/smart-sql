(ns org.gridgain.plus.dml.my-insert
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.tools.my-util :as my-util]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite)
             (org.apache.ignite.binary BinaryObjectBuilder)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model.db MyScenesCache)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.util List ArrayList)
             (org.log MyCljLogger))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyInsert
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 在函数内部处理 ,
(defn my-comma-fn
    ([lst] (my-comma-fn lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 具体过程：
; 1、获取 insert obj 和 insert view obj 两个对象
; 2、通过 group_id 判断这个语句是在：实时数据集中执行还是批处理数据集
; 3、如果是实时数据集，就要判断是否需要记录 log
; 4、在批处理数据集中，就要判断是否来至与实时树集中的表

;(defn get-insert-items
;    ([tokens] (when-let [m (get-insert-items tokens [] [])]
;                  (if (and (= (count m) 2) (= (count (nth m 0)) (count (nth m 1))))
;                      (letfn [(to-kv [n lst1 lst2 lst_kv]
;                                  (if (> n -1)
;                                      (if (not (my-lexical/is-eq? (nth lst1 n) (nth lst2 n)))
;                                          (recur (dec n) lst1 lst2 (concat lst_kv [{:item_name (nth lst1 n) :item_value (nth lst2 n)}]))
;                                          (recur (dec n) lst1 lst2 lst_kv)) lst_kv))]
;                          (to-kv (dec (count (nth m 0))) (nth m 0) (nth m 1) [])))))
;    ([[f & r] stack lst]
;     (if (some? f)
;         (cond (and (my-lexical/is-eq? f "values") (> (count stack) 0)) (recur r [] stack)
;               :else
;               (recur r (conj stack f) lst)
;               )
;         (if (and (> (count stack) 0) (> (count lst) 0)) [lst stack]))))

(defn get-item-line [[f & r]]
    (if (and (= f "(") (= (last r) ")"))
        (reverse (rest (reverse r)))
        (throw (Exception. "insert 语句错误！"))))

(defn get-insert-items
    ([tokens] (when-let [[columns items] (get-insert-items tokens [] [])]
                  (let [my-columns (my-comma-fn (get-item-line columns)) my-items (my-comma-fn (get-item-line items))]
                      (loop [[f_c & r_c] my-columns [f_m & r_m] my-items lst_kv []]
                          (if (and (some? f_c) (some? f_m))
                              (if-not (= (first f_c) \,)
                                  (recur r_c r_m (concat lst_kv [{:item_name (first f_c) :item_value f_m}]))
                                  (recur r_c r_m lst_kv))
                              lst_kv)))))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "values") (> (count stack) 0)) (recur r [] stack)
               :else
               (recur r (concat stack [f]) lst)
               )
         (if (and (> (count stack) 0) (> (count lst) 0)) [lst stack]))))

(defn get_insert_obj_lst [^clojure.lang.PersistentVector lst]
    (letfn [(insert_obj [[f & r]]
                (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                    (if-let [items (get-insert-items (rest (rest r)))]
                        {:table_name (str/lower-case (second r)) :values items}
                        (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！"))
                        )
                    ))]
        (insert_obj lst)))

; 获取 inset_obj
; 例如："INSERT INTO categories (categoryid, categoryname, description) values (12, 'wudafu', 'meiyy')"
; {:table_name "categories",
;  :values ({:item_name "description", :item_value "'meiyy'"}
;           {:item_name "categoryname", :item_value "'wudafu'"}
;           {:item_name "categoryid", :item_value "12"})}
(defn get_insert_obj [lst]
    (letfn [(insert_obj [[f & r]]
                (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                    (if-let [items (get-insert-items (rest (rest r)))]
                        (assoc (my-lexical/get-schema (str/lower-case (second r))) :values items)
                        ;{:table_name (str/lower-case (second r)) :values items}
                        (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！"))
                        )
                    ))]
        (insert_obj lst)))

; 例如：从 my_insert_views 中获取 code = "INSERT INTO City(Name, District)"
; 转换为 view_obj :
; {:table_name "City", :lst #{"District" "Name"}}
(defn get_view_obj [^Ignite ignite ^Long group_id ^String table_name]
    (if-let [lst_rs (first (.getAll (.query (.cache ignite "my_insert_views") (.setArgs (SqlFieldsQuery. "select m.code from my_insert_views as m join my_group_view as v on m.id = v.view_id where m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [table_name group_id "增"])))))]
        (if (some? lst_rs)
            (letfn [(get_insert_view_items [[f & r] lst]
                        (if (some? f)
                            (if (not (or (= f "(") (= f ")") (= f ",")))
                                (recur r (concat lst [f]))
                                (recur r lst))
                            lst))
                    (get_insert_view_obj [[f & r]]
                        (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
                            (if-let [items (get_insert_view_items (rest (rest r)) #{})]
                                {:table_name (second r) :lst items})
                            ))]
                (get_insert_view_obj (my-lexical/to-back (nth lst_rs 0))))
            )
        ))

; 获取表中 PK列 和 数据列
; 输入表名获取 "Categories"
; {:pk ({:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false}),
; :data ({:column_name "categoryname", :column_type "varchar", :pkid false, :auto_increment false}
;        {:column_name "description", :column_type "varchar", :pkid false, :auto_increment false}
;        {:column_name "picture", :column_type "varchar", :pkid false, :auto_increment false})}
(defn get_pk_data [^Ignite ignite ^String table_name]
    (when-let [it (.iterator (.query (.cache ignite "my_meta_tables") (.setArgs (doto (SqlFieldsQuery. "select m.column_name, m.column_type, m.pkid, m.auto_increment from table_item as m join my_meta_tables as t on m.table_id = t.id where t.table_name = ?")
                                                                                    (.setLazy true)) (to-array [table_name]))))]
        (letfn [
                ; 从 iterator 中获取 pk列和数据列
                (get_pk_data_it [it lst_pk lst_data]
                    (if (.hasNext it)
                        (when-let [row (.next it)]
                            (if (true? (.get row 2))
                                (recur it (concat lst_pk [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]) lst_data)
                                (recur it lst_pk (concat lst_data [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]))
                                ))
                        {:pk lst_pk :data lst_data}))]
            (get_pk_data_it it [] []))))

; 获取 insert obj 和 insert view obj 两个对象
; insert obj: get_insert_obj
; insert view obj: get_view_obj

; pk_data: (get_pk_data ignite "Categories")
; insert_obj: (get_insert_obj ignite "insert into Categories values(1,'Beverages','Soft drinks, coffees, teas, beers, and ales', '')")
(defn get_pk_data_with_data [pk_data insert_obj]
    (let [{values :values} insert_obj]
        (letfn [(
                    ; 第一个参数是：
                    ; :values ({:item_name "picture", :item_value "''"}
                    ;           {:item_name "description", :item_value "'Soft drinks, coffees, teas, beers, and ales'"}
                    ;           {:item_name "categoryname", :item_value "'Beverages'"}
                    ;           {:item_name "categoryid", :item_value "1"})
                    ; 第二个参数是：
                    ; column：{:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false}
                    get_value [[f & r] column]
                    (if (some? f)
                        (if (my-lexical/is-eq? (-> column :column_name) (-> f :item_name))
                            (assoc column :item_value (-> f :item_value))
                            (recur r column)))
                    )
                (
                    ; 第一个参数是：({:column_name "categoryname", :column_type "varchar", :pkid false, :auto_increment false}
                    ;         {:column_name "description", :column_type "varchar", :pkid false, :auto_increment false}
                    ;         {:column_name "picture", :column_type "varchar", :pkid false, :auto_increment false})
                    ; 第二个参数是： :values ({:item_name "picture", :item_value "''"}
                    ;           {:item_name "description", :item_value "'Soft drinks, coffees, teas, beers, and ales'"}
                    ;           {:item_name "categoryname", :item_value "'Beverages'"}
                    ;           {:item_name "categoryid", :item_value "1"})
                    ; 返回的结果：({:column_name "categoryname", :column_type "varchar", :item_value "'Beverages'", :pkid false, :auto_increment false}
                    ;         {:column_name "description", :column_type "varchar", :item_value "'Soft drinks, coffees, teas, beers, and ales'", :pkid false, :auto_increment false}
                    ;         {:column_name "picture", :column_type "varchar", :item_value nil, :pkid false, :auto_increment false})
                    get_rs
                    ([lst_column values] (get_rs lst_column values []))
                    ([[f & r] values lst]
                     (if (some? f)
                         (let [m (get_value values f)]
                             (if-not (nil? m)
                                 (recur r values (concat lst [m]))
                                 (recur r values (concat lst [(assoc m :item_value nil)]))))
                         lst))
                    )] {:pk_rs (get_rs (-> pk_data :pk) values) :data_rs (get_rs (-> pk_data :data) values)})
        ))

; {:table_name "categories",
;  :values ({:item_name "description", :item_value "'meiyy'"}
;           {:item_name "categoryname", :item_value "'wudafu'"}
;           {:item_name "categoryid", :item_value "12"})}
(defn insert_users_group [^Ignite ignite ^clojure.lang.PersistentArrayMap insert_obj]
    (letfn [(user_group_sql [^clojure.lang.PersistentArrayMap insert_obj]
                (if-let [{table_name :table_name values :values} insert_obj]
                    (loop [[f & r] values sb_item (StringBuilder.) sb_vs (StringBuilder.)]
                        (if (some? f)
                            (if (contains? #{"group_name" "data_set_id" "group_type"} (str/lower-case (-> f :item_name)))
                                (recur r (doto sb_item (.append (format "%s," (-> f :item_name)))) (doto sb_vs (.append (format "%s," (-> f :item_value)))))
                                (if (false? (my-lexical/is-eq? (-> f :item_name) "id"))
                                    (throw (Exception. (format "插入数据的列 %s 在 my_users_group 表中不存在！" (-> f :item_name))))
                                    (recur r sb_item sb_vs)))
                            (format "insert into %s (%s id) values (%s ?)" table_name (.toString sb_item) (.toString sb_vs)))
                        )))]
        (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. (user_group_sql insert_obj)) (to-array [(.incrementAndGet (.atomicSequence ignite "my_users_group" 0 true))])))))
    )

; {:table_name "my_dataset",
;  :values ({:item_name "id", :item_value "1"}
;           {:item_name "is_real", :item_value "1"}
;           {:item_name "dataset_name", :item_value "'real_ds'"})}
(defn insert_data_set [^Ignite ignite ^clojure.lang.PersistentArrayMap insert_obj]
    (letfn [(data_set_sql [^clojure.lang.PersistentArrayMap insert_obj]
                (if-let [{table_name :table_name values :values} insert_obj]
                    (loop [[f & r] values sb_item (StringBuilder.) sb_vs (StringBuilder.)]
                        (if (some? f)
                            (if (contains? #{"is_real" "dataset_name"} (str/lower-case (-> f :item_name)))
                                (recur r (doto sb_item (.append (format "%s," (-> f :item_name)))) (doto sb_vs (.append (format "%s," (-> f :item_value)))))
                                (if (false? (my-lexical/is-eq? (-> f :item_name) "id"))
                                    (throw (Exception. (format "插入数据的列 %s 在 my_dataset 表中不存在！" (-> f :item_name))))
                                    (recur r sb_item sb_vs)))
                            (format "insert into %s (%s id) values (%s ?)" table_name (.toString sb_item) (.toString sb_vs)))
                        )))]
        (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. (data_set_sql insert_obj)) (to-array [(.incrementAndGet (.atomicSequence ignite "my_dataset" 0 true))])))))
    )

; 添加额外的 pk
; pk_rs: [{:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false, :item_value "1"}]
(defn get_plus_pk
    ([pk_rs] (get_plus_pk pk_rs []))
    ([[f_pk & r_pk] lst]
     (if (some? f_pk)
         (recur r_pk (concat lst [(assoc f_pk :pkid false) (assoc f_pk :column_name (format "%s_pk" (-> f_pk :column_name)) :pkid true :auto_increment false)]))
         lst)))

; 执行 元表的sql
; insert_obj:
; {:table_name "categories",
;  :values ({:item_name "description", :item_value "'meiyy'"}
;           {:item_name "categoryname", :item_value "'wudafu'"}
;           {:item_name "categoryid", :item_value "12"})}
(defn insert_meta_table [^Ignite ignite ^clojure.lang.PersistentArrayMap insert_obj]
    (if-let [{table_name :table_name values :values} insert_obj]
        (cond (my-lexical/is-eq? table_name "my_users_group") (insert_users_group ignite insert_obj)
              (my-lexical/is-eq? table_name "my_dataset") (insert_data_set ignite insert_obj)
            )))

; 对于超级管理员执行 insert 语句
(defn insert_run_super_admin [^Ignite ignite lst-sql]
    (if-let [insert_obj (get_insert_obj lst-sql)]
        (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case (-> insert_obj :table_name)))
            (insert_meta_table ignite insert_obj))
        ))


; 判断是否有权限
; 参数一：({:item_name "description", :item_value "'meiyy'"}
;          {:item_name "categoryname", :item_value "'wudafu'"}
;          {:item_name "categoryid", :item_value "12"})
; 参数二：{:table_name "City", :lst #{"District" "Name"}}
(defn get-authority [[f & r] view_items]
    (if-not (empty? view_items)
        (if (some? f)
            (if-not (contains? view_items (str/lower-case (:item_name f)))
                (throw (Exception. (String/format "字段 %s 没有添加的权限" (object-array [(:item_name f)]))))
                (recur r view_items)))))

(defn insert_obj_to_db [^Ignite ignite ^Long group_id ^String schema_name ^String table_name ^clojure.lang.PersistentArrayMap pk_data]
    ())

(defn insert_obj_to_db_no_log [^Ignite ignite ^Long group_id ^String schema_name ^String table_name ^clojure.lang.PersistentArrayMap pk_data]
    ())

; 执行需要保持 log 的 insert 语句
(defn insert_run_log [^Ignite ignite ^Long group_id lst-sql]
    (let [insert_obj (get_insert_obj lst-sql)]
        (if-not (my-lexical/is-eq? (-> insert_obj :schema_name) "my_meta")
            (let [view_obj (get_view_obj ignite group_id (-> insert_obj :table_name))]
                (if (nil? (get-authority (-> insert_obj :values) (-> view_obj :lst)))
                    (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
                        (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                            (insert_obj_to_db ignite group_id (-> insert_obj :schema_name) (-> insert_obj :table_name) pk_with_data)
                            )
                        )
                    ))
            (throw (Exception. "没有执行 insert 语句的权限！")))
        ))

; 执行不需要保持 log 的 insert 语句
(defn insert_run_no_log [^Ignite ignite ^Long group_id lst-sql]
    (let [insert_obj (get_insert_obj lst-sql)]
        (let [view_obj (get_view_obj ignite group_id (-> insert_obj :table_name))]
            (if (nil? (get-authority (-> insert_obj :values) (-> view_obj :lst)))
                (if-let [pk_data (get_pk_data ignite (-> insert_obj :table_name))]
                    (if-let [pk_with_data (get_pk_data_with_data pk_data insert_obj)]
                        (insert_obj_to_db_no_log ignite group_id (-> insert_obj :schema_name) (-> insert_obj :table_name) pk_with_data)
                        )
                    )
                ))
        ))

; 1、判断用户组在实时数据集，还是非实时数据
; 如果是非实时数据集,
; 获取表名后，查一下，表名是否在 对应的 my_dataset_table 中，如果在就不能添加，否则直接执行 insert sql
; 2、如果是在实时数据集是否需要 log
(defn insert_run [^Ignite ignite ^Long group_id lst-sql sql]
    (if (= group_id 0)
        ; 超级用户
        ;(insert_run_super_admin ignite sql)
        (try
            (.getAll (.query (.cache ignite "my_meta_table") (SqlFieldsQuery. sql)))
            (catch Exception e
                (cond (re-find #"(?i)Duplicate\s+key\s+during\s+INSERT\s+" (.getMessage e)) (throw (Exception. "主键已经存在，不能重复插入！"))
                      :else
                      (throw e)
                      )))
        ; 不是超级用户就要先看看这个用户组在哪个数据集下
        (if (true? (.isDataSetEnabled (.configuration ignite)))
            (my-lexical/trans ignite (insert_run_log ignite group_id lst-sql))
            (my-lexical/trans ignite (insert_run_no_log ignite group_id lst-sql))))
    )











































