(ns org.gridgain.plus.dml.my-insert
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite)
             (org.apache.ignite.binary BinaryObjectBuilder)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model.db MyScenesCache)
             (cn.plus.model.ddl MySchemaTable)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (java.util List ArrayList)
             (org.log MyCljLogger))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyInsert
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
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

(defn get-item-line [[f & r]]
    (if (and (= f "(") (= (last r) ")"))
        ;(reverse (rest (reverse r)))
        (drop-last r)
        (throw (Exception. "insert 语句错误！"))))

(defn insert-body
    ([lst] (insert-body lst nil [] []))
    ([[f & r] flag lst-sn lst]
     (if (some? f)
         (cond (not (nil? flag)) (recur r flag lst-sn (conj lst f))
               (and (nil? flag) (= f "(")) (recur r true lst-sn (conj lst f))
               :else
               (recur r nil (conj lst-sn f) lst)
               )
         (cond (and (= (count lst-sn) 3) (= (second lst-sn) ".")) {:schema_name (first lst-sn) :table_name (last lst-sn) :vs-line lst}
               (= (count lst-sn) 1) {:schema_name "" :table_name (last lst-sn) :vs-line lst}
               :else
               (throw (Exception. "插入语句格式错误！"))
               ))))

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

(defn get-all-items-name [pk items]
    (letfn [(get-pk-auto-items [pk]
                (loop [[f & r] pk lst #{}]
                    (if (some? f)
                        (if (true? (-> f :auto_increment))
                            (recur r (conj lst (-> f :column_name)))
                            (recur r lst))
                        lst)))]
        (loop [[f & r] items pk-lst (get-pk-auto-items pk) lst []]
            (if (some? f)
                (if (contains? pk-lst (str/lower-case (-> f :item_name)))
                    (recur r pk-lst lst)
                    (recur r pk-lst (conj lst (str/lower-case (-> f :item_name)))))
                lst))))

(defn my-authority [^Ignite ignite group_id ^String schema_name ^String table_name]
    (letfn [(get_insert_view_items [[f & r] lst]
                (if (some? f)
                    (if (not (or (= f "(") (= f ")") (= f ",")))
                        (recur r (conj lst (str/lower-case f)))
                        (recur r lst))
                    lst))]
        (if-let [my-lst (my-lexical/get-insert-code ignite schema_name table_name group_id)]
            (let [{schema_name :schema_name table_name :table_name vs-line :vs-line} (insert-body (rest (rest my-lst)))]
                (let [vs (get_insert_view_items vs-line #{})]
                    {:schema_name schema_name :table_name table_name :v-items vs})))))

;(defn has-my-authority [[f & r] v-items]
;    (cond (contains? v-items (str/lower-case (-> f :item_name))) (recur r v-items)
;          :else (throw (Exception. "没有 %s 字段的插入权限！" (-> f :item_name)))
;          ))

(defn has-my-authority [pk items v-items]
    (loop [[f & r] (get-all-items-name pk items)]
        (if (some? f)
            (if (contains? v-items f)
                (recur r)
                (throw (Exception. (format "没有 %s 字段的插入权限！" f)))))))

;(defn my_insert_obj [^Ignite ignite group_id [f & r]]
;    (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
;        (let [{schema_name :schema_name table_name :table_name vs-line :vs-line} (insert-body (rest r))]
;            (if (and (or (my-lexical/is-eq? schema_name "my_meta") (= schema_name "")) (= (first group_id) 0))
;                {:schema_name "MY_META" :table_name table_name :values (get-insert-items vs-line)}
;                (if-let [items (get-insert-items vs-line)]
;                    (if-let [{v-items :v-items} (my-authority ignite group_id schema_name table_name)]
;                        (if (nil? (has-my-authority items v-items))
;                            {:schema_name schema_name :table_name table_name :values items})
;                        {:schema_name schema_name :table_name table_name :values items})
;                    (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！")))))
;        ))

; 获取表中 PK列 和 数据列
; 输入表名获取 "Categories"
; {:pk ({:column_name "categoryid", :column_type "integer", :pkid true, :auto_increment false}),
; :data ({:column_name "categoryname", :column_type "varchar", :pkid false, :auto_increment false}
;        {:column_name "description", :column_type "varchar", :pkid false, :auto_increment false}
;        {:column_name "picture", :column_type "varchar", :pkid false, :auto_increment false})}
(defn get_pk_data [^Ignite ignite ^String schema_name ^String table_name]
    (.get (.cache ignite "table_ast") (MySchemaTable. schema_name table_name)))

(defn schema-name [group_id schema_name]
    (if (my-lexical/is-str-not-empty? schema_name)
        schema_name
        (second group_id)))

(defn my_insert_obj [^Ignite ignite group_id [f & r]]
    (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
        (let [{schema_name_0 :schema_name table_name :table_name vs-line :vs-line} (insert-body (rest r))]
            (let [schema_name (schema-name group_id schema_name_0)]
                (let [{pk :pk} (get_pk_data ignite schema_name table_name)]
                    (cond (and (or (my-lexical/is-eq? schema_name "my_meta") (my-lexical/is-str-empty? schema_name)) (= (first group_id) 0)) (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name))
                                                                                                                                                 (throw (Exception. (format "%s 没有插入数据的权限！" table_name)))
                                                                                                                                                 {:schema_name "MY_META" :table_name table_name :values (get-insert-items vs-line)})
                          (= (first group_id) 0) (if (and (or (my-lexical/is-eq? schema_name "my_meta") (my-lexical/is-str-empty? schema_name)) (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name)))
                                                     (throw (Exception. (format "%s 没有插入数据的权限！" table_name)))
                                                     (if (my-lexical/is-str-not-empty? schema_name)
                                                         {:schema_name schema_name :table_name table_name :values (get-insert-items vs-line)}
                                                         {:schema_name (second group_id) :table_name table_name :values (get-insert-items vs-line)}))
                          (and (my-lexical/is-eq? schema_name "my_meta") (> (first group_id) 0)) (throw (Exception. "用户不存在或者没有权限！添加数据！"))
                          (and (my-lexical/is-str-empty? schema_name) (my-lexical/is-str-not-empty? (second group_id))) (if-let [items (get-insert-items vs-line)]
                                                                                                                            (if-let [{v-items :v-items} (my-authority ignite group_id (second group_id) table_name)]
                                                                                                                                (if (nil? (has-my-authority pk items v-items))
                                                                                                                                    {:schema_name (second group_id) :table_name table_name :values items})
                                                                                                                                {:schema_name (second group_id) :table_name table_name :values items})
                                                                                                                            (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！")))
                          (and (my-lexical/is-eq? schema_name (second group_id)) (my-lexical/is-str-not-empty? (second group_id))) (if-let [items (get-insert-items vs-line)]
                                                                                                                                       (if-let [{v-items :v-items} (my-authority ignite group_id schema_name table_name)]
                                                                                                                                           (if (nil? (has-my-authority pk items v-items))
                                                                                                                                               {:schema_name schema_name :table_name table_name :values items})
                                                                                                                                           {:schema_name schema_name :table_name table_name :values items})
                                                                                                                                       (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！")))
                          (and (not (my-lexical/is-eq? schema_name (second group_id))) (my-lexical/is-str-not-empty? schema_name) (my-lexical/is-str-not-empty? (second group_id))) (if-let [items (get-insert-items vs-line)]
                                                                                                                                                                                        (if-let [{v-items :v-items} (my-authority ignite group_id schema_name table_name)]
                                                                                                                                                                                            (if (nil? (has-my-authority pk items v-items))
                                                                                                                                                                                                {:schema_name schema_name :table_name table_name :values items})
                                                                                                                                                                                            (if (not (my-lexical/is-eq? schema_name "public"))
                                                                                                                                                                                                (throw (Exception. "用户不存在或者没有权限！添加数据！"))
                                                                                                                                                                                                {:schema_name "public" :table_name table_name :values items}))
                                                                                                                                                                                        (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！")))
                          ))))
        ))

(defn my_insert_obj-no-authority [^Ignite ignite group_id [f & r]]
    (if (and (my-lexical/is-eq? f "insert") (my-lexical/is-eq? (first r) "into"))
        (let [{schema_name :schema_name table_name :table_name vs-line :vs-line} (insert-body (rest r))]
            (if (and (or (my-lexical/is-eq? schema_name "my_meta") (= schema_name "")) (= (first group_id) 0))
                {:schema_name "MY_META" :table_name table_name :values (get-insert-items vs-line)}
                (if-let [items (get-insert-items vs-line)]
                    {:schema_name schema_name :table_name table_name :values items}
                    (throw (Exception. "insert 语句错误，必须是 insert into 表名 (...) values (...)！")))))
        ))


;(defn get_pk_data [^Ignite ignite ^String schema_name ^String table_name]
;    (if (my-lexical/is-eq? schema_name "public")
;        (when-let [it (.iterator (.query (.cache ignite "my_meta_tables") (.setArgs (doto (SqlFieldsQuery. "select m.column_name, m.column_type, m.pkid, m.auto_increment from table_item as m join my_meta_tables as t on m.table_id = t.id where t.table_name = ? and t.data_set_id = 0")
;                                                                                        (.setLazy true)) (to-array [(str/lower-case table_name)]))))]
;            (letfn [
;                    ; 从 iterator 中获取 pk列和数据列
;                    (get_pk_data_it [it lst_pk lst_data]
;                        (if (.hasNext it)
;                            (when-let [row (.next it)]
;                                (if (true? (.get row 2))
;                                    (recur it (concat lst_pk [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]) lst_data)
;                                    (recur it lst_pk (concat lst_data [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]))
;                                    ))
;                            {:pk lst_pk :data lst_data}))]
;                (get_pk_data_it it [] [])))
;        (when-let [it (.iterator (.query (.cache ignite "my_meta_tables") (.setArgs (doto (SqlFieldsQuery. "select m.column_name, m.column_type, m.pkid, m.auto_increment from table_item as m join my_meta_tables as t on m.table_id = t.id join my_dataset as ds on ds.id = t.data_set_id where t.table_name = ? and ds.schema_name = ?")
;                                                                                        (.setLazy true)) (to-array [(str/lower-case table_name) (str/lower-case schema_name)]))))]
;            (letfn [
;                    ; 从 iterator 中获取 pk列和数据列
;                    (get_pk_data_it [it lst_pk lst_data]
;                        (if (.hasNext it)
;                            (when-let [row (.next it)]
;                                (if (true? (.get row 2))
;                                    (recur it (concat lst_pk [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]) lst_data)
;                                    (recur it lst_pk (concat lst_data [{:column_name (.get row 0) :column_type (.get row 1) :pkid (.get row 2) :auto_increment (.get row 3)}]))
;                                    ))
;                            {:pk lst_pk :data lst_data}))]
;                (get_pk_data_it it [] [])))))


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
                    ([lst_column values] (if (map? lst_column)
                                             (get_rs [lst_column] values [])
                                             (get_rs lst_column values [])))
                    ([[f & r] values lst]
                     (if (some? f)
                         (let [m (get_value values f)]
                             (if-not (nil? m)
                                 (recur r values (concat lst [m]))
                                 (recur r values lst)))
                         lst))
                    )
                (get-vs-data [data values]
                    (loop [[f & r] values my-data data]
                        (if (some? f)
                            (let [key (str/lower-case (-> f :item_name))]
                                (if (contains? data key)
                                    (let [m (get data key)]
                                        (recur r (assoc my-data key {:define m :vs (-> f :item_value)})))
                                    (recur r my-data)))
                            my-data)))
                (get-vs-insert-data [vs-data]
                    (loop [[f & r] (keys vs-data) lst []]
                        (if (some? f)
                            (if (map? (get vs-data f))
                                (recur r (conj lst (get vs-data f)))
                                (cond (not (nil? (.getDefault_value (get vs-data f)))) (if (string? (.getDefault_value (get vs-data f)))
                                                                                           (recur r (conj lst {:define (get vs-data f) :vs [(.getDefault_value (get vs-data f))]}))
                                                                                           (recur r (conj lst {:define (get vs-data f) :vs [(str (.getDefault_value (get vs-data f)))]})))
                                      (true? (.getNot_null (get vs-data f))) (throw (Exception. (format "字段 %s 不能为空！" (.getColumn_name (get vs-data f)))))
                                      :else (recur r lst)
                                      ))
                            lst)))]
            {:pk_rs (get_rs (-> pk_data :pk) values) :data_rs (get-vs-insert-data (get-vs-data (-> pk_data :data) values))})
        ))











































