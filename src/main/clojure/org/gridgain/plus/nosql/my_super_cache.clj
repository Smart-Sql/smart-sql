(ns org.gridgain.plus.nosql.my-super-cache
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.gridgain.nosql MyNoSqlUtil)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model.nosql MyCacheGroup)
             (com.google.gson Gson GsonBuilder)
             (org.log MyCljLogger)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.sql.MySuperCache
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [myNoSql [org.apache.ignite.Ignite Long String] Object]
                  ^:static [setCacheVs [org.apache.ignite.Ignite Long String String String] Object]
                  ^:static [getCacheVs [org.apache.ignite.Ignite String String] Object]
                  ^:static [myCachePush [org.apache.ignite.Ignite Long String String] Object]
                  ^:static [myCachePop [org.apache.ignite.Ignite Long String] Object]]
        ))

; 创建 cache
; 定义对象的语法 名称: 数据类型
; 数据类型：java 基础数据类型，或自定义类型
; 定义 cache {name: cache的名字 attr: 定义的对象}
; 例如：
; {name: 产品, attr: {key: string value: {产品名: string, suk: string, 库存: int}}, dataRegin: 数据段}
; {name: 热销产品, attr: List}

(declare lst-dic seq-dic lst-no-sql get-lst-big get-doc-with-type-lst get-doc-with-type-dic get-doc-with-type
         get-dic-lst-items get-vs-dic set-cache-vs-lst set-cache-vs get-cache-vs my-cache-push my-cache-pop my-cache-push-lst my-cache-pop-lst)

(defn json_to_str
    ([lst] (json_to_str lst [] [] []))
    ([[f & r] stack stack-lst lst]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "json_to_str") (= (first r) "(")) (recur (rest r) (conj stack (first r)) stack-lst lst)
               (and (not (empty? stack)) (= f ")")) (recur r [] [] (conj lst (str/join stack-lst)))
               (not (empty? stack)) (recur r stack (conj stack-lst f) lst)
               :else
               (recur r stack stack-lst (conj lst f))
               )
         lst)))

; 判断是否是成对出现的
(defn is-pair?
    ([lst lst-ps] (if (= (count lst-ps) 2)
                      (is-pair? lst (first lst-ps) (last lst-ps) [])
                      (throw (Exception. "输入参数必须成对出现！"))))
    ([[f & r] ps_1 ps_2 stack]
     (if (some? f)
         (cond (= f ps_1) (recur r ps_1 ps_2 (conj stack ps_1))
               (= f ps_2) (if (and (nil? r) (= (count stack) 1))
                              true
                              (recur r ps_1 ps_2 (pop stack)))
               :else
               (recur r ps_1 ps_2 stack)
               )
         false)))

; ("pop" "(" "no_sql_query" ":" "{" "table_name" ":" "my_train_ticket" "," "key" ":" "\"G350_成都东_北京西\"" "}" ")")
(defn get-pop-query [[f & r]]
    (if (and (my-lexical/is-eq? f "pop") (= (first r) "(") (= (last r) ")"))
        (loop [index 1 lst []]
            (if (< (+ index 1) (count r))
                (recur (+ index 1) (conj lst (nth r index)))
                (if (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":") (is-pair? (rest (rest lst)) ["{" "}"]))
                    lst)))))

; lst: ["no_sql_query" ":" "{" "table_name" ":" "my_train_ticket" "," "key" ":" "\"G350_成都东_北京西\"" "}" "," "{" "token" ":" "B001" "," "price" ":" "778" "}"]
(defn get-push-params
    ([lst] (get-push-params lst [] [] []))
    ([[f & r] stack stack-lst lst-ps]
     (if (some? f)
         (cond (= f "{") (cond (empty? stack) (recur r (conj stack f) (conj stack-lst f) lst-ps)
                               (not (empty? stack)) (let [top-item (peek stack)]
                                                        (if (= top-item "{")
                                                            (recur r (conj stack f) (conj stack-lst f) lst-ps)
                                                            (recur r stack (conj stack-lst f) lst-ps)
                                                            ))
                               )
               (= f "(") (cond (empty? stack) (recur r (conj stack f) (conj stack-lst f) lst-ps)
                               (not (empty? stack)) (let [top-item (peek stack)]
                                                        (if (= top-item "(")
                                                            (recur r (conj stack f) (conj stack-lst f) lst-ps)
                                                            (recur r stack (conj stack-lst f) lst-ps)
                                                            ))
                               )
               (= f "}") (if (not (empty? stack))
                             (let [top-item (peek stack)]
                                 (if (= top-item "{")
                                     (recur r (pop stack) (conj stack-lst f) lst-ps)
                                     (recur r stack (conj stack-lst f) lst-ps)))
                             (recur r stack (conj stack-lst f) lst-ps))
               (= f ")") (if (not (empty? stack))
                             (let [top-item (peek stack)]
                                 (if (= top-item "(")
                                     (recur r (pop stack) (conj stack-lst f) lst-ps)
                                     (recur r stack (conj stack-lst f) lst-ps)))
                             (recur r stack (conj stack-lst f) lst-ps))
               (= f ",") (if-not (empty? stack)
                             (recur r stack (conj stack-lst f) lst-ps)
                             (recur r [] [] (conj lst-ps stack-lst)))
               :else
               (recur r stack (conj stack-lst f) lst-ps)
               )
         (if-not (empty? stack-lst)
             (conj lst-ps stack-lst)
             lst-ps)
         )))

; lst-push "push(\n    no_sql_query: {\n       table_name: my_train_ticket,\n       key: \"G350_成都东_北京西\"\n    },\n    {token: B001, price: 778}\n)"
; 输入参数：(my-lexical/to-back lst-push)
(defn get-push-query [[f & r]]
    (if (and (my-lexical/is-eq? f "push") (= (first r) "(") (= (last r) ")"))
        (loop [index 1 lst []]
            (if (< (+ index 1) (count r))
                (recur (+ index 1) (conj lst (nth r index)))
                (get-push-params lst)))))

;(defn get-query-lst
;    ([lst] (get-query-lst lst [] [] [] ["{"]))
;    ([[f & r] stack-mid stack-big stack-query lst]
;     (if (some? f)
;         (cond (and (= f "[") (empty? stack-query)) (recur r (conj stack-mid f) stack-big stack-query lst)
;               (and (= f "]") (empty? stack-query)) (recur r (pop stack-mid) stack-big stack-query lst)
;               (and (= f "{") (empty? stack-query)) (recur r stack-mid (conj stack-big f) stack-query lst)
;               (and (= f "}") (empty? stack-query)) (recur r stack-mid (conj stack-big f) stack-query lst)
;               (and (my-lexical/is-eq? f "query") (= (first r) ":") (empty? stack-mid) (empty? stack-big) (empty? stack-query)) (recur (rest r) stack-mid stack-big (conj stack-query ":") lst)
;               (and (not (= f ",")) (empty? stack-mid) (empty? stack-big) (not (empty? stack-query))) (recur r stack-mid stack-big (conj stack-query f) lst)
;               :else
;               (recur r stack-mid stack-big stack-query (conj lst f))
;               )
;         [stack-query (conj lst "}")])))

(defn get-query-lst
    ([lst] (get-query-lst lst false [] []))
    ([[f & r] is-query stack-query lst]
     (if (some? f)
         (cond
             (and (my-lexical/is-eq? f "query") (= (first r) ":") (empty? stack-query)) (recur (rest r) true (conj stack-query ":") lst)
             (= f ",") (if (true? is-query)
                           (recur r false stack-query lst)
                           (recur r is-query stack-query (conj lst f)))
             (= f "}") (if (true? is-query)
                           (recur r false stack-query (conj lst f))
                           (recur r is-query stack-query (conj lst f)))
             (true? is-query) (recur r is-query (conj stack-query f) lst)
             :else
             (recur r is-query stack-query (conj lst f))
             )
         [stack-query lst])))

(defn get-lst-big [lst]
    (if (or (and (= (first lst) "{") (not (= (last lst) "}"))) (and (not (= (first lst) "{")) (= (last lst) "}")))
        (throw (Exception. "输入字符串格式错误！"))
        (my-lexical/get-contain-lst lst)))

(defn get-lst-mid [lst]
    (if (or (and (= (first lst) "[") (not (= (last lst) "]"))) (and (not (= (first lst) "[")) (= (last lst) "]")))
        (throw (Exception. "输入字符串格式错误！"))
        (my-lexical/get-contain-lst lst)))

(defn get-dic-lst
    ([lst] (get-dic-lst lst [] [] false []))
    ([[f & r] stack-kuo kuo-lst flag rs-lst]
     (if (some? f)
         (cond (and (false? flag) (= f "{")) (recur r (conj stack-kuo f) (conj kuo-lst f) flag rs-lst)
               (and (false? flag) (= f "}")) (if (= (count stack-kuo) 1)
                                                 (recur r (pop stack-kuo) (conj kuo-lst f) true rs-lst)
                                                 (recur r (pop stack-kuo) (conj kuo-lst f) flag rs-lst))
               :else
               (if (> (count stack-kuo) 0)
                   (recur r stack-kuo (conj kuo-lst f) flag rs-lst)
                   (recur r stack-kuo kuo-lst flag (conj rs-lst f)))
               )
         [kuo-lst rs-lst])))

(defn get-seq-lst
    ([lst] (get-seq-lst lst [] [] false []))
    ([[f & r] stack-kuo kuo-lst flag rs-lst]
     (if (some? f)
         (cond (and (false? flag) (= f "[")) (recur r (conj stack-kuo f) (conj kuo-lst f) flag rs-lst)
               (and (false? flag) (= f "]")) (if (= (count stack-kuo) 1)
                                                 (recur r (pop stack-kuo) (conj kuo-lst f) true rs-lst)
                                                 (recur r (pop stack-kuo) (conj kuo-lst f) flag rs-lst))
               :else
               (if (> (count stack-kuo) 0)
                   (recur r stack-kuo (conj kuo-lst f) flag rs-lst)
                   (recur r stack-kuo kuo-lst flag (conj rs-lst f)))
               )
         [kuo-lst rs-lst])))

(defn lst-dic
    ([lst] (lst-dic lst [] {}))
    ([[f & r] stack-key dic]
     (if (some? f)
         (if (contains? #{"+" "-" "*" "/" "=" ">" "<" ">=" "<=" "(" ")" "."} f)
             (throw (Exception. (format "输入字符串格式错误！位置：%s" f)))
             (cond (= f "{") (let [[kuo-lst rs-lst] (get-dic-lst (concat [f] r))]
                                 (if (and (not (empty? kuo-lst)) (empty? rs-lst))
                                     (if (empty? stack-key)
                                         (lst-dic (get-lst-big kuo-lst))
                                         (assoc dic (my-lexical/get_str_value (nth stack-key 0)) (lst-dic (get-lst-big kuo-lst))))
                                     (recur rs-lst [] (assoc dic (my-lexical/get_str_value (nth stack-key 0)) (lst-dic (get-lst-big kuo-lst))))
                                     ))
                   (= f "[") (let [[kuo-lst rs-lst] (get-seq-lst (concat [f] r))]
                                 (if (and (not (empty? kuo-lst)) (empty? rs-lst))
                                     (if (empty? stack-key)
                                         (seq-dic (get-lst-mid kuo-lst))
                                         (assoc dic (my-lexical/get_str_value (nth stack-key 0)) (seq-dic (get-lst-mid kuo-lst))))
                                     (recur rs-lst [] (assoc dic (my-lexical/get_str_value (nth stack-key 0)) (seq-dic (get-lst-mid kuo-lst))))
                                     ))
                   (and (empty? stack-key) (not (contains? #{":" ","} f))) (recur r (conj stack-key f) dic)
                   (and (not (empty? stack-key)) (not (contains? #{":" ","} f))) (recur r [] (assoc dic (my-lexical/get_str_value (nth stack-key 0)) (my-lexical/get_str_value f)))
                   (contains? #{":" ","} f) (recur r stack-key dic)
                   :else
                   (recur r (conj stack-key f) dic)
                   ))
         (if (not (empty? stack-key))
             (throw (Exception. (format "输入字符串格式错误！位置：%s" (nth stack-key 0))))
             dic))))

(defn seq-dic
    ([lst] (seq-dic lst []))
    ([[f & r] rs]
     (if (some? f)
         (if (contains? #{"+" "-" "*" "/" "=" ">" "<" ">=" "<=" "(" ")" "."} f)
             (throw (Exception. (format "输入字符串格式错误！位置：%s" f)))
             (cond (= f "{") (let [[kuo-lst rs-lst] (get-dic-lst (concat [f] r))]
                                 (if (and (not (empty? kuo-lst)) (empty? rs-lst))
                                     (conj rs (lst-dic (get-lst-big kuo-lst)))
                                     (recur rs-lst (conj rs (lst-dic (get-lst-big kuo-lst))))
                                     ))
                   (= f "[") (let [[kuo-lst rs-lst] (get-seq-lst (concat [f] r))]
                                 (if (and (not (empty? kuo-lst)) (empty? rs-lst))
                                     (conj rs (seq-dic (get-lst-mid kuo-lst)))
                                     (recur rs-lst (conj rs (seq-dic (get-lst-mid kuo-lst))))
                                     ))
                   (contains? #{":" ","} f) (recur r rs)
                   :else
                   (recur r (conj rs (my-lexical/get_str_value f)))
                   ))
         rs)))

(defn lst-no-sql [lst]
    (if (= (count lst) 1)
        (first lst)
        (cond (and (= (first lst) "{") (= (last lst) "}")) (lst-dic lst)
              (and (= (first lst) "[") (= (last lst) "]")) (seq-dic lst)
              :else
              (throw (Exception. "输入字符串格式错误！")))
        ))

(defn lst-no-sql-query [lst]
    (let [[f r] (get-query-lst lst)]
        (if-not (empty? f)
            [f (lst-no-sql r)]
            [nil (lst-no-sql r)])))

; 创建 cache
; 1、字符串转 dic
(defn line-dic [^String line]
    (if-let [lst (my-lexical/to-back line)]
        (letfn [(get-lst [lst]
                    (if (or (and (= (first lst) "{") (not (= (last lst) "}"))) (and (not (= (first lst) "{")) (= (last lst) "}")))
                        (throw (Exception. "输入字符串格式错误！"))
                        (my-lexical/get-contain-lst lst)))
                (get-dic-lst
                    ([lst] (get-dic-lst lst [] [] false []))
                    ([[f & r] stack-kuo kuo-lst flag rs-lst]
                     (if (some? f)
                         (cond (and (false? flag) (= f "{")) (recur r (conj stack-kuo f) (conj kuo-lst f) flag rs-lst)
                               (and (false? flag) (= f "}")) (if (= (count stack-kuo) 1)
                                                                 (recur r (pop stack-kuo) (conj kuo-lst f) true rs-lst)
                                                                 (recur r (pop stack-kuo) (conj kuo-lst f) flag rs-lst))
                               :else
                               (if (> (count stack-kuo) 0)
                                   (recur r stack-kuo (conj kuo-lst f) flag rs-lst)
                                   (recur r stack-kuo kuo-lst flag (conj rs-lst f)))
                               )
                         [kuo-lst rs-lst])))
                (lst-dic
                    ([lst] (lst-dic lst [] {}))
                    ([[f & r] stack-key dic]
                     (if (some? f)
                         (if (contains? #{"+" "-" "*" "/" "=" ">" "<" ">=" "<=" "(" ")" "." "[" "]"} f)
                             (throw (Exception. (format "输入字符串格式错误！位置：%s" f)))
                             (cond (= f "{") (let [[kuo-lst rs-lst] (get-dic-lst (concat [f] r))]
                                                 (if (and (not (empty? kuo-lst)) (empty? rs-lst))
                                                     (if (empty? stack-key)
                                                         (lst-dic (get-lst kuo-lst))
                                                         (assoc dic (nth stack-key 0) (lst-dic (get-lst kuo-lst))))
                                                     (recur rs-lst [] (assoc dic (nth stack-key 0) (lst-dic (get-lst kuo-lst))))
                                                     ))
                                   (and (empty? stack-key) (not (contains? #{":" ","} f))) (recur r (conj stack-key f) dic)
                                   (and (not (empty? stack-key)) (not (contains? #{":" ","} f))) (recur r [] (assoc dic (nth stack-key 0) f))
                                   (contains? #{":" ","} f) (recur r stack-key dic)
                                   :else
                                   (recur r (conj stack-key f) dic)
                                   ))
                         (if (not (empty? stack-key))
                             (throw (Exception. (format "输入字符串格式错误！位置：%s" (nth stack-key 0))))
                             dic))))]
            (lst-dic (get-lst lst)))))

;(declare get-doc-with-type-lst get-doc-with-type-dic get-doc-with-type)

(defn get-doc-with-type-lst [no-sql-type-lst no-sql]
    (letfn [(equal-lst [lst1 lst2]
                (loop [[f & r] lst1 index 0 lst-rs []]
                    (if (some? f)
                        (recur r (+ index 1) (conj lst-rs (get-doc-with-type f (nth lst2 index))))
                        lst-rs)))]
        (cond (= (count no-sql-type-lst) (count no-sql)) (equal-lst no-sql-type-lst no-sql)
              (< (count no-sql-type-lst) (count no-sql)) (if (> (count no-sql-type-lst) 0)
                                                             (let [last-type (last no-sql-type-lst) last-index (- (count no-sql-type-lst) 1)]
                                                                 (let [lst-rs (equal-lst no-sql-type-lst no-sql)]
                                                                     (loop [my-last-index last-index lst-rs-sub lst-rs]
                                                                         (if (< my-last-index (- (count no-sql) 1))
                                                                             (recur (+ my-last-index 1) (conj lst-rs-sub (get-doc-with-type last-type (nth no-sql (+ my-last-index 1)))))
                                                                             lst-rs-sub))))
                                                             no-sql)
              :else
              (throw (Exception. "输入字符串错误！"))
              ))
    )

(defn get-doc-with-type-dic [no-sql-type-dic no-sql]
    (loop [[f & r] (keys no-sql-type-dic) no-sql-sub no-sql]
        (if (some? f)
            (recur r (assoc no-sql-sub f (get-doc-with-type (get no-sql-type-dic f) (get no-sql f))))
            no-sql-sub)))

; 输入 no-sql-type 返回 no-sql
; no-sql-type 为 no-sql 的定义
; no-sql 为输入的 no-sql 的值，因为 no-sql 输入的时候都是字符串，所以需要按照 no-sql 的定义转换为真实的值
(defn get-doc-with-type [no-sql-type no-sql]
    (cond (map? no-sql-type) (get-doc-with-type-dic no-sql-type no-sql)
          (my-lexical/is-seq? no-sql-type) (get-doc-with-type-lst no-sql-type no-sql)
          (my-lexical/is-eq? no-sql-type "string") (.toString no-sql)
          (my-lexical/is-eq? no-sql-type "long") (MyConvertUtil/ConvertToLong no-sql)
          (my-lexical/is-eq? no-sql-type "int") (MyConvertUtil/ConvertToInt no-sql)
          (or (my-lexical/is-eq? no-sql-type "bool") (my-lexical/is-eq? no-sql-type "bool")) (MyConvertUtil/ConvertToBoolean no-sql)
          (my-lexical/is-eq? no-sql-type "double") (MyConvertUtil/ConvertToDouble no-sql)
          (or (my-lexical/is-eq? no-sql-type "timestamp") (my-lexical/is-eq? no-sql-type "time") (my-lexical/is-eq? no-sql-type "date")) (MyConvertUtil/ConvertToTimestamp no-sql)
          (or (my-lexical/is-eq? no-sql-type "decimal") (my-lexical/is-eq? no-sql-type "bigdecimal")) (MyConvertUtil/ConvertToDecimal no-sql)
          :else
          no-sql
          ))

;; 生成 cache
;(defn my-define-cache [^Ignite ignite ^String line]
;    (if-let [{cache-name "name" {key "key" value "value"} "keyValue" data-regin "dataRegin"} (line-dic line)]
;        (if (and (some? name) (some? key))
;            (MyNoSqlUtil/defineCache ignite name data-regin line)
;            )))

; 操作 no sql
(defn my-no-sql [^Ignite ignite ^Long group_id ^String no-sql-line]
    (if-not (Strings/isNullOrEmpty no-sql-line)
        (if-let [lst (my-lexical/to-back no-sql-line)]
            (cond (and (my-lexical/is-eq? (first lst) "no_sql_create") (= (second lst) ":")) (if-let [{name "name" {my-key "key" no-sql "doc"} "keyValue" data-regin "dataRegin"} (lst-no-sql (rest (rest lst)))]
                                                                                                 (if (and (some? name) (some? my-key))
                                                                                                     (MyNoSqlUtil/defineCache ignite group_id name data-regin no-sql-line)
                                                                                                     ))
                  (and (my-lexical/is-eq? (first lst) "no_sql_insert") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key" no-sql "doc"} (lst-no-sql (rest (rest lst)))]
                                                                                                 (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                                     (let [{{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                         (.put (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))
                                                                                                         )))
                  (and (my-lexical/is-eq? (first lst) "no_sql_update") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                                 (let [{table_name "table_name" my-key "key" no-sql "doc"} r]
                                                                                                     (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                                         (if (or (nil? f) (empty? f))
                                                                                                             (let [{{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                 (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))
                                                                                                             (let [vs-obj (.get (.cache ignite table_name) my-key) query-vs (lst-no-sql (my-lexical/to-back no-sql))]
                                                                                                                 (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items (str/join (rest f)))) vs-obj query-vs) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                     (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))))))
                                                                                                     ))
                  (and (my-lexical/is-eq? (first lst) "no_sql_delete") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key"} (lst-no-sql (rest (rest lst)))]
                                                                                                 (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                                     (.remove (.cache ignite table_name) my-key))
                                                                                                 )
                  (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                                (if-let [{table_name "table_name" my-key "key"} r]
                                                                                                    (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                                        (if (or (nil? f) (empty? f))
                                                                                                            (.get (.cache ignite table_name) my-key)
                                                                                                            (let [vs-obj (.get (.cache ignite table_name) my-key)]
                                                                                                                (get-vs-dic vs-obj (str/join (rest f))))))
                                                                                                    )
                                                                                                )
                  (and (my-lexical/is-eq? (first lst) "no_sql_drop") (= (second lst) ":")) (if-let [{table_name "name"} (lst-no-sql (rest (rest lst)))]
                                                                                               (MyNoSqlUtil/destroyCache ignite table_name group_id))
                  (and (my-lexical/is-eq? (first lst) "push") (= (second lst) "(") (= (last lst) ")")) (let [my-lst (json_to_str lst)]
                                                                                                           (if (= (count my-lst) 6)
                                                                                                               (my-cache-push ignite group_id (nth my-lst 2) (nth my-lst 4))
                                                                                                               (throw (Exception. "push 定义错误！"))))
                  (and (my-lexical/is-eq? (first lst) "pop") (= (second lst) "(") (= (last lst) ")")) (let [my-lst (json_to_str lst)]
                                                                                                          (if (= (count my-lst) 4)
                                                                                                              (my-cache-pop ignite group_id (nth my-lst 2))
                                                                                                              (throw (Exception. "pop 定义错误！"))))
                  :else
                  (throw (Exception. "输入字符串错误！"))
                  ))))

(defn my-no-lst [^Ignite ignite ^Long group_id lst no-sql-line]
    (cond (and (my-lexical/is-eq? (first lst) "no_sql_create") (= (second lst) ":")) (if-let [{name "name" {my-key "key" no-sql "doc"} "keyValue" data-regin "dataRegin"} (lst-no-sql (rest (rest lst)))]
                                                                                         (if (and (some? name) (some? my-key))
                                                                                             (try
                                                                                                 (if (nil? (MyNoSqlUtil/defineCache ignite group_id name data-regin no-sql-line))
                                                                                                     "select show_msg('true') as tip"
                                                                                                     "select show_msg('false') as tip")
                                                                                                 (catch Exception e
                                                                                                     (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                             ))
          (and (my-lexical/is-eq? (first lst) "no_sql_insert") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key" no-sql "doc"} (lst-no-sql (rest (rest lst)))]
                                                                                         (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                             (let [{{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                 (try
                                                                                                     (if (nil? (.put (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))
                                                                                                         "select show_msg('true') as tip"
                                                                                                         "select show_msg('false') as tip")
                                                                                                     (catch Exception e
                                                                                                         (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                                 )))
          (and (my-lexical/is-eq? (first lst) "no_sql_update") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                         (let [{table_name "table_name" my-key "key" no-sql "doc"} r]
                                                                                             (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                                 (if (or (nil? f) (empty? f))
                                                                                                     (let [{{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                         (try
                                                                                                             (if (true? (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))
                                                                                                                 "select show_msg('true') as tip"
                                                                                                                 "select show_msg('false') as tip")
                                                                                                             (catch Exception e
                                                                                                                 (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                                         )
                                                                                                     (let [vs-obj (.get (.cache ignite table_name) my-key) query-vs (lst-no-sql (my-lexical/to-back no-sql))]
                                                                                                         (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items (str/join (rest f)))) vs-obj query-vs) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                             (try
                                                                                                                 (if (true? (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))
                                                                                                                     "select show_msg('true') as tip"
                                                                                                                     "select show_msg('false') as tip")
                                                                                                                 (catch Exception e
                                                                                                                     (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                                             ))))
                                                                                             ))
          (and (my-lexical/is-eq? (first lst) "no_sql_delete") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key"} (lst-no-sql (rest (rest lst)))]
                                                                                         (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                             (try
                                                                                                 (if (true? (.remove (.cache ignite table_name) my-key))
                                                                                                     "select show_msg('true') as tip"
                                                                                                     "select show_msg('false') as tip")
                                                                                                 (catch Exception e
                                                                                                     (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                             )
                                                                                         )
          (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst))) gson (.create (.setDateFormat (.enableComplexMapKeySerialization (GsonBuilder.)) "yyyy-MM-dd HH:mm:ss"))]
                                                                                        (if-let [{table_name "table_name" my-key "key"} r]
                                                                                            (if (true? (MyNoSqlUtil/hasCache ignite table_name group_id))
                                                                                                (if (or (nil? f) (empty? f))
                                                                                                    (try
                                                                                                        (if-let [get-rs (.get (.cache ignite table_name) my-key)]
                                                                                                            ;(do
                                                                                                            ;    (MyCljLogger/myWriter (format "select show_msg('%s')" (.toJson gson get-rs)))
                                                                                                            ;    (format "select show_msg('%s') as value" (.toJson gson get-rs)))
                                                                                                            (format "select show_msg('%s') as value" (.toJson gson get-rs))
                                                                                                            "select show_msg('') as value")
                                                                                                        (catch Exception e
                                                                                                            (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                                    (let [vs-obj (.get (.cache ignite table_name) my-key)]
                                                                                                        (try
                                                                                                            (if-let [get-rs (get-vs-dic vs-obj (str/join (rest f)))]
                                                                                                                ;(do
                                                                                                                ;    (MyCljLogger/myWriter (format "select show_msg('%s')" (.toJson gson get-rs)))
                                                                                                                ;    (format "select show_msg('%s') as value" (.toJson gson get-rs)))
                                                                                                                (format "select show_msg('%s') as value" (.toJson gson get-rs))
                                                                                                                "select show_msg('') as value")
                                                                                                            (catch Exception e
                                                                                                                (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                                        )))
                                                                                            )
                                                                                        )
          (and (my-lexical/is-eq? (first lst) "no_sql_drop") (= (second lst) ":")) (if-let [{table_name "name"} (lst-no-sql (rest (rest lst)))]
                                                                                       (try
                                                                                           (if (nil? (MyNoSqlUtil/destroyCache ignite table_name group_id))
                                                                                               "select show_msg('true') as tip"
                                                                                               "select show_msg('false') as tip")
                                                                                           (catch Exception e
                                                                                               (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                       )
          (and (my-lexical/is-eq? (first lst) "push") (= (second lst) "(") (= (last lst) ")")) (let [my-lst (get-push-query lst)]
                                                                                                   (try
                                                                                                       (if (true? (my-cache-push-lst ignite group_id my-lst))
                                                                                                           "select show_msg('true') as tip"
                                                                                                           "select show_msg('false') as tip")
                                                                                                       (catch Exception e
                                                                                                           (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e))))
                                                                                                   )
          (and (my-lexical/is-eq? (first lst) "pop") (= (second lst) "(") (= (last lst) ")")) (let [my-lst (get-pop-query lst) gson (.create (.setDateFormat (.enableComplexMapKeySerialization (GsonBuilder.)) "yyyy-MM-dd HH:mm:ss"))]
                                                                                                  (try
                                                                                                      (if-let [get-rs (my-cache-pop-lst ignite group_id my-lst)]
                                                                                                          (format "select show_msg('%s') as value" (.toJson gson get-rs))
                                                                                                          "select show_msg('') as value")
                                                                                                      (catch Exception e
                                                                                                          (format "select show_msg('执行失败！原因：%s') as tip" (.getMessage e)))))
          :else
          (throw (Exception. "输入字符串错误！"))
          ))

(defn -myNoSql [^Ignite ignite ^Long group_id ^String no-sql-line]
    (my-no-sql ignite group_id no-sql-line))

; 操作 cache
; 保存数据
;(defn my-cache-save [^Ignite ignite ^String cache-name ^Object key ^Object value]
;    (.put (.cache ignite cache-name) key value))

; 读取数据
;(defn my-cache-read [^Ignite ignite ^String cache-name ^Object key]
;    (.get (.cache ignite cache-name) key))

; 删除数据
;(defn my-cache-delete [^Ignite ignite ^String cache-name ^Object key]
;    (.remove (.cache ignite cache-name) key))

(defn get-dic-lst-items
    ([lst] (letfn [(get-lst-obj
                       ([lst] (get-lst-obj lst [] [] [] []))
                       ([[f & r] stack stack-lst stack-line lst]
                        (letfn [(get-num-dic
                                    ([lst] (get-num-dic lst []))
                                    ([[f-num & r-num] lst-num-dic]
                                     (if (some? f-num)
                                         (if-not (= f-num "")
                                             (recur r-num (conj lst-num-dic {:item "" :index f-num}))
                                             (recur r-num lst-num-dic))
                                         lst-num-dic))
                                    )
                                (to-num-dic [lst]
                                    (concat [{:item (first lst) :index (second lst)}] (get-num-dic (rest (rest lst)))))]
                            (if (some? f)
                                (cond (= f \[) (if (= (count stack) 0)
                                                   (recur r (conj stack f) stack-lst [] (conj lst (str/join stack-line)))
                                                   (throw (Exception. (format "字符串错误！%s" (str/join (cons f r))))))
                                      (= f \]) (if (> (count stack) 0)
                                                   (if (= (count stack) 1)
                                                       (recur r [] [] stack-line (conj lst (str/join stack-lst)))
                                                       (recur r (pop stack) stack-lst stack-line lst))
                                                   (throw (Exception. (format "字符串错误！%s" (str/join (cons f r))))))
                                      (> (count stack) 0) (recur r stack (conj stack-lst f) stack-line lst)
                                      :else
                                      (recur r stack stack-lst (conj stack-line f) lst)
                                      )
                                (cond (and (empty? stack) (empty? stack-line) (empty? stack-lst) (= (count lst) 2)) [{:item (nth lst 0) :index (nth lst 1)}]
                                      (> (count lst) 2) (to-num-dic lst)
                                      (= (count lst) 1) [(first lst)]
                                      (and (not (empty? stack-line)) (empty? stack-lst) (empty? lst)) [(str/join stack-line)]
                                      :else
                                      (throw (Exception. (format "字符串错误！%s" (str/join (cons f r)))))
                                      )))
                        ))]
               (loop [[f & r] (get-dic-lst-items lst [] []) rs-lst []]
                   (if (some? f)
                       (if-let [m (get-lst-obj f)]
                           (recur r (concat rs-lst m))
                           (recur r (conj rs-lst f))
                           )
                       rs-lst)
                   )))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (= f \.) (if (> (count stack) 0)
                            (recur r [] (concat lst [(str/join stack) "."]))
                            (recur r [] (concat lst ["."]))
                            )
               :else
               (recur r (conj stack f) lst)
               )
         (if (> (count stack) 0)
             (concat lst [(str/join stack)])
             lst))))

(defn get-vs-dic [vs-obj items-line]
    (loop [[f & r] (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-obj vs-obj]
        (if (some? f)
            (if (map? f)
                (if (= (get f :item) "")
                    (recur r (nth my-obj (MyConvertUtil/ConvertToInt (get f :index))))
                    (recur r (nth (get my-obj (get f :item)) (MyConvertUtil/ConvertToInt (get f :index)))))
                (recur r (get my-obj f)))
            (if-not (= my-obj vs-obj)
                my-obj
                nil)))
    )

; [f & r]: items-lst
; vs-obj: cache 的值
; query-vs: 要修改的值  query-vs 的值为 nil 表示要删除
(defn set-cache-vs-lst [[f & r] vs-obj query-vs]
    (if (some? f)
        (cond (map? vs-obj) (if (or (nil? r) (empty? r))
                                (if (map? f)
                                    (let [m-vs (get vs-obj (get f :item))]
                                        (if-not (nil? query-vs)
                                            (assoc vs-obj (get f :item) (set-cache-vs-lst [{:item "" :index (get f :index)}] m-vs query-vs))
                                            (dissoc vs-obj (get f :item)))
                                        )
                                    (if-not (nil? query-vs)
                                        (assoc vs-obj f query-vs)
                                        (dissoc vs-obj f)))
                                (if (map? f)
                                    (let [m-vs (get vs-obj (get f :item))]
                                        (assoc vs-obj (get f :item) (set-cache-vs-lst (concat [{:item "" :index (get f :index)}] r) m-vs query-vs)))
                                    (let [m-vs (set-cache-vs-lst r (get vs-obj f) query-vs)]
                                        (if-not (nil? m-vs)
                                            (assoc vs-obj f m-vs)
                                            (dissoc vs-obj f)))
                                    )
                                )
              (and (my-lexical/is-seq? vs-obj) (map? f)) (if (or (nil? r) (empty? r))
                                                                                    (loop [[f-vs & r-vs] vs-obj index 0 my-index (get f :index) vs-lst []]
                                                                                        (if (some? f-vs)
                                                                                            (if-not (= index (MyConvertUtil/ConvertToLong my-index))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst f-vs))
                                                                                                (if-not (nil? query-vs)
                                                                                                    (recur r-vs (+ index 1) my-index (conj vs-lst query-vs))
                                                                                                    (recur r-vs (+ index 1) my-index vs-lst))
                                                                                                )
                                                                                            vs-lst))
                                                                                    (loop [[f-vs & r-vs] vs-obj index 0 my-index (get f :index) vs-lst []]
                                                                                        (if (some? f-vs)
                                                                                            (if-not (= index (MyConvertUtil/ConvertToLong my-index))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst f-vs))
                                                                                                (recur r-vs (+ index 1) my-index (conj vs-lst (set-cache-vs-lst r f-vs query-vs)))
                                                                                                )
                                                                                            vs-lst)))
              :else
              query-vs
              )
        ))

; no-sql-line: no_sql_query: {
;   table_name: my_seckill,
;   key: "wu-da-fu-0"
;}
; items-line: [1].stock
; query-line: 54321
(defn set-cache-vs [^Ignite ignite ^Long group_id ^String no-sql-line ^String items-line ^String query-line]
    (if-not (Strings/isNullOrEmpty no-sql-line)
        (if-let [lst (my-lexical/to-back no-sql-line)]
            (cond
                (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key"} (lst-no-sql (rest (rest lst)))]
                                                                                              (let [vs-obj (.get (.cache ignite table_name) my-key) query-vs (lst-no-sql (my-lexical/to-back query-line))]
                                                                                                  (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) vs-obj query-vs) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                      (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))))
                :else
                (throw (Exception. "输入字符串错误！"))
                )))
    )

(defn -setCacheVs [^Ignite ignite ^Long group_id ^String no-sql-line ^String items-line ^String query-line]
    (set-cache-vs ignite group_id no-sql-line items-line query-line))

; no-sql-line: no_sql_query: {
;   table_name: my_seckill,
;   key: "wu-da-fu-0"
;}
; items-line: [1].stock
(defn get-cache-vs [^Ignite ignite ^String no-sql-line ^String items-line]
    (if-not (Strings/isNullOrEmpty no-sql-line)
        (if-let [lst (my-lexical/to-back no-sql-line)]
            (cond
                (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key"} (lst-no-sql (rest (rest lst)))]
                                                                                              (let [vs-obj (.get (.cache ignite table_name) my-key)]
                                                                                                  (get-vs-dic vs-obj items-line)))
                :else
                (throw (Exception. "输入字符串错误！"))
                )))
    )

(defn -getCacheVs [^Ignite ignite ^String no-sql-line ^String items-line]
    (get-cache-vs ignite no-sql-line items-line))

; 对堆栈的操作，push
;(defn my-cache-push [^Ignite ignite ^Long group_id ^String no-sql-line ^String vs & items-line]
;    (if-not (Strings/isNullOrEmpty no-sql-line)
;        (if-let [lst (my-lexical/to-back no-sql-line)]
;            (cond
;                (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (if-let [{table_name "table_name" my-key "key"} (lst-no-sql (rest (rest lst)))]
;                                                                                              (let [vs-obj (.get (.cache ignite table_name) my-key)]
;                                                                                                  (if-not (Strings/isNullOrEmpty items-line)
;                                                                                                      (let [my-stack (get-vs-dic vs-obj items-line)]
;                                                                                                          (if (vector? my-stack)
;                                                                                                              (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-stack (conj my-stack (lst-no-sql (my-lexical/to-back vs)))) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
;                                                                                                                  (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))))
;                                                                                                      (if (vector? vs-obj)
;                                                                                                          (let [no-sql (conj vs-obj (lst-no-sql (my-lexical/to-back vs))) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
;                                                                                                              (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))))
;                                                                                                      )))
;                :else
;                (throw (Exception. "输入字符串错误！"))
;                )))
;    )

(defn my-cache-push [^Ignite ignite ^Long group_id ^String no-sql-line ^String vs]
    (if-not (Strings/isNullOrEmpty no-sql-line)
        (if-let [lst (my-lexical/to-back no-sql-line)]
            (cond
                (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                              (if-let [{table_name "table_name" my-key "key"} r]
                                                                                                  (let [vs-obj (.get (.cache ignite table_name) my-key) items-line (str/join (rest f))]
                                                                                                      (if-not (and (nil? f) (empty? f))
                                                                                                          (let [my-stack (get-vs-dic vs-obj items-line)]
                                                                                                              (if (my-lexical/is-seq? my-stack)
                                                                                                                  (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-stack (conj my-stack (lst-no-sql (my-lexical/to-back vs)))) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                      (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))))
                                                                                                          (if (my-lexical/is-seq? vs-obj)
                                                                                                              (let [no-sql (conj vs-obj (lst-no-sql (my-lexical/to-back vs))) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                  (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))))
                                                                                                          ))))
                :else
                (throw (Exception. "输入字符串错误！"))
                )))
    )

(defn my-cache-push-lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector no-sql-lst]
    (let [lst (nth no-sql-lst 0) vs-lst (nth no-sql-lst 1)]
        (cond
            (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                          (if-let [{table_name "table_name" my-key "key"} r]
                                                                                              (let [vs-obj (.get (.cache ignite table_name) my-key) items-line (str/join (rest f))]
                                                                                                  (if-not (and (nil? f) (empty? f))
                                                                                                      (let [my-stack (get-vs-dic vs-obj items-line)]
                                                                                                          (if (my-lexical/is-seq? my-stack)
                                                                                                              (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-stack (conj my-stack (lst-no-sql vs-lst))) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                  (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql)))))
                                                                                                      (if (my-lexical/is-seq? vs-obj)
                                                                                                          (let [no-sql (conj vs-obj (lst-no-sql vs-lst)) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                              (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))))
                                                                                                      ))))
            :else
            (throw (Exception. "输入字符串错误！"))
            ))
    )

(defn -myCachePush [^Ignite ignite ^Long group_id ^String no-sql-line ^String vs]
    (my-cache-push ignite group_id no-sql-line vs))

(defn my-cache-pop [^Ignite ignite ^Long group_id ^String no-sql-line]
    (if-not (Strings/isNullOrEmpty no-sql-line)
        (if-let [lst (my-lexical/to-back no-sql-line)]
            (cond
                (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                              (if-let [{table_name "table_name" my-key "key"} r]
                                                                                                  (let [vs-obj (.get (.cache ignite table_name) my-key) items-line (str/join (rest f))]
                                                                                                      (if-not (Strings/isNullOrEmpty items-line)
                                                                                                          (let [my-stack (get-vs-dic vs-obj items-line)]
                                                                                                              (if (my-lexical/is-seq? my-stack)
                                                                                                                  (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-stack (pop my-stack)) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                      (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))
                                                                                                                      (peek my-stack)
                                                                                                                      )))
                                                                                                          (if (my-lexical/is-seq? vs-obj)
                                                                                                              (let [no-sql (pop vs-obj) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                                  (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))
                                                                                                                  (peek vs-obj)))
                                                                                                          ))))
                :else
                (throw (Exception. "输入字符串错误！"))
                )))
    )

(defn my-cache-pop-lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector lst]
    (cond
        (and (my-lexical/is-eq? (first lst) "no_sql_query") (= (second lst) ":")) (let [[f r] (lst-no-sql-query (rest (rest lst)))]
                                                                                      (if-let [{table_name "table_name" my-key "key"} r]
                                                                                          (let [vs-obj (.get (.cache ignite table_name) my-key) items-line (str/join (rest f))]
                                                                                              (if-not (Strings/isNullOrEmpty items-line)
                                                                                                  (let [my-stack (get-vs-dic vs-obj items-line)]
                                                                                                      (if (my-lexical/is-seq? my-stack)
                                                                                                          (let [no-sql (set-cache-vs-lst (filter #(not (= % ".")) (get-dic-lst-items items-line)) my-stack (pop my-stack)) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                              (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))
                                                                                                              (peek my-stack)
                                                                                                              )))
                                                                                                  (if (my-lexical/is-seq? vs-obj)
                                                                                                      (let [no-sql (pop vs-obj) {{no-sql-type "doc"} "keyValue"} (lst-no-sql (rest (rest (my-lexical/to-back (.getSql_line (.get (.cache ignite "my_cache") (MyCacheGroup. table_name group_id)))))))]
                                                                                                          (.replace (.cache ignite table_name) my-key (get-doc-with-type no-sql-type no-sql))
                                                                                                          (peek vs-obj)))
                                                                                                  ))))
        :else
        (throw (Exception. "输入字符串错误！"))
        )
    )

(defn -myCachePop [^Ignite ignite ^Long group_id ^String no-sql-line]
    (my-cache-pop ignite group_id no-sql-line))



































