(ns org.gridgain.plus.dml.my-select-plus
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil MyDbUtil KvSql)
             (cn.plus.model.db MyScenesCache)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySelectPlus
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

; 函数的参数
(defrecord scenesPs [index ps_name])

; scenesObj 存到 my_meta_cache_all_scenes 中的对象
; 函数参数， 函数 ast
(defrecord scenesObj [lst_func_ps ast])

; 确定 query items 的权限
; 函数的参数
(defrecord table_select_view [schema_name table_name ast])

(defn get-scenes [^Ignite ignite ^String scenes_name]
    (scenesObj. nil nil))

(declare sql-to-ast get-my-sql-to-ast my-get-items)
(defn get-my-sql-to-ast [m]
                   (try
                       (sql-to-ast m)
                       (catch Exception e)))

(defn my-get-items
    ([lst] (my-get-items lst [] nil [] []))
    ([[f & r] stack mid-small stack-lst lst]
     (if (some? f)
         (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                             (recur r stack mid-small (conj stack-lst f) lst)
                             (recur r (conj stack f) "small" (conj stack-lst f) lst))
               (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                             (recur r stack mid-small (conj stack-lst f) lst)
                             (recur r (conj stack f) "mid" (conj stack-lst f) lst))
               (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                             (recur r stack mid-small (conj stack-lst f) lst)
                             (recur r (conj stack f) "big" (conj stack-lst f) lst))
               (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) lst)
                               (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) lst)
                               (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) lst)
                               )
               (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) lst)
                               (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) lst)
                               (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) lst)
                               )
               (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) lst)
                               (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) lst)
                               (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) lst)
                               )
               (= f ",") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                             (recur r [] nil [] (conj lst stack-lst))
                             (recur r stack mid-small (conj stack-lst f) lst))
               :else
               (recur r stack mid-small (conj stack-lst f) lst)
               )
         (if-not (empty? stack-lst)
             (conj lst stack-lst)
             lst))))

(defn sql-to-ast [^clojure.lang.LazySeq sql-lst]
    (letfn [
            (get-items
                ([lst] (get-items lst [] nil [] []))
                ([[f & r] stack mid-small stack-lst lst]
                 (if (some? f)
                     (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) lst)
                                         (recur r (conj stack f) "small" (conj stack-lst f) lst))
                           (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) lst)
                                         (recur r (conj stack f) "mid" (conj stack-lst f) lst))
                           (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                                         (recur r stack mid-small (conj stack-lst f) lst)
                                         (recur r (conj stack f) "big" (conj stack-lst f) lst))
                           (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) lst)
                                           (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) lst)
                                           (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) lst)
                                           )
                           (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) lst)
                                           (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) lst)
                                           (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) lst)
                                           )
                           (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) lst)
                                           (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) lst)
                                           (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) lst)
                                           )
                           (= f ",") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                         (recur r [] nil [] (conj lst stack-lst))
                                         (recur r stack mid-small (conj stack-lst f) lst))
                           :else
                           (recur r stack mid-small (conj stack-lst f) lst)
                           )
                     (if-not (empty? stack-lst)
                         (conj lst stack-lst)
                         lst))))
            (get-items-dic
                ([lst] (get-items-dic lst [] nil [] [] []))
                ([[f & r] stack mid-small stack-lst k-v lst]
                 (if (some? f)
                     (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst)
                                         (recur r (conj stack f) "small" (conj stack-lst f) k-v lst))
                           (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst)
                                         (recur r (conj stack f) "mid" (conj stack-lst f) k-v lst))
                           (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst)
                                         (recur r (conj stack f) "big" (conj stack-lst f) k-v lst))
                           (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) k-v lst)
                                           (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) k-v lst)
                                           (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                           )
                           (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) k-v lst)
                                           (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) k-v lst)
                                           (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                           )
                           (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) k-v lst)
                                           (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) k-v lst)
                                           (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                           )
                           (= f ",") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                         (if (= (count k-v) 1)
                                             (recur r [] nil [] [] (conj lst (conj k-v stack-lst)))
                                             (throw (Exception. (format "字符串格式错误 %s" (str/join lst)))))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst))
                           (= f ":") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                         (recur r [] nil [] (conj k-v stack-lst) lst)
                                         (recur r stack mid-small (conj stack-lst f) k-v lst))
                           :else
                           (recur r stack mid-small (conj stack-lst f) k-v lst)
                           )
                     (if (and (not (empty? stack-lst)) (not (empty? k-v)))
                         (conj lst (conj k-v stack-lst))
                         lst))))
            (kv-to-token [lst-dic]
                (loop [[f-dic & r-dic] lst-dic lst-kv []]
                    (if (some? f-dic)
                        (recur r-dic (conj lst-kv {:key (to-token (first f-dic)) :value (to-token (last f-dic))}))
                        lst-kv)))
            (to-token [vs]
                (cond (and (= (first vs) "[") (= (last vs) "]")) {:seq-obj (get-item-tokens (my-lexical/get-contain-lst vs))}
                      (and (= (first vs) "{") (= (last vs) "}")) {:map-obj (get-item-tokens (my-lexical/get-contain-lst vs))}
                      :else
                      (get-token vs)
                      ))
            (get-item-tokens [lst]
                (loop [[f & r] (get-items lst) lst-rs []]
                    (if (some? f)
                        (cond (and (= (first f) "[") (= (last f) "]")) (recur r (conj lst-rs {:seq-obj (get-item-tokens (my-lexical/get-contain-lst f))}))
                              (and (= (first f) "{") (= (last f) "}")) (let [lst-dic (get-items-dic (my-lexical/get-contain-lst f))]
                                                                           (recur r (conj lst-rs {:map-obj (kv-to-token lst-dic)})))
                              :else
                              (recur r (conj lst-rs (get-token f))))
                        (if (= (count lst-rs) 1)
                            (first lst-rs)
                            lst-rs))))
            (my-item-tokens [lst]
                (cond (and (= (first lst) "[") (= (last lst) "]")) (get-item-tokens lst) ;{:seq-obj (get-item-tokens lst)}
                      (and (= (first lst) "{") (= (last lst) "}")) (get-item-tokens lst) ;{:map-obj (get-item-tokens lst)}
                      :else
                      (get-token lst)))
            ; 判断是 () 的表达式
            (is-operate-fn?
                ([lst] (if (and (= (first lst) "(") (= (last lst) ")")) (let [m (is-operate-fn? lst [] [] [])]
                                                                            (if (and (some? m) (= (count m) 1)) (take (- (count (nth m 0)) 2) (rest (nth m 0)))))))
                ([[f & rs] stack lst result-lst]
                 (if (some? f)
                     (cond
                         (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
                         (= f ")") (if (= (count stack) 1) (recur rs (pop stack) [] (concat result-lst [(conj lst f)])) (if (> (count stack) 0) (recur rs (pop stack) (conj lst f) result-lst) (recur rs [] (conj lst f) result-lst)))
                         :else
                         (recur rs stack (conj lst f) result-lst)
                         ) (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))
            ; 按照 and or 切分字符串
            (get-where-line
                ([lst] (get-where-line lst [] [] []))
                ([[f & rs] stack lst result-lst]
                 (if (some? f)
                     (cond (and (contains? #{"and" "or" "between"} (str/lower-case f)) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
                           (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
                           (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
                           :else
                           (recur rs stack (conj lst f) result-lst)
                           )
                     (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))
            ; 处理多个
            (get-where-item-line
                ([lst] (get-where-item-line lst [] [] []))
                ([[f & rs] stack lst result-lst]
                 (if (some? f)
                     (cond (and (contains? #{">=" "<=" "<>" ">" "<" "=" "!=" "in" "exists"} (str/lower-case f)) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
                           (and (my-lexical/is-eq? "not" f) (my-lexical/is-eq? "in" (first rs)) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "not in"])) (recur rs stack [] result-lst))
                           (and (my-lexical/is-eq? "not" f) (my-lexical/is-eq? "exists" (first rs)) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "not exists"])) (recur rs stack [] result-lst))
                           (and (my-lexical/is-eq? "is" f) (my-lexical/is-eq? "not" (first rs)) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "is not"])) (recur rs stack [] result-lst))
                           (and (my-lexical/is-eq? "is" f) (not (my-lexical/is-eq? "not" (first rs))) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "is"])) (recur rs stack [] result-lst))
                           (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
                           (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
                           :else
                           (recur rs stack (conj lst f) result-lst)
                           )
                     (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))
            ; 处理算法运算符 #{"+" "-" "*" "/"}
            (arithmetic-fn
                ([lst] (arithmetic-fn lst [] [] []))
                ([[f & rs] stack lst result-lst]
                 (if (some? f)
                     (cond (and (contains? #{"+" "-" "*" "/"} f) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
                           (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
                           (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
                           :else
                           (recur rs stack (conj lst f) result-lst)
                           )
                     (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))
            ; 在函数内部处理 ,
            (my-comma-fn
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
            ; 处理 func lst_ps
            (func-lst-ps [lst]
                (letfn [(new-comma-fn
                            ([lst] (new-comma-fn lst []))
                            ([[f & r] lst]
                             (if (some? f)
                                 (if (nil? r)
                                     (recur r (conj lst f))
                                     (recur r (conj lst f ",")))
                                 lst)))]
                    (map my-item-tokens (new-comma-fn (get-items lst))))
                )
            ; 2、处理函数
            (func-fn [[f & rs]]
                (if (some? f) (let [m (is-operate-fn? rs)]
                                  (if (some? m) {:func-name f :lst_ps (func-lst-ps m)}))))
            ; 输入 line 获取 token
            (get-token-line [line]
                (letfn [
                        ; 1、常数的处理
                        (element-item [line]
                            (if (some? line)
                                (cond
                                    (contains? #{"+" "-" "*" "/"} line) {:operation_symbol line}
                                    (contains? #{"(" ")" "[" "]"} line) {:parenthesis_symbol line}
                                    (contains? #{">=" "<=" "<>" ">" "<" "=" "!="} line) {:comparison_symbol line}
                                    (contains? #{"and" "or" "between"} (str/lower-case line)) {:and_or_symbol (str/lower-case line)}
                                    (contains? #{"in" "not in"} (str/lower-case line)) {:in_symbol (str/lower-case line)}
                                    (contains? #{"exists" "not exists"} (str/lower-case line)) {:exists_symbol (str/lower-case line)}
                                    (contains? #{","} line) {:comma_symbol line}
                                    (some? (re-find #"^(?i)\d+$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Integer :const true}
                                    (some? (re-find #"^(?i)\d+\.\d$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type BigDecimal :const true}
                                    ;(some? (re-find #"^(?i)\"\w*\"$|^(?i)'\w*'$|^(?i)\"\W*\"$|^(?i)'\W*'$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
                                    (some? (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$|^\'\'$|^\"\"$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
                                    (some? (re-find #"^(?i)\d+D$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Double :const true}
                                    (some? (re-find #"^(?i)\d+L$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Long :const true}
                                    (some? (re-find #"^(?i)\d+F$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Float :const true}
                                    (some? (re-find #"^(?i)true$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Boolean :const true}
                                    (some? (re-find #"^(?i)false$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Boolean :const true}
                                    :else
                                    {:table_alias "" :item_name line :item_type "" :java_item_type nil :const false}
                                    )))
                        ; m.name 生成 {:table_alias "" :item_name line :item_type "" :java_item_type Integer :const false}
                        ; line = "m.name"
                        (get-item-alias [line]
                            (if-let [m (str/split line #"\.")]
                                (if (and (some? m) (= (count m) 2))
                                    {:item_name (nth m 1) :table_alias (nth m 0) :const false})))]
                    (if (and (some? line) (= (empty? line) false))
                        (cond
                            ; 如果是 m.name 这种形式
                            (some? (re-find #"^(?i)\w+\.\w+$" line)) (if (some? (re-find #"^(?i)\d+\.\d+$|^(?i)\d+\.\d+[DFL]$" line))
                                                                         (element-item line)
                                                                         (get-item-alias line))
                            :else
                            (element-item line)
                            ))))
            ; 处理四则运算
            ; 例如：a + b * (c - d)
            (operation [lst]
                (when-let [m (arithmetic-fn lst)]
                    (if (> (count m) 1) {:operation (map get-token m)})))
            ; 对括号的处理
            ; 例如：(a + b * c)
            (parenthesis
                [lst]
                (letfn [(is-sql-obj? [sql-obj]
                            (if (and (some? sql-obj) (instance? clojure.lang.LazySeq sql-obj) (map? (first sql-obj)) (contains? (first sql-obj) :sql_obj))
                                (let [{sqlObj :sql_obj} (first sql-obj)]
                                    (if (and (> (count (-> sqlObj :query-items)) 0) (> (count (-> sqlObj :table-items)) 0)) true false)) false)
                            )
                        (get-lazy [lst]
                            (if (instance? clojure.lang.LazySeq lst)
                                lst
                                (my-lexical/to-lazy lst)))
                        (eliminate-parentheses [lst]
                            (if (and (= (first lst) "(") (= (last lst) ")"))
                                (let [m (get-token (my-lexical/get-contain-lst lst))]
                                    (if-not (nil? m)
                                        m
                                        (eliminate-parentheses (my-lexical/get-contain-lst lst))))))]
                    (when-let [m (is-operate-fn? (get-lazy lst))]
                        (let [ast-m (get-my-sql-to-ast m)]
                            (if (is-sql-obj? ast-m)
                                {:parenthesis ast-m}
                                (let [where-line-m (get-where-line m)]
                                    (if (is-true? where-line-m)
                                        {:parenthesis (map get-token where-line-m)}
                                        (let [where-item-line-m (get-where-item-line m)]
                                            (if (is-true? where-item-line-m)
                                                {:parenthesis (map get-token where-item-line-m)}
                                                (let [comma-fn-m (my-comma-fn m)]
                                                    (if (is-true? comma-fn-m)
                                                        {:parenthesis (map get-token comma-fn-m)}
                                                        (let [fn-m (arithmetic-fn m)]
                                                            (if (is-true? fn-m)
                                                                {:parenthesis (map get-token fn-m)}
                                                                (let [tk-m (get-token m)]
                                                                    (if (is-true? tk-m)
                                                                        tk-m
                                                                        (eliminate-parentheses m)))))))))))))

                        ;(cond
                        ;    (is-sql-obj? sql-to-ast-m m)  {:parenthesis (sql-to-ast-m m)}
                        ;    (is-true? get-where-line-m m) {:parenthesis (map get-token (get-where-line-m m))}
                        ;    (is-true? get-where-item-line-m m) {:parenthesis (map get-token (get-where-item-line-m m))}
                        ;    (is-true? my-comma-fn-m m) {:parenthesis (map get-token (my-comma-fn-m m))}
                        ;    (is-true? arithmetic-fn-m m) {:parenthesis (map get-token (arithmetic-fn-m m))}
                        ;    )
                        )))
            ; 判断执行 f 函数 p 参数
            (is-true? [obj]
                (and (some? obj) (> (count obj) 1)))
            ; Smart Data Struct 类型 序列和字典
            (ds-fun-lst
                ([lst] (ds-fun-lst lst [] [] nil []))
                ([[f & r] stack stack-lst func-name lst]
                 (if (some? f)
                     (cond (= f "(") (if-not (nil? func-name)
                                         (recur r (conj stack f) (conj stack-lst f) func-name lst))
                           (= f ")") (if-not (nil? func-name)
                                         (if (= (count stack) 1)
                                             (recur r [] [] nil (conj lst (cons func-name (conj stack-lst f))))
                                             (recur r stack (conj stack-lst f) func-name lst)))
                           :else
                           (if (nil? func-name)
                               (recur r [] [] f lst)
                               (recur r stack (conj stack-lst f) func-name lst))
                           )
                     (if (and (not (nil? func-name)) (not (empty? stack-lst)))
                         (conj lst [func-name stack-lst])
                         lst))))
            (ds-func
                ([lst] (ds-func lst nil))
                ([[f & r] my-obj]
                 (if (some? f)
                     (let [func-obj (get-my-sql-to-ast f)]
                         (let [[schema_name func-name] (str/split (-> func-obj :func-name) #"\.")]
                             (if (nil? my-obj)
                                 (recur r (assoc func-obj :ds-name schema_name :func-name func-name))
                                 (recur r (assoc my-obj :ds-lst (assoc func-obj :ds-name schema_name :func-name func-name))))))
                     my-obj)))
            ; 获取 token ast
            (get-token
                [lst]
                (if (some? lst)
                    (if (string? lst)
                        (get-token-line lst)
                        (if (and (= (count lst) 1) (string? (first lst)))
                            (get-token-line (first lst))
                            (let [where-line-m (get-where-line lst)]
                                (if (is-true? where-line-m)
                                    (map get-token where-line-m)
                                    (let [where-item-line-m (get-where-item-line lst)]
                                        (if (is-true? where-item-line-m)
                                            (map get-token where-item-line-m)
                                            (if-let [fn-m (func-fn lst)]
                                                fn-m
                                                (if-let [oprate-m (operation lst)]
                                                    oprate-m
                                                    (if-let [p-m (parenthesis lst)]
                                                        p-m
                                                        (let [not-exists-m (parenthesis (rest (rest lst)))]
                                                            (if (and (my-lexical/is-eq? (first lst) "not") (my-lexical/is-eq? (second lst) "exists") (some? not-exists-m))
                                                                {:exists "not exists" :select_sql not-exists-m}
                                                                (let [exists-m (parenthesis (rest lst))]
                                                                    (if (and (my-lexical/is-eq? (first lst) "exists") (some? exists-m))
                                                                        {:exists "exists" :select_sql exists-m}
                                                                        (if-let [ds-m (ds-func (ds-fun-lst lst))]
                                                                            ds-m))))))))
                                            ))))
                            )
                        ;(cond (and (= (count lst) 1) (string? (first lst))) (get-token-line (first lst))
                        ;      (is-true? get-where-line-m lst) (map get-token (get-where-line-m lst))
                        ;      (is-true? get-where-item-line-m lst) (map get-token (get-where-item-line-m lst))
                        ;      (some? (func-fn-m lst)) (when-let [m (func-fn-m lst)] m)
                        ;      (some? (operation-m lst)) (when-let [m (operation-m lst)] m)
                        ;      (some? (parenthesis-m lst)) (when-let [m (parenthesis-m lst)] m)
                        ;      (and (my-lexical/is-eq? (first lst) "not") (my-lexical/is-eq? (second lst) "exists") (some? (parenthesis-m (rest (rest lst))))) (when-let [m (parenthesis-m (rest (rest lst)))] {:exists "not exists" :select_sql m})
                        ;      (and (my-lexical/is-eq? (first lst) "exists") (some? (parenthesis-m (rest lst)))) (when-let [m (parenthesis-m (rest lst))] {:exists "exists" :select_sql m})
                        ;      )
                        )))
            ; 预处理 get-query-items 输入
            (pre-query-lst [[f & rs]]
                (if (some? f)
                    (cond (string? (first f)) (if (my-lexical/is-eq? (first f) "distinct") (concat [{:keyword "distinct"}] (pre-query-lst (concat [(rest f)] rs))) (concat [f] (pre-query-lst rs)))
                          :else
                          (concat [f] (pre-query-lst rs)))))
            ; 处理别名
            (get-item-lst [lst]
                (letfn [(get-item-rv [[f & rs]]
                            (cond
                                (= (count (concat [f] rs)) 1) {:item-lst (concat [f]) :alias nil}
                                (my-lexical/is-eq? (first rs) "as") {:item-lst (reverse (rest rs)) :alias f}
                                (and (= (count rs) 1) (= (first rs) (last rs))) {:item-lst (first rs) :alias f}
                                (and (my-lexical/is-word? f) (= (first rs) ")")) {:item-lst (reverse rs) :alias f}
                                (and (my-lexical/is-word? f) (my-lexical/is-word? (first rs)) (not (my-lexical/is-word? (second rs))) (> (count rs) 2)) {:item-lst (reverse rs) :alias f}
                                :else
                                {:item-lst (reverse (concat [f] rs)) :alias nil}
                                ))]
                    (if (> (count lst) 1) (get-item-rv (reverse lst)) {:item-lst lst :alias nil})))
            ; 获取 table
            (get-table-items [table-items]
                (letfn [(is-select?
                            ([lst] (if (some? lst)
                                       (if (and (= (first lst) "(") (= (last lst) ")"))
                                           (let [m (is-select? (rest lst) [])]
                                               (if (and (some? m) (> (count m) 0))
                                                   (when-let [sql_objs (get-my-sql-to-ast m)]
                                                       (if (> (count sql_objs) 0) true)))))))
                            ([[f & r] my-lst]
                             (if (empty? r) my-lst (recur r (concat my-lst [f])))))
                        (get-select-line
                            ([lst] (if (some? lst) (get-select-line (rest lst) [])))
                            ([[f & r] my-lst]
                             (if (empty? r) my-lst (recur r (concat my-lst [f])))))
                        ; 处理 join 类型的
                        (table-join [[f & rs]]
                            (if (some? f)
                                (cond (contains? f :tables)
                                      (let [{tables :tables} f]
                                          (cond (= (count tables) 1)(concat [(assoc (my-lexical/get-schema (first tables)) :table_alias "")] (table-join rs))
                                                (= (count tables) 2) (concat [(assoc (my-lexical/get-schema (nth tables 0)) :table_alias (nth tables 1))] (table-join rs))
                                                (and (= (count tables) 3) (my-lexical/is-eq? (nth tables 1) "as")) (concat [(assoc (my-lexical/get-schema (nth tables 0)) :table_alias (nth tables 2))] (table-join rs))
                                                :else
                                                (throw (Exception. "sql 语句错误！from 关键词之后"))
                                                ))
                                      (contains? f :join) (concat [{:join (-> f :join)}] (table-join rs))
                                      (contains? f :on) (cons {:on (map get-token (get f :on))} (table-join rs))
                                      )))
                        ; 处理 table-items
                        (get-table
                            ([lst] (when-let [m (get-table (reverse lst) [] [])]
                                       (if (> (count m) 1) (reverse m) m)))
                            ([[f & rs] stack lst]
                             (if (some? f)
                                 (cond (and (my-lexical/is-eq? f "on") (= (count stack) 0)) (if (> (count lst) 0) (concat [{:on (reverse lst)}] (get-table rs stack [])) (get-table rs stack []))
                                       (and (my-lexical/is-eq? f "join") (contains? #{"left" "inner" "right"} (str/lower-case (first rs))) (= (count stack) 0)) (if (> (count lst) 0) (concat [{:tables (reverse lst)}] [{:join (str/join [(first rs) " " f])}] (get-table (rest rs) stack [])) (get-table (rest rs) stack []))
                                       (and (my-lexical/is-eq? f "join") (not (contains? #{"left" "inner" "right"} (str/lower-case (first rs)))) (= (count stack) 0)) (if (> (count lst) 0) (concat [{:tables (reverse lst)}] [{:join f}] (get-table rs stack [])) (get-table rs stack []))
                                       (= f ")") (get-table rs (conj stack f) (conj lst f))
                                       (= f "(") (get-table rs (pop stack) (conj lst f))
                                       :else
                                       (get-table rs stack (conj lst f))
                                       )
                                 (if (> (count lst) 0) [{:tables (reverse lst)}]))))
                        ; 处理逗号类型的
                        (table-comma
                            [lst]
                            (if (and (string? lst) (my-lexical/is-eq? lst ","))
                                (get-token-line lst)
                                (cond (= (count lst) 1) (assoc (my-lexical/get-schema (first lst)) :table_alias nil) ;{:table_name (first lst) :table_alias ""}
                                      (= (count lst) 2) (assoc (my-lexical/get-schema (first lst)) :table_alias (second lst)) ;{:table_name (nth lst 0) :table_alias (nth lst 1)}
                                      (and (= (count lst) 3) (my-lexical/is-eq? (nth lst 1) "as")) (assoc (my-lexical/get-schema (first lst)) :table_alias (last lst)) ;{:table_name (nth lst 0) :table_alias (nth lst 2)}
                                      :else
                                      (when-let [m (get-query-items (concat [lst]))]
                                          (first m))
                                      )))
                        ]
                    (if (= (count table-items) 1)
                        (let [m (nth table-items 0)]
                            (cond (string? m) (concat [(assoc (my-lexical/get-schema m) :table_alias nil)])
                                  (and (my-lexical/is-seq? m) (is-select? m)) {:parenthesis (get-my-sql-to-ast (get-select-line m))}
                                  :else
                                  (if (my-lexical/is-contains? (nth table-items 0) "join")
                                      (table-join (get-table (nth table-items 0)))
                                      (map table-comma table-items))))
                        (map table-comma table-items)))
                )
            ; 获取 order by
            (get-order-by
                ([lst] (let [m (reverse lst)] (get-order-by (reverse (rest m)) (first m))))
                ([f l]
                 (if (and (some? f) (some? l))
                     {:order-item (map get-token f) :order l})))
            ; 获取 limit
            (get-limit [lst]
                (let [m (my-comma-fn lst)]
                    (if (= (count m) 3) (map get-token m))))
            ; query items 处理
            (get-query-items [lst]
                (when-let [[f & rs] (pre-query-lst lst)]
                    (if (map? f) (concat [f] (get-query-items rs))
                                 (when-let [{item-lst :item-lst alias :alias} (get-item-lst f)]
                                     (concat [(assoc (get-token item-lst) :alias alias)] (get-query-items rs))))))
            (sql-to-ast-single [sql-lst]
                (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} (my-lexical/get-segments-list sql-lst)]
                    {:query-items (get-query-items (my-lexical/to-lazy query-items)) :table-items (get-table-items (my-lexical/to-lazy table-items)) :where-items (get-token where-items) :group-by (get-token group-by) :having (get-token having) :order-by (get-order-by order-by) :limit (get-limit limit)}))
            (to-ast [lst]
                (if (string? lst) {:keyword lst}
                                           {:sql_obj (sql-to-ast-single lst)}))]
        (if-not (my-lexical/is-eq? (first sql-lst) "select")
            (get-token sql-lst)
            (when-let [m (my-lexical/sql-union sql-lst)]
                (map to-ast m)))))

; 1、替换 function 在 select ... from table, func(a, b) where ... 中用到
(defn find-table-func [^Ignite ignite ast]
    (letfn [(map-replace
                ([ignite m] (map-replace ignite (keys m) m))
                ([ignite [f & rs] m]
                 (if (some? f)
                     (let [vs (get m f)]
                         (cond (= f :table-items) (cond (instance? clojure.lang.LazySeq vs) (recur ignite rs (assoc m f (map (partial re-func ignite) vs)))
                                                        (map? vs) (recur ignite rs (assoc m f (plus-func ignite vs)))
                                                        :else
                                                        (recur ignite rs m))
                               (not (= f :table-items)) (cond (instance? clojure.lang.LazySeq vs) (recur ignite rs (assoc m f (map (partial plus-func ignite) vs)))
                                                              (map? vs) (recur ignite rs (assoc m f (plus-func ignite vs)))
                                                              :else
                                                              (recur ignite rs m))
                               :else
                               (recur ignite rs m))) m)))
            (re-func [ignite m]
                (if (some? m)
                    (cond (my-lexical/is-seq? m) (find-table-func ignite m)
                          (map? m) (if (contains? m :func-name)
                                       (let [func (my-context/get-func-scenes ignite (get m :func-name))]
                                           (if (some? func) (cond (= func "func") (throw (Exception. "自定义方法不能当作结果来查询！"))
                                                                  (= func "builtin") (throw (Exception. "内置方法不能当作结果来查询！"))
                                                                  (= func "scenes") (if (contains? m :alias) {:parenthesis (get-sql-ast ignite m) :alias (get m :alias)} {:parenthesis (get-sql-ast ignite m)})))) (find-table-func ignite m))
                          :else
                          (find-table-func ignite m))))
            (plus-func [ignite m]
                (if (some? m)
                    (cond (my-lexical/is-seq? m) (find-table-func ignite m)
                          (map? m) (if (contains? m :func-name)
                                       (let [func (my-context/get-func-scenes ignite (get m :func-name))]
                                           (if (some? func) (cond (= func "func") (concat [(func_scenes_invoke m)])
                                                                  (= func "scenes") (concat [(func_scenes_invoke m)])
                                                                  :else
                                                                  (find-table-func ignite m)) (find-table-func ignite m))) (find-table-func ignite m))
                          :else
                          (find-table-func ignite m))))
            ; 调用 func 或 scenes
            (func_scenes_invoke [func_ast]
                (if (and (contains? func_ast :lst_ps) (> (count (get func_ast :lst_ps)) 0))
                    (assoc {:func-name "myInvoke"} :lst_ps (concat [{:table_alias "", :item_name (String/format "'%s'" (object-array [(get func_ast :func-name)])), :item_type "", :java_item_type "String", :const true} {:comma_symbol ","}] (get func_ast :lst_ps)))
                    (assoc {:func-name "myInvoke"} :lst_ps (concat [{:table_alias "", :item_name (String/format "'%s'" (object-array [(get func_ast :func-name)])), :item_type "", :java_item_type "String", :const true}] (get func_ast :lst_ps)))))
            ; 输入 func ast 提取对应的 sql ast
            ; 并提取 lst_ps
            (get-sql-ast [ignite func-ast]
                (when-let [func_obj (get-scenes ignite (get func-ast :func-name))]
                    (let [my_func_ast (:ast func_obj) lst_func_ps (:lst_func_ps func_obj) lst_ps (get func-ast :lst_ps)]
                        (re-func-ast my_func_ast lst_ps lst_func_ps 0)
                        )))
            (re-func-ast [my_func_ast [f & rs] lst_func_ps index]
                (if (some? f)
                    (if (not (contains? f :comma_symbol))
                        (re-func-ast (re-func-obj f (:ps_name (nth lst_func_ps index)) my_func_ast) rs lst_func_ps (+ index 1))
                        (re-func-ast my_func_ast rs lst_func_ps index))
                    my_func_ast))
            ; 输入 ps_lst 替换掉原来对应的元素
            ; 例如：target_ps 参数的 ast
            ; resourc_ps 参数名称 :a 例如：{:table_alias "", :item_name ":a", :item_type "", :java_item_type nil, :const false} 中的 item_name
            ; 具体做法是遍历 ast 替换到
            (re-func-obj [target_ps resourc_ps my_func_ast]
                (if (some? my_func_ast)
                    (cond (instance? clojure.lang.LazySeq my_func_ast) (map (partial re-func-obj target_ps resourc_ps) my_func_ast)
                          (map? my_func_ast) (re-func-obj-map target_ps resourc_ps my_func_ast))))
            (re-func-obj-map [target_ps resourc_ps my_func_ast]
                (if (and (contains? my_func_ast :item_name) (my-lexical/is-eq? (get my_func_ast :item_name) resourc_ps)) target_ps
                                                                                                                         (re-func-obj-map-sub target_ps resourc_ps (keys my_func_ast) my_func_ast)))
            (re-func-obj-map-sub [target_ps resourc_ps [f & rs] my_func_ast]
                (if (some? f)
                    (cond (instance? clojure.lang.LazySeq (get my_func_ast f))
                          (recur target_ps resourc_ps rs (assoc my_func_ast f (map (partial re-func-obj target_ps resourc_ps) (get my_func_ast f))))
                          (map? (get my_func_ast f))
                          (let [m (re-func-obj target_ps resourc_ps (get my_func_ast f))]
                              (recur target_ps resourc_ps rs (assoc my_func_ast f m)))
                          :else
                          (recur target_ps resourc_ps rs my_func_ast)
                          ) my_func_ast))]
        (if (some? ast)
                  (cond (instance? clojure.lang.LazySeq ast) (map (partial find-table-func ignite) ast)
                        (map? ast) (map-replace ignite ast)))))

(defn get_query_table [ignite group_id ast]
    (letfn [
            ; 将 table-items 中的 table_name 和 table_alias 转换为 map
            ; 输入 ignite group_id
            ; table-items = (get select_ast :table-items)
            ; 返回结果：map ，key 值为 别名， value 为 table_select_view
            (get_map_table_items
                ([ignite group_id table-items] (get_map_table_items ignite group_id table-items {}))
                ([ignite group_id [f & rs] m]
                 (if (some? f)
                     (if (contains? f :table_name)
                         (let [table_ast (get_select_view ignite group_id (get f :schema_name) (get f :table_name))]
                             (if (some? table_ast)
                                 (if (contains? f :table_alias)
                                     (recur ignite group_id rs (assoc m (get f :table_alias) (table_select_view. (get f :schema_name) (str/lower-case (str/trim (get f :table_name))) table_ast)))
                                     (recur ignite group_id rs (assoc m "" (table_select_view. (get f :schema_name) (get f :table_name) table_ast))))
                                 (if (contains? f :table_alias)
                                     (recur ignite group_id rs (assoc m (get f :table_alias) (table_select_view. (get f :schema_name) (get f :table_name) nil)))
                                     (recur ignite group_id rs (assoc m "" (table_select_view. (get f :schema_name) (get f :table_name) nil)))))) (recur ignite group_id rs m)) m)))
            ; 获取 table_select_view 的 ast
            ; 重新生成新的 ast
            ; 新的 ast = {query_item = {'item_name': '转换的函数'}}
            (get_select_view [ignite group_id schema_name talbe_name]
                (when-let [code (first (.getAll (.query (.cache ignite "my_select_views") (.setArgs (SqlFieldsQuery. "select m.code from my_select_views as m, my_dataset as ds, my_group_view as v where m.data_set_id = ds.id and m.id = v.view_id and ds.dataset_name = ? and m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [schema_name talbe_name group_id "查"])))))]
                    (when-let [sql_objs (get-my-sql-to-ast (my-lexical/to-back (nth code 0)))]
                        (if (= (count sql_objs) 1)
                            (when-let [{query-items :query-items where-items :where-items} (get (nth sql_objs 0) :sql_obj)]
                                {:query-items (get_query_view query-items) :where-items where-items})))))
            ; 输入参数：
            ; ignite
            ; query_ast 是 query item 的树
            ; group_id : 用户组 ID
            (query_authority [ignite group_id table_alias_ast query_ast]
                (if (some? query_ast)
                    (cond (instance? clojure.lang.LazySeq query_ast) (map (partial query_authority ignite group_id table_alias_ast) query_ast)
                          (map? query_ast) (if (and (contains? query_ast :query-items) (contains? query_ast :table-items))
                                               (get_query_table ignite group_id query_ast)
                                               (query_map ignite group_id table_alias_ast query_ast)))))
            (query_map
                ([ignite group_id table_alias_ast query_ast] (query_map ignite group_id table_alias_ast (keys query_ast) query_ast))
                ([ignite group_id table_alias_ast [f & rs] query_ast]
                 (if (some? f)
                     (let [vs (get query_ast f)]
                         (cond (and (= f :item_name) (= (get query_ast :const) false)) (if (contains? table_alias_ast (get query_ast :table_alias))
                                                                                           ; table_obj 是 table_select_view 的对象
                                                                                           (let [table_obj (get table_alias_ast (get query_ast :table_alias))]
                                                                                               (if (and (some? table_obj) (some? (get table_obj :ast)) (some? (get (get table_obj :ast) :query-items)))
                                                                                                   (if (contains? (get (:ast table_obj) :query-items) (str/lower-case (str/trim (get query_ast :item_name))))
                                                                                                       (replace_alias (get query_ast :table_alias) (get (get (:ast table_obj) :query-items) (get query_ast :item_name)))
                                                                                                       (throw (Exception. (String/format "没有查询字段(%s)的权限" (object-array [(get query_ast :item_name)])))))
                                                                                                   query_ast)) query_ast;(throw (Exception. (String/format "没有查询字段(%s)，请仔细检查拼写是否正确？" (object-array [(get query_ast :item_name)]))))
                                                                                           )
                               (instance? clojure.lang.LazySeq vs) (recur ignite group_id table_alias_ast rs (assoc query_ast f (map (partial query_authority ignite group_id table_alias_ast) vs)))
                               (map? vs) (recur ignite group_id table_alias_ast rs (assoc query_ast f (query_authority ignite group_id table_alias_ast vs)))
                               :else
                               (recur ignite group_id table_alias_ast rs query_ast))) query_ast)))
            (get_where [[f & r] table_select_view_obj where-items]
                (if (some? f)
                    (let [table_select (get table_select_view_obj f)]
                        (if (some? table_select)
                            (let [view_where (get (:ast table_select) :where-items)]
                                (if (some? view_where)
                                    (if (some? where-items) (recur r table_select_view_obj (concat [{:parenthesis (replace_alias f view_where)} {:and_or_symbol "and"} {:parenthesis where-items}]))
                                                            (recur r table_select_view_obj (replace_alias f view_where))) (recur r table_select_view_obj where-items))) (recur r table_select_view_obj where-items))) where-items))
            (replace_alias [alias ast]
                (if (some? ast)
                    (cond (instance? clojure.lang.LazySeq ast) (map (partial replace_alias alias) ast)
                          (map? ast) (map_replace_alias alias (keys ast) ast))))
            (map_replace_alias
                ([alias m] (map_replace_alias alias (keys m) m))
                ([alias [f & rs] m]
                 (if (some? f)
                     (let [vs (get m f)]
                         (cond (and (= f :item_name) (= (get m :const) false)) (assoc m :table_alias alias)
                               (instance? clojure.lang.LazySeq vs) (recur alias rs (assoc m f (map (partial replace_alias alias) vs)))
                               (map? vs) (recur alias rs (assoc m f (replace_alias alias vs)))
                               :else
                               (recur alias rs m))) m)))
            (get_query_view
                ([query-items] (get_query_view query-items {}))
                ([[f & r] dic]
                 (if (some? f)
                     (cond (contains? f :item_name) (recur r (assoc dic (get f :item_name) nil))
                           (and (contains? f :func-name) (my-lexical/is-eq? (get f :func-name) "convert_to") (= (count (get f :lst_ps)) 3)) (recur r (assoc dic (get (first (get f :lst_ps)) :item_name) (last (get f :lst_ps))))
                           (contains? f :comma_symbol) (recur r dic)
                           :else
                           (throw (Exception. "select 权限视图中只能是字段或者是转换函数！")))
                     dic)))
            (get_query_table_map [ignite group_id [f & rs] ast]
                (if (some? f)
                    (let [vs (get ast f)]
                        (cond (instance? clojure.lang.LazySeq vs) (recur ignite group_id rs (assoc ast f (map (partial get_query_table ignite group_id) vs)))
                              (map? vs) (recur ignite group_id rs (assoc ast f (get_query_table ignite group_id vs)))
                              :else
                              (recur ignite group_id rs ast))) ast))
            ]
        (if (and (map? ast) (contains? ast :query-items) (contains? ast :table-items))
            (let [table_obj (get_map_table_items ignite group_id (get ast :table-items))]
                (if (some? table_obj)
                    (let [qt (query_authority ignite group_id table_obj (get ast :query-items)) where (get_where (keys table_obj) table_obj (get ast :where-items))]
                        (assoc ast :query-items qt :where-items where)) ast))
            (cond (instance? clojure.lang.LazySeq ast) (map (partial get_query_table ignite group_id) ast)
                  (map? ast) (get_query_table_map ignite group_id (keys ast) ast)))))

; 转换成查询字符串
(def sql_symbol #{"(" ")" "/" "*" "-" "+" "=" ">" "<" ">=" "<=" "<>" ","})

; array 转换为 sql
(defn ar-to-sql [[f & rs] ^StringBuilder sb]
           (cond (and (some? f) (some? (first rs))) (cond (and (my-lexical/is-eq? f "(") (my-lexical/is-eq? (first rs) "select")) (recur rs (.append sb f))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "from")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "where")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "or")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "in")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "exists")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "and")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "union")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "union all")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "not in")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "not exists")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "from") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "where") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "or") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "and") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "in") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "union") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "union all") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                          (and (my-lexical/is-eq? f "not in") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))

                                                          (and (not (contains? sql_symbol f)) (not (contains? sql_symbol (first rs)))) (recur rs (.append (.append sb f) " "))
                                                          :else
                                                          (recur rs (.append sb f)))
                 (and (some? f) (Strings/isNullOrEmpty (first rs))) (recur rs (.append sb f))
                 :else
                 sb))

(defn my-array-to-sql [lst]
                 (if (nil? lst) nil
                                (letfn [(ar-to-lst [[f & rs]]
                                            (if (some? f)
                                                (try
                                                    (if (string? f) (cons f (ar-to-lst rs))
                                                                    (concat (ar-to-lst f) (ar-to-lst rs)))
                                                    (catch Exception e (.getMessage e))) []))]
                                    (if (string? lst) lst
                                                      (.toString (ar-to-sql (ar-to-lst lst) (StringBuilder.)))))
                                ))

; ast to sql
(defn ast_to_sql [ignite group_id ast]
    (letfn [(select_to_sql_single [ignite group_id ast]
                (if (and (some? ast) (map? ast))
                    (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} ast]
                        (cond (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                              ))))
            (lst-token-to-line
                ([ignite group_id lst_token] (cond (string? lst_token) lst_token
                                                   (map? lst_token) (my-array-to-sql (token-to-sql ignite group_id lst_token))
                                                   :else
                                                   (my-array-to-sql (lst-token-to-line ignite group_id lst_token []))))
                ([ignite group_id [f & rs] lst]
                 (if (some? f)
                     (recur ignite group_id rs (conj lst (my-array-to-sql (token-to-sql ignite group_id f)))) lst)))
            (token-to-sql [ignite group_id m]
                (if (some? m)
                    (cond (my-lexical/is-seq? m) (map (partial token-to-sql ignite group_id) m)
                          (map? m) (map-token-to-sql ignite group_id m))))
            (map-token-to-sql
                [ignite group_id m]
                (if (some? m)
                    (cond
                        (contains? m :sql_obj) (select-to-sql ignite group_id m)
                        (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-line ignite group_id m)
                        (contains? m :and_or_symbol) (get m :and_or_symbol)
                        (contains? m :keyword) (get m :keyword)
                        (contains? m :operation) (map (partial token-to-sql ignite group_id) (get m :operation))
                        (contains? m :comparison_symbol) (get m :comparison_symbol)
                        (contains? m :in_symbol) (get m :in_symbol)
                        (contains? m :operation_symbol) (get m :operation_symbol)
                        (contains? m :join) (get m :join)
                        (contains? m :on) (on-to-line ignite group_id m)
                        (contains? m :comma_symbol) (get m :comma_symbol)
                        (contains? m :order-item) (concat (token-to-sql ignite group_id (-> m :order-item)) [(-> m :order)])
                        (contains? m :item_name) (item-to-line m)
                        (contains? m :table_name) (table-to-line ignite group_id m)
                        (contains? m :exists) (concat [(get m :exists) "("] (token-to-sql ignite group_id (get (get m :select_sql) :parenthesis)) [")"])
                        (contains? m :parenthesis) (concat ["("] (token-to-sql ignite group_id (get m :parenthesis)) [")"])
                        :else
                        (throw (Exception. "select 语句错误！请仔细检查！"))
                        )))
            (on-to-line [ignite group_id m]
                (if (some? m)
                    (str/join ["on " (str/join " " (token-to-sql ignite group_id (get m :on)))])))
            (func-to-line [ignite group_id m]
                (if (and (contains? m :alias) (not (nil? (-> m :alias))))
                    (concat [(-> m :func-name) "("] (map (partial token-to-sql ignite group_id) (-> m :lst_ps)) [")" " as"] [(-> m :alias)])
                    (concat [(-> m :func-name) "("] (map (partial token-to-sql ignite group_id) (-> m :lst_ps)) [")"])))
            (item-to-line [m]
                (let [{table_alias :table_alias item_name :item_name alias :alias} m]
                    (cond
                        (and (not (Strings/isNullOrEmpty table_alias)) (not (nil? alias)) (not (Strings/isNullOrEmpty alias))) (str/join [table_alias "." item_name " as " alias])
                        (and (not (Strings/isNullOrEmpty table_alias)) (Strings/isNullOrEmpty alias)) (str/join [table_alias "." item_name])
                        (and (Strings/isNullOrEmpty table_alias) (Strings/isNullOrEmpty alias)) item_name
                        )))
            ;(table-to-line [ignite group_id m]
            ;    (if (some? m)
            ;        (if-let [{table_name :table_name table_alias :table_alias} m]
            ;            (if (Strings/isNullOrEmpty table_alias)
            ;                (let [data_set_name (get_data_set_name ignite group_id)]
            ;                    (if (Strings/isNullOrEmpty data_set_name)
            ;                        table_name
            ;                        (str/join [data_set_name "." table_name])))
            ;                (let [data_set_name (get_data_set_name ignite group_id)]
            ;                    (if (Strings/isNullOrEmpty data_set_name)
            ;                        (str/join [table_name " " table_alias])
            ;                        (str/join [(str/join [data_set_name "." table_name]) " " table_alias])))
            ;                ))))
            (table-to-line [ignite group_id m]
                (if (some? m)
                    (if-let [{schema_name :schema_name table_name :table_name table_alias :table_alias} m]
                        (if-not (Strings/isNullOrEmpty schema_name)
                            (if (Strings/isNullOrEmpty table_alias)
                                (format "%s.%s" schema_name table_name)
                                (str/join [(format "%s.%s" schema_name table_name) " " table_alias]))
                            (if (= group_id 0)
                                (if (Strings/isNullOrEmpty table_alias)
                                    (format "MY_META.%s" table_name)
                                    (str/join [(format "MY_META.%s" table_name) " " table_alias]))
                                (let [schema_name (get_data_set_name ignite group_id)]
                                    (if (Strings/isNullOrEmpty table_alias)
                                        (format "%s.%s" schema_name table_name)
                                        (str/join [(format "%s.%s" schema_name table_name) " " table_alias])))))
                        )))
            ; 获取 data_set 的名字和对应的表
            (get_data_set_name [^Ignite ignite ^Long group_id]
                (when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.dataset_name from my_users_group as g JOIN my_dataset as m ON m.id = g.data_set_id where g.id = ?") (to-array [group_id])))))]
                    (first m)))
            (select-to-sql
                ([ignite group_id ast] (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (.toString (ar-to-sql (select-to-sql ignite group_id ast []) (StringBuilder.)))
                                             (contains? ast :sql_obj) (select_to_sql_single ignite group_id (get ast :sql_obj))
                                             :else
                                             (throw (Exception. "select 语句错误！"))))
                ([ignite group_id [f & rs] lst_rs]
                 (if (some? f)
                     (if (map? f)
                         (cond (contains? f :sql_obj) (recur ignite group_id rs (conj lst_rs (select_to_sql_single ignite group_id (get f :sql_obj))))
                               (contains? f :keyword) (recur ignite group_id rs (conj lst_rs (get f :keyword)))
                               :else
                               (throw (Exception. "select 语句错误！"))) (throw (Exception. "select 语句错误！"))) lst_rs)))]
        (select-to-sql ignite group_id ast)))

; sql to myAst
(defn get_my_ast [ignite group_id lst-sql]
    (when-let [ast (get-my-sql-to-ast lst-sql)]
        (when-let [func_ast (find-table-func ignite ast)]
            (get_query_table ignite group_id func_ast))))

(defn get_my_ast_lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector sql_lst]
    (if-not (empty? sql_lst)
        (when-let [ast (get-my-sql-to-ast sql_lst)]
            (when-let [func_ast (find-table-func ignite ast)]
                (get_query_table ignite group_id func_ast)))
        (throw (Exception. "查询字符串不能为空！"))
        ))

(defn get_update_delete_ast [ignite group_id sql]
    (if-not (Strings/isNullOrEmpty sql)
        (when-let [ast (get-my-sql-to-ast (my-lexical/to-back sql))]
            (get_query_table ignite group_id ast))))

; 输入 sql 获取处理后的 sql
; 用于 update 和 delete
(defn my_update_delete_sql [^Ignite ignite ^Long group_id ^String sql]
    (if-let [ast (get_update_delete_ast ignite group_id sql)]
        (ast_to_sql ignite group_id ast)
        (throw (Exception. (format "查询字符串 %s 错误！" sql)))))

; 输入 sql 获取处理后的 sql
(defn my_plus_sql [^Ignite ignite ^Long group_id lst-sql]
    (if-let [ast (get_my_ast ignite group_id lst-sql)]
        (ast_to_sql ignite group_id ast)
        (throw (Exception. (format "查询字符串 %s 错误！" (str/join " " lst-sql))))))

(defn my_plus_sql_lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector sql_lst]
    (if-let [ast (get_my_ast_lst ignite group_id sql_lst)]
        (ast_to_sql ignite group_id ast)
        (throw (Exception. (format "查询字符串错误！")))))

; 执行sql
(defn select_run [^Ignite ignite ^Long group_id ^String sql]
    (.getAll (.query (.getOrCreateCache ignite (MyDbUtil/getPublicCfg)) (SqlFieldsQuery. (my_plus_sql ignite group_id sql)))))



























































