(ns org.gridgain.plus.dml.my-select-plus
    (:require
        [org.gridgain.plus.init.smart-func-init :as smart-func-init]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.tools.my-cache :as my-cache]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil MyDbUtil KvSql)
             (cn.plus.model.db MyScenesCache)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (org.tools MyGson)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        :implements [org.gridgain.superservice.ISqlToAst]
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySelectPlus
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [sqlToAst [java.util.ArrayList] Object]
        ;          ^:static [mySqlToAst [String] Object]]
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

(declare sql-to-ast get-my-sql-to-ast my-get-items my-get-items-as ar-to-sql my-array-to-sql ast_to_sql query-to-sql)

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

(defn my-get-items-as
    ([lst] (my-get-items-as lst [] nil [] []))
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
               (my-lexical/is-eq? f "as") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                             (recur r [] nil [] (conj lst stack-lst))
                             (recur r stack mid-small (conj stack-lst f) lst))
               :else
               (recur r stack mid-small (conj stack-lst f) lst)
               )
         (if-not (empty? stack-lst)
             (conj lst stack-lst)
             lst))))

(defn get-case-when
    ([items] (get-case-when items [] [] [] []))
    ([[f & r] stack-when stack-then stack-else lst]
     (if (some? f)
         (cond (my-lexical/is-eq? f "when") (cond (empty? stack-then) (recur r (conj stack-when f) stack-then stack-else lst)
                                                  (and (not (empty? stack-when)) (not (empty? stack-then))) (recur r [f] [] stack-else (conj lst {:when stack-when :then stack-then}))
                                                  :else
                                                  (throw (Exception. "case when 语法错误！")))
               (my-lexical/is-eq? f "then") (if (not (empty? stack-when))
                                                (recur r stack-when (conj stack-then f) stack-else lst))
               (my-lexical/is-eq? f "else") (if (and (not (empty? stack-when)) (not (empty? stack-then)))
                                                (recur r [] [] (conj stack-else f) (conj lst {:when stack-when :then stack-then})))
               :else
               (if (not (my-lexical/is-eq? f "end"))
                   (cond (and (not (empty? stack-when)) (empty? stack-then)) (recur r (conj stack-when f) stack-then stack-else lst)
                         (and (not (empty? stack-when)) (not (empty? stack-then))) (recur r stack-when (conj stack-then f) stack-else lst)
                         (not (empty? stack-else)) (recur r stack-when stack-then (conj stack-else f) lst))
                   (if (nil? r)
                       (cond (and (not (empty? stack-when)) (not (empty? stack-then))) (recur nil [] [] [] (conj lst {:when stack-when :then stack-then}))
                             (not (empty? stack-else)) (recur nil [] [] [] (conj lst {:else stack-else}))
                             )
                       (throw (Exception. "case when 语法错误！"))))
               )
         lst)))

(defn my-func-link
    ([lst] (my-func-link lst [] nil [] []))
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
               (= f ".") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                             (recur r [] nil [] (conj lst stack-lst "."))
                             (recur r stack mid-small (conj stack-lst f) lst))
               :else
               (recur r stack mid-small (conj stack-lst f) lst)
               )
         (if-not (empty? stack-lst)
             (conj lst stack-lst)
             lst))))

(defn sql-to-ast [^clojure.lang.LazySeq sql-lst]
    (letfn [(get-items
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
                      (and (= (first vs) "{") (= (last vs) "}")) (get-item-tokens vs) ;{:map-obj (get-item-tokens (my-lexical/get-contain-lst vs))}
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
            (smart-item-tokens [lst]
                (cond (and (= (first lst) "[") (= (last lst) "]")) (get-item-tokens lst)
                      (and (= (first lst) "{") (= (last lst) "}")) (get-item-tokens lst)
                      ))
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
                     (cond (and (contains? #{">=" "<=" "<>" ">" "<" "=" "!=" "like" "in" "exists"} (str/lower-case f)) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
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
                (if (and (some? f) (not (= f "-")))
                    (let [m (is-operate-fn? rs)]
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
                                    (contains? #{">=" "<=" "<>" ">" "<" "=" "!=" "like"} line) {:comparison_symbol line}
                                    (contains? #{"and" "or" "between"} (str/lower-case line)) {:and_or_symbol (str/lower-case line)}
                                    (contains? #{"in" "not in"} (str/lower-case line)) {:in_symbol (str/lower-case line)}
                                    (contains? #{"exists" "not exists"} (str/lower-case line)) {:exists_symbol (str/lower-case line)}
                                    (contains? #{","} line) {:comma_symbol line}
                                    (some? (re-find #"^(?i)\d+$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Integer :const true}
                                    (some? (re-find #"^(?i)\d+\.\d$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Double :const true}
                                    ;(some? (re-find #"^(?i)\"\w*\"$|^(?i)'\w*'$|^(?i)\"\W*\"$|^(?i)'\W*'$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
                                    (some? (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$|^\'\'$|^\"\"$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
                                    (some? (re-find #"^(?i)\d+D$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Double :const true}
                                    (some? (re-find #"^(?i)\d+L$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Long :const true}
                                    (some? (re-find #"^(?i)\d+F$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Double :const true}
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
                        (element-item line)
                        ;(cond
                        ;    ; 如果是 m.name 这种形式
                        ;    (some? (re-find #"^(?i)\w+\.\w+$" line)) (if (some? (re-find #"^(?i)\d+\.\d+$|^(?i)\d+\.\d+[DFL]$" line))
                        ;                                                 (element-item line)
                        ;                                                 (get-item-alias line))
                        ;    :else
                        ;    (element-item line)
                        ;    )
                        )))
            ; 处理四则运算
            ; 例如：a + b * (c - d)
            (operation [lst]
                (if (= (first lst) "-")
                    (if-let [m (arithmetic-fn (concat ["0" "-"] lst))]
                        (if (> (count m) 1) {:operation (map get-token m)}))
                    (if-let [m (arithmetic-fn lst)]
                        (if (> (count m) 1) {:operation (map get-token m)})))
                ;(when-let [m (arithmetic-fn lst)]
                ;    (if (> (count m) 1) {:operation (map get-token m)}))
                )

            (parenthesis-case-when [items]
                (if (and (my-lexical/is-eq? (first items) "case") (my-lexical/is-eq? (second items) "when") (my-lexical/is-eq? (last items) "end"))
                    (loop [[f & r] (get-case-when (rest items)) lst-rs []]
                        (if (some? f)
                            (cond (contains? f :when) (recur r (conj lst-rs {:when (get-token (rest (-> f :when))) :then (get-token (rest (-> f :then)))}))
                                  (contains? f :else) (recur r (conj lst-rs {:else (get-token (rest (-> f :else)))}))
                                  )
                            {:case-when lst-rs}))))
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
                                (let [case-when (parenthesis-case-when m)]
                                    (if-not (nil? case-when)
                                        {:parenthesis case-when}
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
                                                                                (let [em-m (eliminate-parentheses m)]
                                                                                    (if-not (nil? em-m)
                                                                                        em-m
                                                                                        (if (= (first m) "-")
                                                                                            (let [pc-m (get-token (concat ["0"] m))]
                                                                                                (if-not (nil? pc-m)
                                                                                                    {:parenthesis [pc-m]}))))
                                                                                    )))))))))))))))

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

            (link-func [lst]
                (let [func-lst (my-func-link lst)]
                    (if (>= (count func-lst) 2)
                        (letfn [(is-link-func? [lst]
                                    (loop [[f & r] (filter odd? (range (count lst))) flag []]
                                        (if (some? f)
                                            (recur r (conj flag (nth lst f)))
                                            (if (empty? (filter #(not (= % ".")) flag))
                                                true false))))]
                            (if (is-link-func? func-lst)
                                (loop [[f & r] func-lst rs []]
                                    (if (some? f)
                                        (if (string? f)
                                            (recur r rs)
                                            (recur r (conj rs (get-token f)))
                                            )
                                        {:func-link rs})))))))
            ; 获取 token ast
            (get-token
                [lst]
                (if (some? lst)
                    (if (string? lst)
                        (get-token-line lst)
                        (if (and (= (count lst) 1) (string? (first lst)))
                            (get-token-line (first lst))
                            (if (and (= (count lst) 3) (= (second lst) "."))
                                (if (and (re-find #"^(?i)\d+$" (first lst)) (re-find #"^(?i)\d+$" (last lst)))
                                    {:table_alias "" :item_name (str (first lst) "." (last lst)) :item_type "" :java_item_type Double :const true}
                                    {:item_name (last lst) :table_alias (first lst) :const false})
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
                                                                            (if (= (first lst) "-")
                                                                                (let [fu-m-1 (get-token (rest lst))]
                                                                                    {:parenthesis (conj [{:table_alias "", :item_name "0", :item_type "", :java_item_type java.lang.Integer, :const true} {:operation_symbol "-"}] fu-m-1)})
                                                                                (if (my-lexical/is-eq? (first lst) "distinct")
                                                                                    [{:keyword "distinct"} (get-token (rest lst))]
                                                                                    (if-let [ds-m (link-func lst)]
                                                                                        ds-m
                                                                                        (smart-item-tokens lst))))
                                                                            )))))))
                                                )))))
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
                                ;(= (first rs) ")") {:item-lst (reverse rs) :alias f}
                                (and (my-lexical/is-word? f) (my-lexical/is-word? (first rs)) (not (my-lexical/is-word? (second rs))) (> (count rs) 2)) {:item-lst (reverse rs) :alias f}
                                ;(and (not (my-lexical/is-word? (second rs))) (> (count rs) 2)) {:item-lst (reverse rs) :alias f}
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
                                          (cond (= (count tables) 1)(concat [{:schema_name "" :table_name (first tables) :table_alias ""}] (table-join rs))
                                                (= (count tables) 2) (concat [{:schema_name "" :table_name (first tables) :table_alias (last tables)}] (table-join rs))
                                                (and (= (count tables) 3) (my-lexical/is-eq? (nth tables 1) "as")) (concat [{:schema_name "" :table_name (first tables) :table_alias (last tables)}] (table-join rs))
                                                (and (= (count tables) 3) (= (nth tables 1) ".")) (concat [{:schema_name (first tables) :table_name (last tables) :table_alias ""}] (table-join rs))
                                                (and (= (count tables) 4) (= (nth tables 1) ".")) (concat [{:schema_name (first tables) :table_name (nth tables 2) :table_alias (last tables)}] (table-join rs))
                                                (and (= (count tables) 5) (= (nth tables 1) ".") (my-lexical/is-eq? (nth tables 3) "as")) (concat [{:schema_name (first tables) :table_name (nth tables 2) :table_alias (last tables)}] (table-join rs))
                                                :else
                                                (throw (Exception. "sql 语句错误！from 关键词之后"))
                                                ))
                                      (contains? f :join) (concat [{:join (-> f :join)}] (table-join rs))
                                      (contains? f :on) (cons {:on (get-token (get f :on))} (table-join rs))
                                      )))
                        ; 处理 table-items
                        (get-table
                            ([lst] (when-let [m (get-table (reverse lst) [] [])]
                                       (if (> (count m) 1) (reverse m) m)))
                            ([[f & rs] stack lst]
                             (if (some? f)
                                 (cond (and (my-lexical/is-eq? f "on") (= (count stack) 0)) (if (> (count lst) 0) (concat [{:on (reverse lst)}] (get-table rs stack [])) (get-table rs stack []))
                                       (and (my-lexical/is-eq? f "join") (contains? #{"left" "inner" "right" "outer" "cross"} (str/lower-case (first rs))) (= (count stack) 0)) (if (> (count lst) 0) (concat [{:tables (reverse lst)}] [{:join (str/join [(first rs) " " f])}] (get-table (rest rs) stack [])) (get-table (rest rs) stack []))
                                       (and (my-lexical/is-eq? f "join") (not (contains? #{"left" "inner" "right" "outer" "cross"} (str/lower-case (first rs)))) (= (count stack) 0)) (if (> (count lst) 0) (concat [{:tables (reverse lst)}] [{:join f}] (get-table rs stack [])) (get-table rs stack []))
                                       (= f ")") (get-table rs (conj stack f) (conj lst f))
                                       (= f "(") (get-table rs (pop stack) (conj lst f))
                                       :else
                                       (get-table rs stack (conj lst f))
                                       )
                                 (if (> (count lst) 0) [{:tables (reverse lst)}]))))
                        (my-not-contains
                            ([lst item-name] (my-not-contains lst item-name []))
                            ([[f & r] item-name stack]
                             (if (some? f)
                                 (cond (= f "(") (recur r item-name (conj stack f))
                                       (= f ")") (if (> (count stack) 0)
                                                     (recur r item-name (pop stack))
                                                     (throw (Exception. "语句 from 后的语句错误！")))
                                       :else
                                       (if (and (my-lexical/is-eq? item-name f) (empty? stack))
                                           true
                                           (recur r item-name stack))
                                       )
                                 false)))
                        (my-table-select-split
                            ([lst] (if (= (first lst) "(")
                                       (my-table-select-split lst [] [] [])))
                            ([[f & r] stack lst lst-rs]
                             (if (some? f)
                                 (cond (= f "(") (recur r (conj stack f) (conj lst f) lst-rs)
                                       (= f ")") (cond (= (count stack) 1) (recur r (pop stack) [] (conj lst-rs (conj lst f)))
                                                       (> (count stack) 1) (recur r (pop stack) (conj lst f) lst-rs)
                                                       :else (throw (Exception. "语句 from 后的语句错误！")))
                                       :else
                                       (recur r stack (conj lst f) lst-rs)
                                       )
                                 (if (not (empty? lst))
                                     (conj lst-rs lst)
                                     lst-rs))))
                        (pre-to-table-item [lst]
                            (loop [[f & r] lst lst-rs [] indexs-stack [] indexs [] lst-index []]
                                (if (some? f)
                                    (cond (and (empty? lst-rs) (empty? lst-index)) (recur r (conj lst-rs f) indexs-stack indexs lst-index)
                                          (and (not (empty? lst-rs)) (my-lexical/is-eq? "USE" f) (not (nil? r)) (my-lexical/is-eq? "INDEX" (first r))) (recur (rest r) lst-rs indexs-stack indexs (conj lst-index "USE INDEX "))
                                          (and (not (empty? lst-rs)) (not (empty? lst-index)) (= (count lst-index) 1) (= f "(") (empty? indexs-stack)) (recur r lst-rs (conj indexs-stack f) (conj indexs f) lst-index)
                                          (and (not (empty? indexs-stack)) (not (= f ")"))) (recur r lst-rs indexs-stack (conj indexs f) lst-index)
                                          (and (not (empty? indexs-stack)) (= f ")")) (if (= (count indexs-stack) 1)
                                                                                          (recur r lst-rs [] [] (concat lst-index (conj indexs f)))
                                                                                          (recur r lst-rs (pop indexs-stack) (conj indexs f) lst-index))
                                          :else
                                          (recur r (conj lst-rs f) indexs-stack indexs lst-index)
                                          )
                                    [lst-rs lst-index])))
                        (to-table-item [my-lst]
                            (let [[lst lst-hint] (pre-to-table-item my-lst)]
                                (cond (= (count lst) 1) {:schema_name "" :table_name (first lst) :table_alias "" :hints (str/join lst-hint)}
                                      (= (count lst) 2) {:schema_name "" :table_name (first lst) :table_alias (last lst) :hints (str/join lst-hint)}
                                      (= (count lst) 3) (cond (my-lexical/is-eq? (second lst) "as") {:schema_name "" :table_name (first lst) :table_alias (last lst) :hints (str/join lst-hint)}
                                                              (= (second lst) ".") {:schema_name (first lst) :table_name (last lst) :table_alias "" :hints (str/join lst-hint)})
                                      (and (= (count lst) 4) (= (second lst) ".")) {:schema_name (first lst) :table_name (nth lst 2) :table_alias (last lst) :hints (str/join lst-hint)}
                                      (and (= (count lst) 5) (my-lexical/is-eq? (nth lst 3) "as")) {:schema_name (first lst) :table_name (nth lst 2) :table_alias (last lst) :hints (str/join lst-hint)}
                                      :else
                                      (if-let [lst-m (my-get-items-as lst)]
                                          (if (and (= (count lst-m) 2) (is-select? (first lst-m)))
                                              {:parenthesis (get-my-sql-to-ast (get-select-line (first lst-m))) :table_alias (first (last lst-m))}
                                              (if-let [lst-m-1 (my-table-select-split lst)]
                                                  (if (and (= (count lst-m-1) 2) (is-select? (first lst-m-1)))
                                                      {:parenthesis (get-my-sql-to-ast (get-select-line (first lst-m-1))) :table_alias (first (last lst-m-1))}
                                                      (get-query-items lst))))
                                          (get-query-items lst))
                                      )))
                        ; 处理逗号类型的
                        (table-comma
                            [lst]
                            (if (and (string? lst) (my-lexical/is-eq? lst ","))
                                (get-token-line lst)
                                (to-table-item lst)))
                        ]
                    (if (= (count table-items) 1)
                        (let [m (first table-items)]
                            (cond (string? m) (concat [{:schema_name "", :table_name m, :table_alias "" :hints nil}])
                                  (and (my-lexical/is-seq? m) (is-select? m)) {:parenthesis (get-my-sql-to-ast (get-select-line m))}
                                  :else
                                  (if (my-not-contains (first table-items) "join")
                                      (table-join (get-table (first table-items)))
                                      (map table-comma table-items))))
                        (map table-comma table-items)))
                )
            ; 获取 order by
            (get-order-by
                ([lst] (let [m (reverse lst)] (get-order-by (reverse (rest m)) (first m))))
                ([f l]
                 (if (and (some? f) (some? l))
                     {:order-item (map get-token f) :order l})))

            (to-order-obj [m]
                (if (contains? #{"desc" "asc"} (str/lower-case (last m)))
                    (if-let [item-obj (get-token (drop-last 1 m))]
                        (if (map? item-obj)
                            {:order-item item-obj :order (last m)}
                            (throw (Exception. "order by 语法错误！")))
                        (throw (Exception. "order by 语法错误！")))
                    (if-let [item-obj (get-token m)]
                        (if (map? item-obj)
                            {:order-item item-obj :order "asc"}
                            (throw (Exception. "order by 语法错误！")))
                        (throw (Exception. "order by 语法错误！")))))

            (my-get-order-by [lst]
                (if-let [m (get-items lst)]
                    (loop [[f & r] m lst []]
                        (if (some? f)
                            (if (some? r)
                                (recur r (concat lst [(to-order-obj f) {:comma_symbol ","}]))
                                (recur r (concat lst [(to-order-obj f)]))
                                )
                            lst))))

            (get-group-by-tokens [group-by]
                (if-let [tokens (get-items group-by)]
                    (if (= (count tokens) 1)
                        (get-token (first tokens))
                        (loop [[f & r] tokens lst []]
                            (if (some? f)
                                (if (some? r)
                                    (recur r (concat lst [(get-token f) {:comma_symbol ","}]))
                                    (recur r (concat lst [(get-token f)])))
                                lst))
                        )))
            ; 获取 limit
            (get-limit [lst]
                (let [m (my-comma-fn lst)]
                    (if (= (count m) 3) (map get-token m))))
            ;(my-get-limit [lst]
            ;    (if (my-lexical/is-eq? (first lst) "limit")
            ;        (if-let [m (get-items (rest lst))]
            ;            (if (= (count m) 2)
            ;                [(get-token (first m)) {:comma_symbol ","} (get-token (last m))]))))
            (my-get-limit [lst]
                (if-let [m (get-items lst)]
                    (if (= (count m) 2)
                        [(get-token (first m)) {:comma_symbol ","} (get-token (last m))])))
            ; query items 处理
            (get-query-items [lst]
                (when-let [[f & rs] (pre-query-lst lst)]
                    (if (map? f) (concat [f] (get-query-items rs))
                                 (when-let [{item-lst :item-lst alias :alias} (get-item-lst f)]
                                     (if (and (my-lexical/is-seq? item-lst) (my-lexical/is-eq? (first item-lst) "case"))
                                         (loop [[f & r] (get-case-when (rest item-lst)) lst-rs []]
                                             (if (some? f)
                                                 (cond (contains? f :when) (recur r (conj lst-rs {:when (get-token (rest (-> f :when))) :then (get-token (rest (-> f :then)))}))
                                                       (contains? f :else) (recur r (conj lst-rs {:else (get-token (rest (-> f :else)))}))
                                                       )
                                                 (if-not (Strings/isNullOrEmpty alias)
                                                     (concat [(assoc {:case-when lst-rs} :alias alias)] (get-query-items rs))
                                                     (concat [(assoc {:case-when lst-rs} :alias (format "\"%s\"" (-> (query-to-sql nil {:case-when lst-rs}) :sql)))] (get-query-items rs)))))
                                         (if-let [tk (get-token item-lst)]
                                             (cond (contains? tk :item_name) (if-not (Strings/isNullOrEmpty alias)
                                                                                 (concat [(assoc tk :alias alias)] (get-query-items rs))
                                                                                 ;(concat [(assoc tk :alias (-> tk :item_name))] (get-query-items rs))
                                                                                 (concat [tk] (get-query-items rs))
                                                                                 )
                                                   (or (contains? tk :comma_symbol) (contains? tk :and_or_symbol) (contains? tk :keyword) (contains? tk :comparison_symbol) (contains? tk :in_symbol) (contains? tk :operation_symbol) (contains? tk :join) (contains? tk :on) (contains? tk :comma_symbol))  (concat [tk] (get-query-items rs))
                                                   :else (if-not (Strings/isNullOrEmpty alias)
                                                             (concat [(assoc tk :alias alias)] (get-query-items rs))
                                                             (concat [(assoc tk :alias (format "\"%s\"" (-> (query-to-sql nil tk) :sql)))] (get-query-items rs)))
                                                   )
                                             ))
                                     ))))
            (sql-to-ast-single [sql-lst]
                (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} (my-lexical/get-segments-list sql-lst)]
                    {:query-items (get-query-items query-items) :table-items (get-table-items table-items) :where-items (get-token where-items) :group-by (get-group-by-tokens group-by) :having (get-token having) :order-by (my-get-order-by order-by) :limit (my-get-limit limit)}))
            (to-ast [lst]
                (if (string? lst) {:keyword lst}
                                  {:sql_obj (sql-to-ast-single lst)}))]
        (if-not (my-lexical/is-eq? (first sql-lst) "select")
            (get-token sql-lst)
            (when-let [m (my-lexical/sql-union sql-lst)]
                (map to-ast m)))))

(defn to-lst [ast]
    (cond (map? ast) (loop [[f & r] (keys ast) my-ast (Hashtable.)]
                         (if (some? f)
                             (cond (map? (get ast f)) (if-let [m (to-lst (get ast f))]
                                                          (recur r (doto my-ast (.put (str/join (rest (str f))) m)))
                                                          (recur r (doto my-ast (.put (str/join (rest (str f))) ""))))
                                   (my-lexical/is-seq? (get ast f)) (if-let [m (to-lst (get ast f))]
                                                                        (recur r (doto my-ast (.put (str/join (rest (str f))) m)))
                                                                        (recur r (doto my-ast (.put (str/join (rest (str f))) ""))))
                                   :else
                                   (if-let [m (get ast f)]
                                       (recur r (doto my-ast (.put (str/join (rest (str f))) m)))
                                       (recur r (doto my-ast (.put (str/join (rest (str f))) ""))))
                                   )
                             my-ast))
          (my-lexical/is-seq? ast) (my-lexical/to_arryList (map to-lst ast))
          ))

(defn my-sql-to-ast [^java.util.List lst]
    (let [ast (sql-to-ast lst)]
        (to-lst ast)))

(defn -sqlToAst [this ^java.util.List lst]
    (let [ast (sql-to-ast lst)]
        (to-lst ast)))

(defn -mySqlToAst [this ^String sql]
    (sql-to-ast (my-lexical/to-back sql)))

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


; PreparedStatement 调用所有方法 参数用 ? 来传递
; 返回函数名 和 参数列表，通过 (apply (eval (read-string func-name)) ignite group_id ps) 来调用
;(defn func-args [^Ignite ignite group_id m]
;    (let [{func-name :func-name lst_ps :lst_ps} m]
;        (if-let [my-lexical-func (my-lexical/smart-func func-name)]
;            my-lexical-func ;{:func-name my-lexical-func :args [(ast_to_sql ignite group_id lst_ps)]}
;            (cond
;                ;(my-lexical/is-eq? "log" func-name) (format "(log %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
;                (my-lexical/is-eq? "println" func-name) "my-lexical/my-show-msg" ; {:func-name "my-lexical/my-show-msg" :args [(my-lexical/gson (ast_to_sql ignite group_id lst_ps))]}
;                (re-find #"\." func-name) (let [{let-name :schema_name method-name :table_name} (my-lexical/get-schema func-name)]
;                                              (if (> (count lst_ps) 0)
;                                                  {:func-name (my-lexical/smart-func method-name) :args [(my-lexical/get-value let-name) (ast_to_sql ignite group_id lst_ps)]}
;                                                  {:func-name (my-lexical/smart-func method-name) :args [(my-lexical/get-value let-name)]})
;                                              )
;                ; 系统函数
;                (contains? #{"first" "rest" "next" "second" "last"} (str/lower-case func-name)) {:func-name (str/lower-case func-name) :args [(ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "query_sql") (cond (= (count lst_ps) 1) {:func-name "my-smart-db/query_sql" :args [ignite group_id (ast_to_sql ignite group_id lst_ps) nil]}
;                                                                (> (count lst_ps) 1) {:func-name "my-smart-db/query_sql" :args [group_id (ast_to_sql ignite group_id (first lst_ps)) [(ast_to_sql ignite group_id (rest lst_ps))]]}
;                                                                )
;                (my-lexical/is-eq? func-name "empty?") {:func-name "empty?" :args [(ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "notEmpty?") {:func-name "my-lexical/not-empty?" :args [(ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-eq? func-name "noSqlCreate") {:func-name "my-lexical/no-sql-create" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlGet") {:func-name "my-lexical/no-sql-get-vs" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-eq? func-name "noSqlInsertTran") {:func-name "my-lexical/no-sql-insert-tran" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlUpdateTran") {:func-name "my-lexical/no-sql-update-tran" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlDeleteTran") {:func-name "my-lexical/no-sql-delete-tran" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlDrop") {:func-name "my-lexical/no-sql-drop" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlInsert") {:func-name "my-lexical/no-sql-insert" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlUpdate") {:func-name "my-lexical/no-sql-update" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "noSqlDelete") {:func-name "my-lexical/no-sql-delete" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "auto_id") {:func-name "my-lexical/auto_id" :args [ignite (ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-eq? func-name "trans") {:func-name "my-smart-db/trans" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "my_view") {:func-name "smart-func/smart-view" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "rm_view") {:func-name "smart-func/rm-smart-view" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-eq? func-name "add_scenes_to") {:func-name "smart-func/add-scenes-to" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "rm_scenes_from") {:func-name "smart-func/rm-scenes-from" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-eq? func-name "add_job") {:func-name "smart-func/add-job" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "remove_job") {:func-name "smart-func/remove-job" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "job_snapshot") {:func-name "smart-func/get-job-snapshot" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-eq? func-name "add_func") {:func-name "smart-func/add_func" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "remove_func") {:func-name "smart-func/remove_func" :args [ignite group_id (ast_to_sql ignite group_id lst_ps)]}
;                (my-lexical/is-eq? func-name "recovery_to_cluster") {:func-name "smart-func/recovery-to-cluster" :args [ignite (ast_to_sql ignite group_id lst_ps)]}
;
;                (my-lexical/is-func? ignite func-name) (if-not (empty? lst_ps)
;                                                {:func-name "my-smart-scenes/my-invoke-func" :args [ignite (format "\"%s\"" func-name) [(ast_to_sql ignite group_id lst_ps)]]}
;                                                {:func-name "my-smart-scenes/my-invoke-func-no-ps" :args [ignite (format "\"%s\"" func-name)]})
;                (my-lexical/is-scenes? ignite group_id func-name) (if-not (empty? lst_ps)
;                                                           {:func-name "my-smart-scenes/my-invoke-scenes" :args [ignite group_id (format "\"%s\"" func-name) [(ast_to_sql ignite group_id lst_ps)]]}
;                                                           {:func-name "my-smart-scenes/my-invoke-scenes-no-ps" :args [ignite group_id (format "\"%s\"" func-name)]})
;                (my-lexical/is-call-scenes? ignite group_id func-name) (if-not (empty? lst_ps)
;                                                                {:func-name "my-smart-scenes/my-invoke-scenes" :args [ignite group_id (format "\"%s\"" func-name) [(ast_to_sql ignite group_id lst_ps)]]}
;                                                                {:func-name "my-smart-scenes/my-invoke-scenes-no-ps" :args [ignite group_id (format "\"%s\"" func-name)]})
;
;                ;(my-lexical/is-eq? func-name "loadCode") (.loadSmartSql (.getLoadSmartSql (MyLoadSmartSqlService/getInstance)) ignite group_id (-> (first lst_ps) :item_name))
;                :else
;                (throw (Exception. (format "%s 不存在，或没有权限！" func-name)))
;                ))
;        ))

; ast to sql
(defn ast_to_sql [ignite group_id ast]
    (letfn [(my-select_to_sql_single [ignite group_id ast]
                (loop [[f & r] ast sb (StringBuilder.)]
                    (if (some? f)
                        (cond (= (first f) :query-items) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                             (recur r (doto sb (.append "select ") (.append tk))))
                              (= (first f) :table-items) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                             (recur r (doto sb (.append " from ") (.append tk))))
                              (and (= (first f) :where-items) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                                                      (recur r (doto sb (.append " where ") (.append tk))))
                              (and (= (first f) :group-by) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                                                   (recur r (doto sb (.append " group by ") (.append tk))))
                              (and (= (first f) :having) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                                                 (recur r (doto sb (.append " having ") (.append tk))))
                              (and (= (first f) :order-by) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                                                   (recur r (doto sb (.append " order by ") (.append tk))))
                              (and (= (first f) :limit) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line ignite group_id (second f))]
                                                                                (recur r (doto sb (.append " limit ") (.append tk))))
                              :else
                              (recur r sb)
                              )
                        (.toString sb))))
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
                        (contains? m :func-link) (func-link-to-line ignite group_id m)
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
                        (contains? m :case-when) (case-when-line ignite group_id m)
                        :else
                        (throw (Exception. "select 语句错误！请仔细检查！"))
                        )))
            (case-when-line [ignite group_id m]
                (loop [[f & r] (-> m :case-when) lst-rs ["case"]]
                    (if (some? f)
                        (cond (contains? f :when) (recur r (concat lst-rs ["when" (lst-token-to-line ignite group_id (-> f :when)) "then" (lst-token-to-line ignite group_id (-> f :then))]))
                              (contains? f :else) (recur r (concat lst-rs ["else" (lst-token-to-line ignite group_id (-> f :else))]))
                              )
                        (concat lst-rs ["end"]))))
            (on-to-line [ignite group_id m]
                (if (some? m)
                    (str/join ["on " (str/join " " (token-to-sql ignite group_id (get m :on)))])))
            (func-to-line [ignite group_id m]
                (cond (contains? smart-func-init/func-smart (str/lower-case (-> m :func-name))) (let [lst-ps-items (map (partial token-to-sql ignite group_id) (-> m :lst_ps))]
                                                                                                (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                                                                                                    (if-not (empty? (-> m :lst_ps))
                                                                                                        (concat ["my_invoke_all(" (format "'%s', '%s'," (-> m :func-name) (MyGson/groupObjToLine group_id))] lst-ps-items [") as " (-> m :alias)])
                                                                                                        (concat ["my_invoke_all(" (format "'%s', '%s'" (-> m :func-name) (MyGson/groupObjToLine group_id))] [") as " (-> m :alias)]))
                                                                                                    (if-not (empty? (-> m :lst_ps))
                                                                                                        (concat ["my_invoke_all(" (format "'%s', '%s'," (-> m :func-name) (MyGson/groupObjToLine group_id))] lst-ps-items [")"])
                                                                                                        (concat ["my_invoke_all(" (format "'%s', '%s'" (-> m :func-name) (MyGson/groupObjToLine group_id))] [")"]))))
                      (contains? smart-func-init/func-set (str/lower-case (-> m :func-name))) (let [lst-ps-items (map (partial token-to-sql ignite group_id) (-> m :lst_ps))]
                                                                                              (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                                                                                                  (if-not (empty? (-> m :lst_ps))
                                                                                                      (concat ["my_invoke_all(" (format "'%s', '%s'," (-> m :func-name) (MyGson/groupObjToLine group_id))] lst-ps-items [") as " (-> m :alias)])
                                                                                                      (concat ["my_invoke_all(" (format "'%s', '%s'" (-> m :func-name) (MyGson/groupObjToLine group_id))] [") as " (-> m :alias)]))
                                                                                                  (if-not (empty? (-> m :lst_ps))
                                                                                                      (concat ["my_invoke_all(" (format "'%s', '%s'," (-> m :func-name) (MyGson/groupObjToLine group_id))] lst-ps-items [")"])
                                                                                                      (concat ["my_invoke_all(" (format "'%s', '%s'" (-> m :func-name) (MyGson/groupObjToLine group_id))] [")"]))))
                      (contains? smart-func-init/db-func-set (str/lower-case (-> m :func-name))) (let [lst-ps-items (map (partial token-to-sql ignite group_id) (-> m :lst_ps))]
                                                                                                 (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                                                                                                     (if-not (empty? (-> m :lst_ps))
                                                                                                         (concat [(format "%s(" (-> m :func-name))] lst-ps-items [") as " (-> m :alias)])
                                                                                                         (concat [(format "%s(" (-> m :func-name))] [") as " (-> m :alias)]))
                                                                                                     (if-not (empty? (-> m :lst_ps))
                                                                                                         (concat [(format "%s(" (-> m :func-name))] lst-ps-items [")"])
                                                                                                         (concat [(format "%s(" (-> m :func-name))] [")"]))))
                      (contains? smart-func-init/db-func-no-set (str/lower-case (-> m :func-name))) (throw (Exception. (format "不存在方法 %s !" (-> m :func-name))))
                      (my-cache/is-func? ignite (str/lower-case (-> m :func-name))) (let [lst-ps-items (map (partial token-to-sql ignite group_id) (-> m :lst_ps))]
                                                                                        (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                                                                                            (if-not (empty? (-> m :lst_ps))
                                                                                                (concat ["my_fun(" (format "'%s'," (-> m :func-name))] lst-ps-items [")" " as "] [(-> m :alias)])
                                                                                                (concat ["my_fun(" (format "'%s'" (-> m :func-name))] [")" " as "] [(-> m :alias)]))
                                                                                            (if-not (empty? (-> m :lst_ps))
                                                                                                (concat ["my_fun(" (format "'%s'," (-> m :func-name))] lst-ps-items [")"])
                                                                                                (concat ["my_fun(" (format "'%s'" (-> m :func-name))] [")"]))))
                      (my-cache/is-scenes? ignite group_id (str/lower-case (-> m :func-name))) (let [lst-ps-items (map (partial token-to-sql ignite group_id) (-> m :lst_ps))]
                                                                                                   (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                                                                                                       (if-not (empty? (-> m :lst_ps))
                                                                                                           (concat ["my_invoke(" (format "'%s', '%s'," (-> m :func-name) (MyGson/groupObjToLine group_id))] lst-ps-items [") as " (-> m :alias)])
                                                                                                           (concat ["my_invoke(" (format "'%s', '%s'" (-> m :func-name) (MyGson/groupObjToLine group_id))] [") as " (-> m :alias)]))
                                                                                                       (if-not (empty? (-> m :lst_ps))
                                                                                                           (concat ["my_invoke(" (format "'%s', '%s'," (-> m :func-name) (MyGson/groupObjToLine group_id))] lst-ps-items [")"])
                                                                                                           (concat ["my_invoke(" (format "'%s', '%s'" (-> m :func-name) (MyGson/groupObjToLine group_id))] [")"]))))
                      :else
                      (throw (Exception. (format "不存在方法 %s !" (-> m :func-name))))
                      ))
            (func-link-to-line [ignite group_id m]
                (let [{sql :sql args :args} (my-lexical/my-func-line-code m)]
                    (loop [[f & r] args lst-ps [(MyGson/groupObjToLine group_id)] lst-args []]
                        (if (some? f)
                            (recur r (conj lst-ps f) lst-args)
                            (str/join ["my_invoke_link('" sql "'," (str/join "," lst-ps) ")"])))))
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
            ;                (let [schema_name (get_schema_name ignite group_id)]
            ;                    (if (Strings/isNullOrEmpty schema_name)
            ;                        table_name
            ;                        (str/join [schema_name "." table_name])))
            ;                (let [schema_name (get_schema_name ignite group_id)]
            ;                    (if (Strings/isNullOrEmpty schema_name)
            ;                        (str/join [table_name " " table_alias])
            ;                        (str/join [(str/join [schema_name "." table_name]) " " table_alias])))
            ;                ))))
            (table-to-line [ignite group_id m]
                (if (some? m)
                    (if-let [{schema_name :schema_name table_name :table_name table_alias :table_alias hints :hints} m]
                        (if-not (Strings/isNullOrEmpty schema_name)
                            (if (Strings/isNullOrEmpty table_alias)
                                (if (Strings/isNullOrEmpty hints)
                                    (format "%s.%s" schema_name table_name)
                                    (format "%s.%s %s" schema_name table_name hints))
                                (if (Strings/isNullOrEmpty hints)
                                    (str/join [(format "%s.%s" schema_name table_name) " " table_alias])
                                    (str/join [(format "%s.%s %s" schema_name table_name hints) " " table_alias])))
                            (if (= (first group_id) 0)
                                (if (Strings/isNullOrEmpty table_alias)
                                    (if (Strings/isNullOrEmpty hints)
                                        (format "MY_META.%s" table_name)
                                        (format "MY_META.%s %s" table_name hints))
                                    (if (Strings/isNullOrEmpty hints)
                                        (str/join [(format "MY_META.%s" table_name) " " table_alias])
                                        (str/join [(format "MY_META.%s %s" table_name hints) " " table_alias])))
                                (let [schema_name (get_schema_name ignite group_id)]
                                    (if (Strings/isNullOrEmpty table_alias)
                                        (if (Strings/isNullOrEmpty hints)
                                            (format "%s.%s" schema_name table_name)
                                            (format "%s.%s %s" schema_name table_name hints))
                                        (if (Strings/isNullOrEmpty hints)
                                            (str/join [(format "%s.%s" schema_name table_name) " " table_alias])
                                            (str/join [(format "%s.%s %s" schema_name table_name hints) " " table_alias]))
                                        ))))
                        )))
            ; 获取 data_set 的名字和对应的表
            (get_schema_name [^Ignite ignite group_id]
                (second group_id))
            (select-to-sql
                ([ignite group_id ast] (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (.toString (ar-to-sql (select-to-sql ignite group_id ast []) (StringBuilder.)))
                                             (contains? ast :sql_obj) (my-select_to_sql_single ignite group_id (get ast :sql_obj))
                                             :else
                                             (token-to-sql ignite group_id ast)
                                             ;(throw (Exception. "select 语句错误！"))
                                             ))
                ([ignite group_id [f & rs] lst_rs]
                 (if (some? f)
                     (if (map? f)
                         (cond (contains? f :sql_obj) (recur ignite group_id rs (conj lst_rs (my-select_to_sql_single ignite group_id (get f :sql_obj))))
                               (contains? f :keyword) (recur ignite group_id rs (conj lst_rs (get f :keyword)))
                               :else
                               (throw (Exception. "select 语句错误！")))
                         (throw (Exception. "select 语句错误！"))) lst_rs)))]
        (select-to-sql ignite group_id ast)))

(defn query-to-sql [dic-args ast]
    (letfn [(my-select_to_sql_single [dic-args ast]
                (loop [[f & r] ast sb (StringBuilder.) args []]
                    (if (some? f)
                        (cond (= (first f) :query-items) (let [tk (lst-token-to-line dic-args (second f))]
                                                             (recur r (doto sb (.append "select ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              (= (first f) :table-items) (let [tk (lst-token-to-line dic-args (second f))]
                                                             (recur r (doto sb (.append " from ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              (and (= (first f) :where-items) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line dic-args (second f))]
                                                                                                                (recur r (doto sb (.append " where ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              (and (= (first f) :group-by) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line dic-args (second f))]
                                                                                                             (recur r (doto sb (.append " group by ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              (and (= (first f) :having) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line dic-args (second f))]
                                                                                                           (recur r (doto sb (.append " having ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              (and (= (first f) :order-by) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line dic-args (second f))]
                                                                                                             (recur r (doto sb (.append " order by ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              (and (= (first f) :limit) (some? (second f)) (not (empty? (second f)))) (let [tk (lst-token-to-line dic-args (second f))]
                                                                                                          (recur r (doto sb (.append " limit ") (.append (-> tk :sql))) (concat args (filter #(not (nil? %)) (-> tk :args)))))
                              :else
                              (recur r sb args)
                              )
                        {:sql (.toString sb) :args args})))
            (get-map-token-to-sql [m]
                (loop [[f & r] m lst-sql [] lst-args []]
                    (if (some? f)
                        (let [{sql :sql args :args} f]
                            (recur r (concat lst-sql [sql]) (concat lst-args args))
                            )
                        {:sql lst-sql :args lst-args})))
            (lst-token-to-line
                ([dic-args lst_token] (cond (string? lst_token) lst_token
                                                            (map? lst_token) (let [{sql :sql args :args} (token-to-sql dic-args lst_token)]
                                                                                 {:sql (my-array-to-sql sql) :args args})
                                                            :else
                                                            (let [{sql :sql args :args} (lst-token-to-line dic-args lst_token [] [])]
                                                                {:sql (my-array-to-sql sql) :args args})
                                                            ))
                ([dic-args [f & rs] lst lst-args]
                 (if (some? f)
                     (let [{sql :sql args :args} (token-to-sql dic-args f)]
                         (recur dic-args rs (conj lst (my-array-to-sql sql)) (concat lst-args args)))
                     {:sql lst :args lst-args})))
            (token-to-sql [dic-args m]
                (if (some? m)
                    (cond (my-lexical/is-seq? m) (get-map-token-to-sql (map (partial token-to-sql dic-args) m))
                          (map? m) (map-token-to-sql dic-args m))))
            (map-token-to-sql
                [dic-args m]
                (if (some? m)
                    (cond
                        (contains? m :sql_obj) (select-to-sql dic-args m)
                        (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-line dic-args m)
                        (contains? m :func-link) (func-link-to-line dic-args m)
                        (contains? m :and_or_symbol) {:sql (get m :and_or_symbol) :args nil} ;(get m :and_or_symbol)
                        (contains? m :keyword) {:sql (get m :keyword) :args nil} ;(get m :keyword)
                        (contains? m :operation) (get-map-token-to-sql (map (partial token-to-sql dic-args) (get m :operation)))
                        (contains? m :comparison_symbol) {:sql (get m :comparison_symbol) :args nil} ; (get m :comparison_symbol)
                        (contains? m :in_symbol) {:sql (get m :in_symbol) :args nil} ; (get m :in_symbol)
                        (contains? m :operation_symbol) {:sql (get m :operation_symbol) :args nil} ; (get m :operation_symbol)
                        (contains? m :join) {:sql (get m :join) :args nil} ;(get m :join)
                        (contains? m :on) (on-to-line dic-args m)
                        (contains? m :comma_symbol) {:sql (get m :comma_symbol) :args nil} ;(get m :comma_symbol)
                        (contains? m :order-item) (let [{sql :sql args :args} (token-to-sql dic-args (-> m :order-item))]
                                                      (if (string? sql)
                                                          {:sql (format "%s %s" sql (-> m :order)) :args args}
                                                          {:sql (format "%s %s" (str/join sql) (-> m :order)) :args args}))
                        (contains? m :item_name) (item-to-line dic-args m)
                        (contains? m :table_name) (table-to-line dic-args m)
                        (contains? m :exists) (let [{sql :sql args :args} (token-to-sql dic-args (get (get m :select_sql) :parenthesis))]
                                                  {:sql (concat [(get m :exists) "("] sql [")"]) :args args})
                        (contains? m :parenthesis) (let [{sql :sql args :args} (token-to-sql dic-args (get m :parenthesis))]
                                                       (if (contains? m :alias)
                                                           {:sql (concat ["("] sql [")" " " (-> m :alias)]) :args args}
                                                           {:sql (concat ["("] sql [")"]) :args args}))
                        (contains? m :case-when) (case-when-line dic-args m)
                        :else
                        (throw (Exception. "select 语句错误！请仔细检查！"))
                        )))
            (case-when-line [dic-args m]
                (loop [[f & r] (-> m :case-when) lst-rs ["case"] args []]
                    (if (some? f)
                        (cond (contains? f :when) (let [{when-sql :sql when-agrs :args} (lst-token-to-line dic-args (-> f :when)) {then-sql :sql then-agrs :args} (lst-token-to-line dic-args (-> f :then))]
                                                      (recur r (concat lst-rs ["when" when-sql "then" then-sql]) (concat args when-agrs then-agrs)))
                              (contains? f :else) (let [{else-sql :sql else-agrs :args} (lst-token-to-line dic-args (-> f :else))]
                                                      (recur r (concat lst-rs ["else" else-sql]) else-agrs))
                              )
                        {:sql (concat lst-rs ["end"]) :args (filter #(not (nil? %)) args)})))
            (on-to-line [dic-args m]
                (if (some? m)
                    (let [{sql :sql args :args} (token-to-sql dic-args (get m :on))]
                        {:sql (str/join ["on " (str/join " " sql)]) :args args})
                    ))
            (func-to-line [dic-args m]
                (let [{sql :sql args :args} (get-map-token-to-sql (map (partial token-to-sql dic-args) (-> m :lst_ps)))]
                    (if (and (contains? m :alias) (not (Strings/isNullOrEmpty (-> m :alias))))
                        (if-not (empty? (-> m :lst_ps))
                            {:sql (concat [(format "%s(" (-> m :func-name))] sql [") as " (-> m :alias)]) :args args}
                            {:sql (concat [(format "%s(" (-> m :func-name))] [") as " (-> m :alias)]) :args args})
                        (if-not (empty? (-> m :lst_ps))
                            {:sql (concat [(format "%s(" (-> m :func-name))] sql [")"]) :args args}
                            {:sql (concat [(format "%s(" (-> m :func-name))] [")"]) :args args}))))
            (func-link-to-line [dic-args m]
                (my-lexical/my-func-line-code m)
                ;(let [{sql :sql args :args} (my-lexical/my-func-line-code m)]
                ;    (loop [[f & r] args lst-ps [(MyGson/groupObjToLine group_id)] lst-args []]
                ;        (if (some? f)
                ;            (if (contains? (-> dic-args :dic) f)
                ;                (recur r (conj lst-ps "?") (conj lst-args (first (get (-> dic-args :dic) f))))
                ;                (recur r (conj lst-ps f) lst-args))
                ;            {:sql (str/join ["my_invoke_link('" sql "'," (str/join "," lst-ps) ")"]) :args lst-args})))
                )
            (item-to-line [dic-args m]
                (let [{table_alias :table_alias item_name :item_name alias :alias} m]
                    (cond
                        (and (not (Strings/isNullOrEmpty table_alias)) (not (nil? alias)) (not (Strings/isNullOrEmpty alias))) {:sql (str/join [table_alias "." item_name " as " alias]) :args nil}
                        (and (not (Strings/isNullOrEmpty table_alias)) (Strings/isNullOrEmpty alias)) {:sql (str/join [table_alias "." item_name]) :args nil}
                        (and (Strings/isNullOrEmpty table_alias) (Strings/isNullOrEmpty alias)) (if (contains? (-> dic-args :dic) item_name)
                                                                                                    {:sql "?" :args [(first (get (-> dic-args :dic) item_name))]}
                                                                                                    {:sql item_name :args nil})
                        )))
            (table-to-line [dic-args m]
                (if (some? m)
                    (if-let [{schema_name :schema_name table_name :table_name table_alias :table_alias hints :hints} m]
                        (cond (and (not (Strings/isNullOrEmpty schema_name)) (not (Strings/isNullOrEmpty table_alias))) (if (Strings/isNullOrEmpty hints)
                                                                                                                            {:sql (str/join [(format "%s.%s" schema_name table_name) " " table_alias]) :args nil}
                                                                                                                            {:sql (str/join [(format "%s.%s %s" schema_name table_name hints) " " table_alias]) :args nil})
                              (not (Strings/isNullOrEmpty table_alias)) (if (Strings/isNullOrEmpty hints)
                                                                            {:sql (str/join [(format "%s" table_name) " " table_alias]) :args nil}
                                                                            {:sql (str/join [(format "%s %s" table_name hints) " " table_alias]) :args nil}))
                        )))
            ; 获取 data_set 的名字和对应的表
            (get_schema_name [^Long group_id]
                (second group_id))
            (select-to-sql
                ([dic-args ast]
                 (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (let [{sql :sql args :args} (select-to-sql dic-args ast [] [])]
                                                                                  {:sql sql :args args})
                       (contains? ast :sql_obj) (my-select_to_sql_single dic-args (get ast :sql_obj))
                       :else
                       (throw (Exception. "select 语句错误！"))))
                ([dic-args [f & rs] lst_rs lst-args]
                 (if (some? f)
                     (if (map? f)
                         (cond (contains? f :sql_obj) (let [{sql :sql args :args} (my-select_to_sql_single dic-args (get f :sql_obj))]
                                                          (recur dic-args rs (conj lst_rs sql) (filter #(not (nil? %)) (concat lst-args args))))
                               (contains? f :keyword) (recur dic-args rs (conj lst_rs (get f :keyword)) lst-args)
                               :else
                               (throw (Exception. "select 语句错误！"))) (throw (Exception. "select 语句错误！")))
                     {:sql (str/join " " lst_rs) :args (filter #(not (nil? %)) lst-args)})))]
        (lst-token-to-line dic-args ast)))


























































