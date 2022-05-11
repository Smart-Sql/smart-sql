(ns org.gridgain.plus.dml.my-smart-func-args-token-clj
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar MyLetLayer)
             (com.google.common.base Strings)
             (cn.plus.model MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache MyScenesCachePk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartFuncArgsTokenClj
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare is-symbol-priority run-express calculate is-func? is-scenes? func-to-clj item-to-clj token-lst-to-clj
         token-to-clj map-token-to-clj parenthesis-to-clj seq-obj-to-clj map-obj-to-clj smart-func-to-clj)

; 判断符号优先级
; f symbol 的优先级大于等于 s 返回 true 否则返回 false
(defn is-symbol-priority [f s]
    (cond (or (= (-> f :operation_symbol) "*") (= (-> f :operation_symbol) "/")) true
          (and (or (= (-> f :operation_symbol) "+") (= (-> f :operation_symbol) "-")) (or (= (-> s :operation_symbol) "+") (= (-> s :operation_symbol) "-"))) true
          :else
          false))

(defn run-express [stack_number stack_symbo args-dic]
    (if (some? (peek stack_symbo))
        (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbo)]
            (cond (and (contains? args-dic (-> first_item :item_name)) (contains? args-dic (-> second_item :item_name)))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (get args-dic (-> first_item :item_name)) (get args-dic (-> second_item :item_name))), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                  (contains? args-dic (-> first_item :item_name))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (get args-dic (-> first_item :item_name)) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                  (contains? args-dic (-> second_item :item_name))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (get args-dic (-> second_item :item_name))), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                  :else
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                  ))
        (-> (first stack_number) :item_name)
        ;(first stack_number)
        ))

(defn calculate
    ([^Ignite ignite ^Long group_id lst args-dic] (calculate ignite group_id lst [] [] args-dic))
    ([^Ignite ignite ^Long group_id [f & r] stack_number stack_symbol args-dic]
     (if (some? f)
         (cond (contains? f :operation_symbol) (cond
                                                   ; 若符号栈为空，则符号直接压入符号栈
                                                   (= (count stack_symbol) 0) (recur ignite group_id r stack_number (conj stack_symbol f) args-dic)
                                                   ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                   (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id r stack_number (conj stack_symbol f) args-dic)
                                                   ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                   :else
                                                   (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                       ;(recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                       (cond (and (contains? args-dic (-> first_item :item_name)) (contains? args-dic (-> second_item :item_name)))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (get args-dic (-> first_item :item_name)) (get args-dic (-> second_item :item_name))), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                             (contains? args-dic (-> first_item :item_name))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (get args-dic (-> first_item :item_name)) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                             (contains? args-dic (-> second_item :item_name))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (get args-dic (-> second_item :item_name))), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                             :else
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                             ))
                                                   )
               (contains? f :parenthesis) (let [m (calculate ignite group_id (reverse (-> f :parenthesis)) args-dic)]
                                              (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol args-dic))
               (contains? f :item_name) (recur ignite group_id r (conj stack_number f) stack_symbol args-dic)
               (contains? f :func-name) (let [m (func-to-clj ignite group_id f args-dic)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol args-dic))
               :else
               (recur ignite group_id r (conj stack_number f) stack_symbol args-dic)
               )
         (run-express stack_number stack_symbol args-dic))))

; 判断是否有函数
(defn has-func? [let-obj func-name]
    (cond (contains? let-obj :seq-obj) (if (contains? #{"add" "remove" "nth" "concat" "pop" "take" "takeLast" "drop" "dropLast"} (str/lower-case func-name))
                                           true
                                           false)
          (contains? let-obj :map-obj) (if (contains? #{"put" "get" "remove"} (str/lower-case func-name))
                                           true
                                           false)
          :else
          true
          ))

(defn smart-func [func-name]
    (cond (my-lexical/is-eq? func-name "add") "my-lexical/list-add"
          (my-lexical/is-eq? func-name "set") "my-lexical/list-set"
          (my-lexical/is-eq? func-name "take") "my-lexical/list-take"
          (my-lexical/is-eq? func-name "drop") "my-lexical/list-drop"
          (my-lexical/is-eq? func-name "nth") "nth"
          (my-lexical/is-eq? func-name "count") "count"
          (my-lexical/is-eq? func-name "concat") "concat"
          (my-lexical/is-eq? func-name "put") "assoc"
          (my-lexical/is-eq? func-name "get") "my-lexical/map-list-get"
          (my-lexical/is-eq? func-name "remove") "dissoc"
          (my-lexical/is-eq? func-name "pop") "my-lexical/list-peek"
          (my-lexical/is-eq? func-name "peek") "my-lexical/list-peek"
          (my-lexical/is-eq? func-name "takeLast") "my-lexical/list-take-last"
          (my-lexical/is-eq? func-name "dropLast") "my-lexical/list-drop-last"
          :else
          (str/lower-case func-name)
          ))

(defn smart-func-lst
    ([^Ignite ignite group_id m args-dic] (smart-func-lst ignite group_id m args-dic [] []))
    ([^Ignite ignite group_id m args-dic head-lst tail-lst]
     (let [{my-func-name :func-name lst_ps :lst_ps ds-name :ds-name} m]
         (let [func-name (smart-func my-func-name)]
             (if-not (nil? (-> m :ds-lst))
                 (recur ignite group_id (-> m :ds-lst) args-dic (conj head-lst (format "%s " func-name)) (conj tail-lst (token-to-clj ignite group_id lst_ps args-dic)))
                 (if-not (= ds-name "")
                     [(conj head-lst (format "%s (.getVar %s)" func-name ds-name)) (conj tail-lst (token-to-clj ignite group_id lst_ps args-dic))]
                     [(conj head-lst (format "%s " func-name)) (conj tail-lst (token-to-clj ignite group_id lst_ps args-dic))])))
         )))

(defn smart-func-to-clj [^Ignite ignite group_id m args-dic]
    (let [[head-lst tail-lst] (smart-func-lst ignite group_id m args-dic)]
        (loop [[f & r] (reverse head-lst) [f-t & r-t] (reverse tail-lst) sb (StringBuilder.)]
            (if (some? f)
                (if-not (Strings/isNullOrEmpty (.toString sb))
                    (recur r r-t (doto sb (.append (str "(" f " "))
                                          (.append (.toString sb))
                                          (.append (str " " f-t))
                                          (.append ")")
                                          ))
                    (recur r r-t (doto sb (.append (str "(" f " " f-t ")"))
                                          )))
                (.toString sb)))))

; 调用方法这个至关重要
(defn func-to-clj [^Ignite ignite group_id m args-dic]
    (let [{func-name :func-name lst_ps :lst_ps} m]
        (cond (my-lexical/is-eq? func-name "trans") (let [t_f (gensym "t_f") t_r (gensym "t_r") lst-rs (gensym "lst-rs-")]
                                                        (format "(loop [[f_%s & r_%s] %s lst-rs-%s []]
                                                                   (if (some? f_%s)
                                                                         (let [[sql args] f_%s]
                                                                                 (let [sql-lst (my-lexical/to-back (apply format sql (my-args args)))]
                                                                                     (cond (my-lexical/is-eq? (first sql-lst) \"insert\") (recur r_%s (conj lst-rs-%s (insert-to-cache ignite group_id %s sql-lst)))
                                                                                           (my-lexical/is-eq? (first sql-lst) \"update\") ()
                                                                                           (my-lexical/is-eq? (first sql-lst) \"delete\") ()
                                                                                     )))
                                                                         (if-not (empty? lst-rs-%s)
                                                                             (MyCacheExUtil/transCache ignite lst-rs-%s))
                                                                   ))" t_f t_r (-> (first lst_ps) :item_name) lst-rs
                                                                t_f
                                                                t_f
                                                                t_r lst-rs (str args-dic)
                                                                lst-rs
                                                                lst-rs
                                                                ))
              (is-func? ignite func-name) (format "(my-smart-scenes/my-invoke-func ignite group_id %s %s)" func-name (token-to-clj ignite group_id lst_ps args-dic))
              (is-scenes? ignite group_id func-name) (format "(my-smart-scenes/my-invoke-scenes ignite group_id %s %s)" func-name (token-to-clj ignite group_id lst_ps args-dic))
              (my-lexical/is-eq? "log" func-name) (format "(log %s)" (token-to-clj ignite group_id lst_ps args-dic))
              (my-lexical/is-eq? "println" func-name) (format "(println %s)" (token-to-clj ignite group_id lst_ps args-dic))
              (re-find #"\." func-name) (let [{let-name :schema_name method-name :table_name} (my-lexical/get-schema func-name)]
                                            (if (> (count lst_ps) 0)
                                                (format "(%s (.getVar %s) %s)" (smart-func method-name) let-name (token-to-clj ignite group_id lst_ps args-dic))
                                                (format "(%s (.getVar %s))" (smart-func method-name) let-name))
                                            ;(if-let [let-obj (get-let-context let-name args-dic)]
                                            ;    (if (and (true? (has-func? let-obj method-name)) (= let-name ""))
                                            ;        (if (> (count lst_ps) 0)
                                            ;            (format "(%s %s %s)" method-name let-name (token-to-clj ignite group_id lst_ps args-dic))
                                            ;            (format "(%s %s)" method-name let-name)))
                                            ;    )
                                            )
              ; 系统函数
              (contains? #{"first" "rest" "next" "second"} (str/lower-case func-name)) (format "(%s %s)" (str/lower-case func-name) (token-to-clj ignite group_id lst_ps args-dic))
              ; inner function
              ;(get-inner-function-context (str/lower-case func-name) args-dic) (format "(%s %s)" (str/lower-case func-name) (token-to-clj ignite group_id lst_ps args-dic))
              (my-lexical/is-eq? func-name "query_sql") (format "(%s %s)" (str/lower-case func-name) (token-to-clj ignite group_id lst_ps args-dic))
              :else
              (println "Inner func")
              )))

(defn item-to-clj [m args-dic]
    (if (contains? args-dic (-> m :item_name))
        (get args-dic (-> m :item_name))
        (-> m :item_name)))

(defn judge [ignite group_id lst args-dic]
    (cond (= (count lst) 3) (format "(%s %s %s)" (str/lower-case (-> (second lst) :and_or_symbol)) (token-to-clj ignite group_id (first lst) args-dic) (token-to-clj ignite group_id (last lst) args-dic))
          (> (count lst) 3) (let [top (judge ignite group_id (take 3 lst) args-dic)]
                                (judge ignite group_id (concat [top] (drop 3 lst)) args-dic))
          :else
          (throw (Exception. "判断语句错误！"))
          ))

(defn parenthesis-to-clj [ignite group_id m args-dic]
    (if (> (count (filter #(contains? % :comma_symbol) (-> m :parenthesis))))
        (loop [[f & r] (filter #(not (contains? % :comma_symbol)) (-> m :parenthesis)) lst []]
            (if (some? f)
                (recur r (conj lst (token-to-clj ignite group_id f args-dic)))
                (str/join " " lst)))
        (calculate ignite group_id (reverse (-> m :parenthesis)) args-dic)))

(defn token-lst-to-clj [ignite group_id m args-dic]
    (cond (and (= (count m) 3) (and (contains? (second m) :comparison_symbol) (= (-> (second m) :comparison_symbol) "=")) (contains? (first m) :item_name)) (format "(.setVar %s %s)" (-> (first m) :item_name) (token-to-clj ignite group_id (last m) args-dic))
          (and (= (count m) 3) (and (contains? (second m) :comparison_symbol) (contains? #{">" ">=" "<" "<="} (-> (second m) :comparison_symbol))) (contains? (first m) :item_name)) (format "(%s %s %s)" (-> (second m) :comparison_symbol) (token-to-clj ignite group_id (first m) args-dic) (token-to-clj ignite group_id (last m) args-dic))
          (and (= (count m) 3) (and (contains? (second m) :comparison_symbol) (= (-> (second m) :comparison_symbol) "<>")) (contains? (first m) :item_name)) (format "(not (= %s %s))" (token-to-clj ignite group_id (first m) args-dic) (token-to-clj ignite group_id (last m) args-dic))
          (and (>= (count m) 3) (contains? (second m) :and_or_symbol)) (judge ignite group_id m args-dic)
          (and (= (count m) 3) (contains? (second m) :in_symbol)) (if (my-lexical/is-eq? (-> (second m) :comparison_symbol) "in")
                                                                      (format "(contains? #{%s} %s)" (token-to-clj ignite group_id (last m) args-dic) (token-to-clj ignite group_id (first m) args-dic))
                                                                      (format "(not (contains? #{%s} %s))" (token-to-clj ignite group_id (last m) args-dic) (token-to-clj ignite group_id (first m) args-dic)))
          :else
          (loop [[f & r] m lst []]
              (if (some? f)
                  (recur r (conj lst (token-to-clj ignite group_id f args-dic)))
                  (str/join " " lst)))
          )
    )

(defn token-clj [ignite group_id m args-dic]
    (if (some? m)
        (cond (my-lexical/is-seq? m) (token-lst-to-clj ignite group_id m args-dic)
              (map? m) (map-token-to-clj ignite group_id m args-dic)
              (string? m) m
              )))

(defn token-to-clj [ignite group_id m args-dic]
    (let [rs (token-clj ignite group_id m args-dic)]
        (if (nil? rs)
            ""
            rs)))

(defn map-token-to-clj [ignite group_id m args-dic]
    (if (some? m)
        (cond (and (contains? m :func-name) (contains? m :lst_ps) (not (contains? m :ds-name))) (func-to-clj ignite group_id m args-dic)
              (and (contains? m :func-name) (contains? m :lst_ps) (contains? m :ds-name)) (smart-func-to-clj ignite group_id m args-dic)
              (contains? m :and_or_symbol) (get m :and_or_symbol)
              (contains? m :operation) (calculate ignite group_id (reverse (-> m :operation)) args-dic)
              (contains? m :comparison_symbol) (get m :comparison_symbol)
              (contains? m :operation_symbol) (get m :operation_symbol)
              ;(contains? m :comma_symbol) (get m :comma_symbol)
              (contains? m :item_name) (item-to-clj m args-dic)
              (contains? m :parenthesis) (parenthesis-to-clj ignite group_id m args-dic)
              (contains? m :seq-obj) (seq-obj-to-clj ignite group_id (-> m :seq-obj) args-dic)
              (contains? m :map-obj) (map-obj-to-clj ignite group_id (-> m :map-obj) args-dic)
              )))

(defn seq-obj-to-clj [ignite group_id m args-dic]
    (format "(my-lexical/to_arryList [%s])" (token-to-clj ignite group_id m args-dic)))

(defn map-obj-to-clj [ignite group_id m args-dic]
    (loop [[f & r] m lst-rs []]
        (if (some? f)
            (recur r (concat lst-rs [(token-to-clj ignite group_id (-> f :key) args-dic) (token-to-clj ignite group_id (-> f :value) args-dic)]))
            (format "{%s}" (str/join " " lst-rs)))))

(defn func-token-to-clj [ignite group_id m args-dic]
    (if (contains? m :item_name)
        (item-to-clj m args-dic)
        (let [fn-line (token-to-clj ignite group_id m args-dic)]
            (if-not (Strings/isNullOrEmpty fn-line)
                (apply (eval (read-string (format "(fn [ignite group_id %s]\n     %s)" (str/join " " (keys args-dic)) fn-line))) (concat [ignite group_id] (vals args-dic))))))
    )