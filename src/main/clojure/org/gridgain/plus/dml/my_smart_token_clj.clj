(ns org.gridgain.plus.dml.my-smart-token-clj
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
        :name org.gridgain.plus.dml.MySmartTokenClj
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare is-symbol-priority run-express calculate is-func? is-scenes? func-to-clj item-to-clj token-lst-to-clj
         token-to-clj map-token-to-clj parenthesis-to-clj seq-obj-to-clj map-obj-to-clj smart-func-to-clj)

; 添加 let 定义到 my-context
(defn add-let-to-context [let-name let-vs my-context]
    (if (some? my-context)
        (let [let-params (-> my-context :let-params)]
            (assoc my-context :let-params (assoc let-params let-name let-vs)))))

; 获取 let 的定义
(defn get-let-context [let-name my-context]
    (let [let-params (-> my-context :let-params)]
        (let [m (get let-params let-name)]
            (if (some? m)
                m
                (if-not (nil? (-> my-context :up-my-context))
                    (get-let-context let-name (-> my-context :up-my-context)))))))

; 获取 inner function
(defn get-inner-function-context [func-name my-context]
    (let [inner-funcs (-> my-context :inner-func)]
        (if (contains? inner-funcs func-name)
            true
            (if-let [my-ctx (-> my-context :up-my-context)]
                (get-inner-function-context func-name my-ctx)
                false))
        ))

; 判断符号优先级
; f symbol 的优先级大于等于 s 返回 true 否则返回 false
(defn is-symbol-priority [f s]
    (cond (or (= (-> f :operation_symbol) "*") (= (-> f :operation_symbol) "/")) true
          (and (or (= (-> f :operation_symbol) "+") (= (-> f :operation_symbol) "-")) (or (= (-> s :operation_symbol) "+") (= (-> s :operation_symbol) "-"))) true
          :else
          false))

(defn run-express [stack_number stack_symbo my-context]
    (if (some? (peek stack_symbo))
        (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbo)]
            (cond (and (contains? (-> my-context :let-params) (-> first_item :item_name)) (contains? (-> my-context :let-params) (-> second_item :item_name)))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  (contains? (-> my-context :let-params) (-> first_item :item_name))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  (contains? (-> my-context :let-params) (-> second_item :item_name))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  :else
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  ))
        (-> (first stack_number) :item_name)
        ;(first stack_number)
        ))

(defn calculate
    ([^Ignite ignite ^Long group_id lst my-context] (calculate ignite group_id lst [] [] my-context))
    ([^Ignite ignite ^Long group_id [f & r] stack_number stack_symbol my-context]
     (if (some? f)
         (cond (contains? f :operation_symbol) (cond
                                                   ; 若符号栈为空，则符号直接压入符号栈
                                                   (= (count stack_symbol) 0) (recur ignite group_id r stack_number (conj stack_symbol f) my-context)
                                                   ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                   (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id r stack_number (conj stack_symbol f) my-context)
                                                   ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                   :else
                                                   (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                       ;(recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                       (cond (and (contains? (-> my-context :let-params) (-> first_item :item_name)) (contains? (-> my-context :let-params) (-> second_item :item_name)))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             (contains? (-> my-context :let-params) (-> first_item :item_name))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             (contains? (-> my-context :let-params) (-> second_item :item_name))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             :else
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             ))
                                                   )
               (contains? f :parenthesis) (let [m (calculate ignite group_id (reverse (-> f :parenthesis)) my-context)]
                                              (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               (contains? f :item_name) (recur ignite group_id r (conj stack_number f) stack_symbol my-context)
               (contains? f :func-name) (let [m (func-to-clj ignite group_id f my-context)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               :else
               (recur ignite group_id r (conj stack_number f) stack_symbol my-context)
               )
         (run-express stack_number stack_symbol my-context))))

; 判断 func
(defn is-func? [^Ignite ignite ^String func-name]
    (.containsKey (.cache ignite "my_func") func-name))

; 判断 scenes
(defn is-scenes? [^Ignite ignite ^Long group_id ^String scenes-name]
    (.containsKey (.cache ignite "my_scenes") (MyScenesCachePk. group_id scenes-name)))

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
          (my-lexical/is-eq? func-name "put") ".put"
          (my-lexical/is-eq? func-name "get") "my-lexical/map-list-get"
          (my-lexical/is-eq? func-name "remove") "dissoc"
          (my-lexical/is-eq? func-name "pop") "my-lexical/list-peek"
          (my-lexical/is-eq? func-name "peek") "my-lexical/list-peek"
          (my-lexical/is-eq? func-name "takeLast") "my-lexical/list-take-last"
          (my-lexical/is-eq? func-name "dropLast") "my-lexical/list-drop-last"
          (my-lexical/is-eq? func-name "empty?") "empty?"
          (my-lexical/is-eq? func-name "notEmpty?") "my-lexical/not-empty?"
          :else
          (str/lower-case func-name)
          ))

(defn smart-func-lst
    ([^Ignite ignite group_id m my-context] (smart-func-lst ignite group_id m my-context [] []))
    ([^Ignite ignite group_id m my-context head-lst tail-lst]
     (let [{my-func-name :func-name lst_ps :lst_ps ds-name :ds-name} m]
         (let [func-name (smart-func my-func-name)]
             (if-not (nil? (-> m :ds-lst))
                 (recur ignite group_id (-> m :ds-lst) my-context (conj head-lst (format "%s " func-name)) (conj tail-lst (token-to-clj ignite group_id lst_ps my-context)))
                 (if-not (= ds-name "")
                     [(conj head-lst (format "%s (my-lexical/get-value %s)" func-name ds-name)) (conj tail-lst (token-to-clj ignite group_id lst_ps my-context))]
                     [(conj head-lst (format "%s " func-name)) (conj tail-lst (token-to-clj ignite group_id lst_ps my-context))])))
         )))

(defn smart-func-to-clj [^Ignite ignite group_id m my-context]
    (let [[head-lst tail-lst] (smart-func-lst ignite group_id m my-context)]
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
(defn func-to-clj [^Ignite ignite group_id m my-context]
    (let [{func-name :func-name lst_ps :lst_ps} m]
        (cond (my-lexical/is-eq? func-name "trans") (format "(%s ignite group_id %s)" (str/lower-case func-name) (token-to-clj ignite group_id lst_ps my-context))
              (is-func? ignite func-name) (format "(my-smart-scenes/my-invoke-func ignite \"%s\" %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (is-scenes? ignite group_id func-name) (format "(my-smart-scenes/my-invoke-scenes ignite group_id \"%s\" %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (my-lexical/is-eq? "log" func-name) (format "(log %s)" (token-to-clj ignite group_id lst_ps my-context))
              (my-lexical/is-eq? "println" func-name) (format "(println %s)" (token-to-clj ignite group_id lst_ps my-context))
              (re-find #"\." func-name) (let [{let-name :schema_name method-name :table_name} (my-lexical/get-schema func-name)]
                                            (if (> (count lst_ps) 0)
                                                (format "(%s (my-lexical/get-value %s) %s)" (smart-func method-name) let-name (token-to-clj ignite group_id lst_ps my-context))
                                                (format "(%s (my-lexical/get-value %s))" (smart-func method-name) let-name))
                                            ;(if-let [let-obj (get-let-context let-name my-context)]
                                            ;    (if (and (true? (has-func? let-obj method-name)) (= let-name ""))
                                            ;        (if (> (count lst_ps) 0)
                                            ;            (format "(%s %s %s)" method-name let-name (token-to-clj ignite group_id lst_ps my-context))
                                            ;            (format "(%s %s)" method-name let-name)))
                                            ;    )
                                            )
              ; 系统函数
              (contains? #{"first" "rest" "next" "second"} (str/lower-case func-name)) (format "(%s %s)" (str/lower-case func-name) (token-to-clj ignite group_id lst_ps my-context))
              ; inner function
              (get-inner-function-context (str/lower-case func-name) my-context) (format "(%s %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (my-lexical/is-eq? func-name "query_sql") (format "(%s ignite group_id %s)" (str/lower-case func-name) (token-to-clj ignite group_id lst_ps my-context))
              (and (contains? my-context :top-func) (= func-name (-> my-context :top-func))) (format "(%s ignite group_id %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (= func-name "empty?") (format "(empty? %s)" (token-to-clj ignite group_id lst_ps my-context))
              (= func-name "notEmpty?") (format "(my-lexical/not-empty? %s)" (token-to-clj ignite group_id lst_ps my-context))
              :else
              ;(format "(my-smart-scenes/my-invoke-func ignite %s %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (format "(%s %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              ;(println "Inner func")
              )))

(defn item-to-clj [m my-context]
    (let [my-let (get-let-context (-> m :item_name) my-context)]
        (if (some? my-let)
            (format "(my-lexical/get-value %s)" (-> m :item_name))
            (-> m :item_name))))

;(defn judge [ignite group_id lst my-context]
;    (cond (= (count lst) 3) (format "(%s %s %s)" (str/lower-case (-> (second lst) :and_or_symbol)) (token-to-clj ignite group_id (first lst) my-context) (token-to-clj ignite group_id (last lst) my-context))
;          (> (count lst) 3) (let [top (judge ignite group_id (take 3 lst) my-context)]
;                                (judge ignite group_id (concat [top] (drop 3 lst)) my-context))
;          :else
;          (throw (Exception. "判断语句错误！"))
;          ))

(defn judge [ignite group_id lst my-context]
    (let [[f s t & rs] lst]
        (if (and (not (empty? f)) (not (empty? s)) (not (empty? t)))
            (let [rs-line (format "(%s %s %s)" (str/lower-case (-> s :and_or_symbol)) (token-to-clj ignite group_id f my-context) (token-to-clj ignite group_id t my-context))]
                (if (nil? rs)
                    rs-line
                    (judge ignite group_id (concat [rs-line] rs) my-context)))
            (throw (Exception. "判断语句错误！")))))

(defn parenthesis-to-clj [ignite group_id m my-context]
    (cond (> (count (filter #(and (map? %) (contains? % :comma_symbol)) (-> m :parenthesis))) 0) (loop [[f & r] (filter #(and (map? %) (contains? % :comma_symbol)) (-> m :parenthesis)) lst []]
                                                                                                     (if (some? f)
                                                                                                         (recur r (conj lst (token-to-clj ignite group_id f my-context)))
                                                                                                         (str/join " " lst)))
          (and (> (count (filter #(and (map? %) (contains? % :comparison_symbol)) (-> m :parenthesis))) 0) (= (count (-> m :parenthesis)) 3)) (format "(%s %s %s)" (-> (second (-> m :parenthesis)) :comparison_symbol) (token-to-clj ignite group_id (first (-> m :parenthesis)) my-context) (token-to-clj ignite group_id (last (-> m :parenthesis)) my-context))
          (and (>= (count (-> m :parenthesis)) 3) (contains? (second (-> m :parenthesis)) :and_or_symbol)) (judge ignite group_id (-> m :parenthesis) my-context)
          :else
          (calculate ignite group_id (reverse (-> m :parenthesis)) my-context)))

(defn token-lst-to-clj [ignite group_id m my-context]
    (cond (and (= (count m) 3) (and (contains? (second m) :comparison_symbol) (= (-> (second m) :comparison_symbol) "=")) (contains? (first m) :item_name)) (format "(.setVar %s %s)" (-> (first m) :item_name) (token-to-clj ignite group_id (last m) my-context))
          (and (= (count m) 3) (and (contains? (second m) :comparison_symbol) (contains? #{">" ">=" "<" "<="} (-> (second m) :comparison_symbol))) (contains? (first m) :item_name)) (format "(%s %s %s)" (-> (second m) :comparison_symbol) (token-to-clj ignite group_id (first m) my-context) (token-to-clj ignite group_id (last m) my-context))
          (and (= (count m) 3) (and (contains? (second m) :comparison_symbol) (= (-> (second m) :comparison_symbol) "<>")) (contains? (first m) :item_name)) (format "(not (= %s %s))" (token-to-clj ignite group_id (first m) my-context) (token-to-clj ignite group_id (last m) my-context))
          (and (>= (count m) 3) (contains? (second m) :and_or_symbol)) (judge ignite group_id m my-context)
          (and (= (count m) 3) (contains? (second m) :in_symbol)) (if (my-lexical/is-eq? (-> (second m) :comparison_symbol) "in")
                                                                      (format "(contains? #{%s} %s)" (token-to-clj ignite group_id (last m) my-context) (token-to-clj ignite group_id (first m) my-context))
                                                                      (format "(not (contains? #{%s} %s))" (token-to-clj ignite group_id (last m) my-context) (token-to-clj ignite group_id (first m) my-context)))
          :else
          (loop [[f & r] m lst []]
              (if (some? f)
                  (recur r (conj lst (token-to-clj ignite group_id f my-context)))
                  (str/join " " lst)))
          )
    )

(defn token-clj [ignite group_id m my-context]
    (if (some? m)
        (cond (my-lexical/is-seq? m) (token-lst-to-clj ignite group_id m my-context)
              (map? m) (map-token-to-clj ignite group_id m my-context)
              (string? m) m
              )))

(defn token-to-clj [ignite group_id m my-context]
    (let [rs (token-clj ignite group_id m my-context)]
        (if (nil? rs)
            ""
            rs)))

(defn map-token-to-clj [ignite group_id m my-context]
    (if (some? m)
        (cond (and (contains? m :func-name) (contains? m :lst_ps) (not (contains? m :ds-name))) (func-to-clj ignite group_id m my-context)
              (and (contains? m :func-name) (contains? m :lst_ps) (contains? m :ds-name)) (smart-func-to-clj ignite group_id m my-context)
              (contains? m :and_or_symbol) (get m :and_or_symbol)
              (contains? m :operation) (calculate ignite group_id (reverse (-> m :operation)) my-context)
              (contains? m :comparison_symbol) (get m :comparison_symbol)
              (contains? m :operation_symbol) (get m :operation_symbol)
              ;(contains? m :comma_symbol) (get m :comma_symbol)
              (contains? m :item_name) (item-to-clj m my-context)
              (contains? m :parenthesis) (parenthesis-to-clj ignite group_id m my-context)
              (contains? m :seq-obj) (seq-obj-to-clj ignite group_id (-> m :seq-obj) my-context)
              (contains? m :map-obj) (map-obj-to-clj ignite group_id (-> m :map-obj) my-context)
              )))

(defn seq-obj-to-clj [ignite group_id m my-context]
    (format "(my-lexical/to_arryList [%s])" (token-to-clj ignite group_id m my-context)))

(defn map-obj-to-clj [ignite group_id m my-context]
    (loop [[f & r] m sb (StringBuilder.)]
        (if (some? f)
            (if (and (contains? (-> f :key) :item_name) (nil? (-> f :key :java_item_type)) (false? (-> f :key :const)))
                (recur r (doto sb (.append (format "(.put %s %s)" (-> f :key :item_name) (token-to-clj ignite group_id (-> f :value) my-context)))))
                (recur r (doto sb (.append (format "(.put %s %s)" (token-to-clj ignite group_id (-> f :key) my-context) (token-to-clj ignite group_id (-> f :value) my-context)))))
                )
            (format "(doto (Hashtable.)\n    %s)" (.toString sb)))))

;(defn map-obj-to-clj [ignite group_id m my-context]
;    (loop [[f & r] m lst-rs []]
;        (if (some? f)
;            (if (and (contains? (-> f :key) :item_name) (nil? (-> f :key :java_item_type)) (false? (-> f :key :const)))
;                (recur r (concat lst-rs [(format "\"%s\"" (-> f :key :item_name)) (token-to-clj ignite group_id (-> f :value) my-context)]))
;                (recur r (concat lst-rs [(token-to-clj ignite group_id (-> f :key) my-context) (token-to-clj ignite group_id (-> f :value) my-context)])))
;            (format "{%s}" (str/join " " lst-rs)))))
