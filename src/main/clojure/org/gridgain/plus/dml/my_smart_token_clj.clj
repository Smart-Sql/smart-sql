(ns org.gridgain.plus.dml.my-smart-token-clj
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar MyLetLayer)
             (com.google.common.base Strings)
             (cn.plus.model MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.gridgain.myservice MyLoadSmartSqlService MySmartFuncInit)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
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
         token-to-clj map-token-to-clj parenthesis-to-clj seq-obj-to-clj map-obj-to-clj func-link-to-clj get-lst-ps-vs)

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
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  ))
        (-> (first stack_number) :item_name)
        ;(first stack_number)
        ))

(defn calculate
    ([^Ignite ignite group_id lst my-context] (calculate ignite group_id lst [] [] my-context))
    ([^Ignite ignite group_id [f & r] stack_number stack_symbol my-context]
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
               (contains? f :operation) (let [m (calculate ignite group_id (reverse (-> f :operation)) my-context)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               (contains? f :item_name) (recur ignite group_id r (conj stack_number f) stack_symbol my-context)
               (contains? f :func-name) (let [m (func-to-clj ignite group_id f my-context)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               (contains? f :func-link) (let [m (func-link-to-clj ignite group_id (-> f :func-link) my-context)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               :else
               (recur ignite group_id r (conj stack_number f) stack_symbol my-context)
               )
         (run-express stack_number stack_symbol my-context))))

; 判断 func
(defn is-func? [^Ignite ignite ^String func-name]
    (.containsKey (.cache ignite "my_func") (str/lower-case func-name)))

; 判断 scenes
(defn is-scenes? [^Ignite ignite group_id ^String scenes-name]
    (.containsKey (.cache ignite "my_scenes") (MyScenesCachePk. (first group_id) (str/lower-case scenes-name))))

(defn is-call-scenes? [^Ignite ignite group_id ^String scenes-name]
    (.containsKey (.cache ignite "call_scenes") (MyCallScenesPk. (first group_id) scenes-name)))

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

(defn add_user_group [ignite group_id ^String group_name ^String user_token ^String group_type ^String schema_name]
    (.addUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id group_name user_token group_type schema_name))

(defn update_user_group [ignite group_id ^String group_name ^String group_type]
    (.updateUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id group_name group_type))

(defn delete_user_group [ignite group_id ^String group_name]
    (.deleteUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id group_name))

(defn has_user_token_type [^String user_token_type]
    (.hasUserTokenType (.getSmartFuncInit (MySmartFuncInit/getInstance)) user_token_type))

(defn get_user_group [ignite group_id user_token]
    (.getUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id user_token))

(defn get_user_token [ignite group_name]
    (.getUserToken (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_name))

; 调用方法这个至关重要
(defn func-to-clj [^Ignite ignite group_id m my-context]
    (let [{func-name :func-name lst_ps :lst_ps} m]
        (if-let [my-lexical-func (my-lexical/smart-func func-name)]
            (format "(%s %s)" my-lexical-func (get-lst-ps-vs ignite group_id lst_ps my-context))
            (cond
                  ;(my-lexical/is-eq? "log" func-name) (format "(log %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? "println" func-name) (format "(my-lexical/my-show-msg (my-lexical/gson %s))" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  ;(re-find #"\." func-name) (let [{let-name :schema_name method-name :table_name} (my-lexical/get-schema func-name)]
                  ;                              (if (> (count lst_ps) 0)
                  ;                                  (format "(%s (my-lexical/get-value %s) %s)" (my-lexical/smart-func method-name) let-name (get-lst-ps-vs ignite group_id lst_ps my-context))
                  ;                                  (format "(%s (my-lexical/get-value %s))" (my-lexical/smart-func method-name) let-name))
                  ;                              )
                  ; 系统函数
                  (contains? #{"first" "rest" "next" "second" "last"} (str/lower-case func-name)) (format "(%s %s)" (str/lower-case func-name) (get-lst-ps-vs ignite group_id lst_ps my-context))
                  ; inner function
                  (get-inner-function-context func-name my-context) (format "(%s %s)" func-name (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "query_sql") (cond (= (count lst_ps) 1) (format "(my-smart-db/query_sql ignite group_id %s nil)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                                                                  (> (count lst_ps) 1) (format "(my-smart-db/query_sql ignite group_id %s [%s])" (get-lst-ps-vs ignite group_id (first lst_ps) my-context) (get-lst-ps-vs ignite group_id (rest lst_ps) my-context))
                                                                  )
                  (and (contains? my-context :top-func) (= func-name (-> my-context :top-func))) (format "(%s ignite group_id %s)" func-name (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "empty?") (format "(empty? %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "notEmpty?") (format "(my-lexical/not-empty? %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "noSqlCreate") (format "(my-lexical/no-sql-create ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlGet") (format "(my-lexical/no-sql-get-vs ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "noSqlInsertTran") (format "(my-lexical/no-sql-insert-tran ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlUpdateTran") (format "(my-lexical/no-sql-update-tran ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlDeleteTran") (format "(my-lexical/no-sql-delete-tran ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlDrop") (format "(my-lexical/no-sql-drop ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlInsert") (format "(my-lexical/no-sql-insert ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlUpdate") (format "(my-lexical/no-sql-update ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "noSqlDelete") (format "(my-lexical/no-sql-delete ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "auto_id") (format "(my-lexical/auto_id ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "trans") (format "(my-smart-db/trans ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "my_view") (format "(smart-func/smart-view ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "rm_view") (format "(smart-func/rm-smart-view ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "add_scenes_to") (format "(smart-func/add-scenes-to ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "rm_scenes_from") (format "(smart-func/rm-scenes-from ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "add_job") (format "(smart-func/add-job ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "remove_job") (format "(smart-func/remove-job ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "job_snapshot") (format "(smart-func/get-job-snapshot ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "add_func") (format "(smart-func/add_func ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "remove_func") (format "(smart-func/remove_func ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "recovery_to_cluster") (format "(smart-func/recovery-to-cluster ignite %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  ; 机器学习
                  (my-lexical/is-eq? func-name "create_train_matrix") (format "(my-ml-train-data/create-train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "has_train_matrix") (format "(my-ml-train-data/has-train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "drop_train_matrix") (format "(my-ml-train-data/drop-train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "train_matrix") (format "(my-ml-train-data/train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "fit") (format "(my-ml-func/ml-fit ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "predict") (format "(my-ml-func/ml-predict ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))

                  (my-lexical/is-eq? func-name "train_matrix_single") (format "(my-ml-train-data/train-matrix-single ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "loadCsv") (.loadCsv (.getLoadSmartSql (MyLoadSmartSqlService/getInstance)) ignite group_id (eval (read-string (get-lst-ps-vs ignite group_id lst_ps my-context))))

                  (is-func? ignite func-name) (if-not (empty? lst_ps)
                                                  (format "(my-smart-scenes/my-invoke-func ignite \"%s\" [%s])" func-name (get-lst-ps-vs ignite group_id lst_ps my-context))
                                                  (format "(my-smart-scenes/my-invoke-func-no-ps ignite \"%s\")" func-name))
                  (is-scenes? ignite group_id func-name) (if-not (empty? lst_ps)
                                                             (format "(my-smart-scenes/my-invoke-scenes ignite group_id \"%s\" [%s])" func-name (get-lst-ps-vs ignite group_id lst_ps my-context))
                                                             (format "(my-smart-scenes/my-invoke-scenes-no-ps ignite group_id \"%s\")" func-name))
                  (is-call-scenes? ignite group_id func-name) (if-not (empty? lst_ps)
                                                                  (format "(my-smart-scenes/my-invoke-scenes ignite group_id \"%s\" [%s])" func-name (get-lst-ps-vs ignite group_id lst_ps my-context))
                                                                  (format "(my-smart-scenes/my-invoke-scenes-no-ps ignite group_id \"%s\")" func-name))

                  (my-lexical/is-eq? func-name "loadCode") (.loadSmartSql (.getLoadSmartSql (MyLoadSmartSqlService/getInstance)) ignite group_id (-> (first lst_ps) :item_name))

                  (my-lexical/is-eq? func-name "has_user_token_type") (format "(my-smart-token-clj/has_user_token_type %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "get_user_group") (format "(my-smart-token-clj/get_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "get_user_token") (format "(my-smart-token-clj/get_user_token ignite %s)" (get-lst-ps-vs ignite group_id lst_ps my-context))
                  (my-lexical/is-eq? func-name "add_user_group") (format "(my-smart-token-clj/add_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context));(apply add_user_group (concat [ignite group_id] (eval (read-string (format "[%s]" (get-lst-ps-vs ignite group_id lst_ps my-context))))))
                  (my-lexical/is-eq? func-name "update_user_group") (format "(my-smart-token-clj/update_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context));(apply update_user_group (concat [ignite group_id] (eval (read-string (format "[%s]" (get-lst-ps-vs ignite group_id lst_ps my-context))))))
                  (my-lexical/is-eq? func-name "delete_user_group") (format "(my-smart-token-clj/delete_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps my-context));(delete_user_group ignite group_id (get-lst-ps-vs ignite group_id lst_ps my-context))
                  :else
                  (throw (Exception. (format "%s 不存在，或没有权限！" func-name)))
                  ))
        ))

(defn get-lst-ps-vs
    ([ignite group_id lst my-context] (get-lst-ps-vs ignite group_id lst my-context []))
    ([ignite group_id [f & r] my-context lst]
     (if (some? f)
         (if-not (contains? f :comma_symbol)
             (let [m-line (token-to-clj ignite group_id f my-context)]
                 (if (and (contains? f :item_name) (false? (-> f :const)) (not (re-find #"^\(my-lexical/get-value\s" m-line)))
                     (recur ignite group_id r my-context (conj lst (format "(my-lexical/get-value %s)" m-line)))
                     (recur ignite group_id r my-context (conj lst m-line))))
             (recur ignite group_id r my-context lst))
         (str/trim (str/join " " lst)))))

(defn item-to-clj [m my-context]
    (let [my-let (get-let-context (-> m :item_name) my-context)]
        (if (some? my-let)
            (format "(my-lexical/get-value %s)" (-> m :item_name))
            (cond (my-lexical/is-eq? (-> m :item_name) "null") "nil"
                  (and (= java.lang.String (-> m :java_item_type)) (true? (-> m :const))) (my-lexical/my-str-value (-> m :item_name))
                  (and (not (= java.lang.String (-> m :java_item_type))) (true? (-> m :const))) (-> m :item_name)
                  :else
                  (-> m :item_name)))))

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
          (and (> (count (filter #(and (map? %) (contains? % :comparison_symbol)) (-> m :parenthesis))) 0) (= (count (-> m :parenthesis)) 3)) (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> (second (-> m :parenthesis)) :comparison_symbol) (token-to-clj ignite group_id (first (-> m :parenthesis)) my-context) (token-to-clj ignite group_id (last (-> m :parenthesis)) my-context))
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
        (cond (contains? m :func-name) (func-to-clj ignite group_id m my-context)
              ;(and (contains? m :func-name) (contains? m :lst_ps)) (func-to-clj ignite group_id m my-context)
              ;(and (contains? m :func-name) (contains? m :lst_ps) (contains? m :ds-name)) (smart-func-to-clj ignite group_id m my-context)
              ;(contains? m :func-link) (func-link-to-clj ignite group_id (reverse (-> m :func-link)) my-context)
              (contains? m :func-link) (func-link-to-clj ignite group_id (-> m :func-link) my-context)
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

;(defn func-link-to-clj [ignite group_id [f & r] my-context]
;    (cond (and (some? f) (or (nil? r) (empty? r))) (token-clj ignite group_id f my-context)
;          (and (some? f) (some? r)) (let [up-items (func-link-to-clj ignite group_id r my-context)]
;                                        (if-not (Strings/isNullOrEmpty up-items)
;                                            (let [m (assoc f :lst_ps (conj (-> f :lst_ps) {:table_alias "", :item_name up-items, :item_type "", :java_item_type nil, :const false}))]
;                                                (func-to-clj ignite group_id m my-context))
;                                            ))
;          )
;    )

(defn func-link-to-clj [ignite group_id [f & r] my-context]
    (cond (and (some? f) (or (nil? r) (empty? r))) (token-clj ignite group_id f my-context)
          (and (some? f) (some? r)) (let [first-item (token-clj ignite group_id f my-context) next-item (first r)]
                                        (let [m (assoc next-item :lst_ps (concat [{:table_alias "", :item_name first-item, :item_type "", :java_item_type nil, :const false}] (-> next-item :lst_ps)))]
                                            (func-link-to-clj ignite group_id (concat [m] (rest r)) my-context)))
          ))

(declare func-link-to-ps-clj func-link-to-ps-map-clj func-link-to-ps-seq-clj)

(defn func-link-to-ps-clj [m]
    (cond (map? m) (func-link-to-ps-map-clj m)
          (my-lexical/is-seq? m) (func-link-to-ps-seq-clj m)))

(defn func-link-to-ps-map-clj [m]
    (cond (and (contains? m :item_name) (false? (-> m :const))) (cond (= (-> m :table_alias) "") (if (= (-> m :item_name) "?")
                                                                                                     (let [ps-line (format "%s" (gensym "ps"))]
                                                                                                         {:args [ps-line] :ast (assoc m :item_name ps-line)})
                                                                                                     {:args [(-> m :item_name)] :ast m})
                                                                      :else {:args [(format "%s_%s" (-> m :table_alias) (-> m :item_name))] :ast (assoc m :item_name (format "%s_%s" (-> m :table_alias) (-> m :item_name)) :table_alias "")})
          (and (contains? m :item_name) (true? (-> m :const))) {:args [] :ast m}
          :else
          (loop [[f & r] (keys m) lst-rs [] my-ast m]
              (if (some? f)
                  (let [{f-m :args ast :ast} (func-link-to-ps-clj (get m f))]
                      (if-not (nil? ast)
                          (if (and (not (empty? f-m)) (not (nil? f-m)))
                              (recur r (concat lst-rs f-m) (assoc my-ast f ast))
                              (recur r lst-rs (assoc my-ast f ast)))
                          (recur r lst-rs my-ast))
                      )
                  {:args lst-rs :ast my-ast}))
          ))

(defn func-link-to-ps-seq-clj [m]
    (loop [[f & r] m lst-rs [] my-ast []]
        (if (some? f)
            (let [{f-m :args ast :ast} (func-link-to-ps-clj f)]
                (if-not (nil? ast)
                    (if (and (not (empty? f-m)) (not (nil? f-m)))
                        (recur r (concat lst-rs f-m) (conj my-ast ast))
                        (recur r lst-rs (conj my-ast ast)))
                    (recur r lst-rs my-ast)))
            {:args lst-rs :ast my-ast})))

; 生成联级方法调用的匿名函数
; 1、 获取所有的参数，形成一个参数列表
(defn func-link-clj [ignite group_id m]
    (let [{lst-ps :args my-ast :ast} (func-link-to-ps-clj m)]
        (let [func-body (func-link-to-clj ignite group_id (-> my-ast :func-link) nil)]
            [(format "(fn [ignite group_id %s]\n     (do %s))" (str/join " " lst-ps) func-body) lst-ps])))

;(defn map-obj-to-clj [ignite group_id m my-context]
;    (loop [[f & r] m lst-rs []]
;        (if (some? f)
;            (if (and (contains? (-> f :key) :item_name) (nil? (-> f :key :java_item_type)) (false? (-> f :key :const)))
;                (recur r (concat lst-rs [(format "\"%s\"" (-> f :key :item_name)) (token-to-clj ignite group_id (-> f :value) my-context)]))
;                (recur r (concat lst-rs [(token-to-clj ignite group_id (-> f :key) my-context) (token-to-clj ignite group_id (-> f :value) my-context)])))
;            (format "{%s}" (str/join " " lst-rs)))))




































