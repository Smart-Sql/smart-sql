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
             (org.gridgain.myservice MyLoadSmartSqlService MySmartFuncInit)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (org.tools MyGson)
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

(declare is-symbol-priority run-express calculate is-func? is-scenes? func-to-clj item-to-clj token-lst-to-clj get-const-vs
         token-to-clj map-token-to-clj parenthesis-to-clj seq-obj-to-clj map-obj-to-clj func-link-to-clj get-lst-ps-vs)

(defn has-dic-item [args-dic m]
    (cond (contains? (-> args-dic :dic) (-> m :item_name)) true
          (contains? (-> args-dic :dic) (str/lower-case (-> m :item_name))) true
          :else false
          ))

(defn my-dic-item-name [args-dic m]
    (cond (contains? (-> args-dic :dic) (-> m :item_name)) (-> m :item_name)
          (contains? (-> args-dic :dic) (str/lower-case (-> m :item_name))) (str/lower-case (-> m :item_name))
          ))

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
            (cond (and (has-dic-item args-dic first_item) (has-dic-item args-dic second_item))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (my-dic-item-name args-dic first_item) (my-dic-item-name args-dic second_item)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                  (has-dic-item args-dic first_item) (cond (true? (-> second_item :const)) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) %s)" (-> top_symbol :operation_symbol) (my-dic-item-name args-dic first_item) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                                                           (false? (-> second_item :const)) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (my-dic-item-name args-dic first_item) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                                                           )
                  (has-dic-item args-dic second_item) (cond (true? (-> first_item :const)) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (my-dic-item-name args-dic second_item)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                                                            (false? (-> first_item :const)) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (my-dic-item-name args-dic second_item)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                                                            )
                  :else
                  (cond (and (true? (-> first_item :const)) (true? (-> second_item :const))) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                        (true? (-> first_item :const)) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                        (true? (-> second_item :const)) (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                        :else
                        (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) args-dic)
                        )
                  ))
        (-> (first stack_number) :item_name)
        ;(first stack_number)
        ))

(defn calculate
    ([^Ignite ignite group_id lst args-dic] (calculate ignite group_id lst [] [] args-dic))
    ([^Ignite ignite group_id [f & r] stack_number stack_symbol args-dic]
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
                                                       (cond (and (has-dic-item args-dic first_item) (has-dic-item args-dic second_item))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (my-dic-item-name args-dic first_item) (my-dic-item-name args-dic second_item)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                             (has-dic-item args-dic first_item) (cond (true? (-> second_item :const)) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) %s)" (-> top_symbol :operation_symbol) (my-dic-item-name args-dic first_item) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                                                      (false? (-> second_item :const)) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (my-dic-item-name args-dic first_item) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                                                      )
                                                             (has-dic-item args-dic second_item) (cond (true? (-> first_item :const)) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (my-dic-item-name args-dic second_item)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                                                       (false? (-> first_item :const)) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (my-dic-item-name args-dic second_item)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                                                       )
                                                             :else
                                                             (cond (and (true? (-> first_item :const)) (true? (-> second_item :item_name))) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                   (true? (-> first_item :const)) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                   (true? (-> second_item :const)) (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                   :else
                                                                   (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) args-dic)
                                                                   )
                                                             ))
                                                   )
               (contains? f :parenthesis) (let [m (calculate ignite group_id (reverse (-> f :parenthesis)) args-dic)]
                                              (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol args-dic))
               (contains? f :operation) (let [m (calculate ignite group_id (reverse (-> f :operation)) args-dic)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol args-dic))
               (contains? f :item_name) (recur ignite group_id r (conj stack_number f) stack_symbol args-dic)
               (contains? f :func-name) (let [m (func-to-clj ignite group_id f args-dic)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol args-dic))
               (contains? f :func-link) (let [m (func-link-to-clj ignite group_id (-> f :func-link) args-dic)]
                                            (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol args-dic))
               :else
               (recur ignite group_id r (conj stack_number f) stack_symbol args-dic)
               )
         (run-express stack_number stack_symbol args-dic)
         )))

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

;(defn add_user_group [ignite group_id ^String group_name ^String user_token ^String group_type ^String schema_name]
;    (.addUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id group_name user_token group_type schema_name))
;
;(defn update_user_group [ignite group_id ^String group_name ^String group_type]
;    (.updateUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id group_name group_type))
;
;(defn delete_user_group [ignite group_id ^String group_name]
;    (.deleteUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id group_name))
;
;(defn has_user_token_type [^String user_token_type]
;    (.hasUserTokenType (.getSmartFuncInit (MySmartFuncInit/getInstance)) user_token_type))
;
;(defn get_user_group [ignite group_id user_token]
;    (.getUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id user_token))
;
;(defn get_user_token [ignite group_name]
;    (.getUserToken (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_name))

; 调用方法这个至关重要
(defn func-to-clj [^Ignite ignite group_id m args-dic]
    (let [{func-name :func-name lst_ps :lst_ps} m]
        (if-let [my-lexical-func (my-lexical/smart-func func-name)]
            (format "(%s %s)" my-lexical-func (get-lst-ps-vs ignite group_id lst_ps args-dic))
            (cond
                  ;(my-lexical/is-eq? "log" func-name) (format "(log %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  ;(my-lexical/is-eq? "println" func-name) (format "(println %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? "println" func-name) (format "(my-lexical/my-show-msg (my-lexical/gson %s))" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  ;(re-find #"\." func-name) (let [{let-name :schema_name method-name :table_name} (my-lexical/get-schema func-name)]
                  ;                              (if (> (count lst_ps) 0)
                  ;                                  (format "(%s (my-lexical/get-value %s) %s)" (my-lexical/smart-func method-name) let-name (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  ;                                  (format "(%s (my-lexical/get-value %s))" (my-lexical/smart-func method-name) let-name))
                  ;                              )
                  ; 系统函数
                  (contains? #{"first" "rest" "next" "second" "last"} (str/lower-case func-name)) (format "(%s %s)" (str/lower-case func-name) (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  ; inner function
                  ;(get-inner-function-context (str/lower-case func-name) args-dic) (format "(%s %s)" func-name (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "query_sql") (cond (= (count lst_ps) 1) (format "(my-smart-db/query_sql ignite group_id %s nil)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                                                                  (> (count lst_ps) 1) (format "(my-smart-db/query_sql ignite group_id %s [%s])" (get-lst-ps-vs ignite group_id (first lst_ps) args-dic) (get-lst-ps-vs ignite group_id (rest lst_ps) args-dic))
                                                                  )
                  (and (contains? args-dic :top-func) (= func-name (-> args-dic :top-func))) (format "(%s ignite group_id %s)" func-name (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "empty?") (format "(empty? %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "notEmpty?") (format "(my-lexical/not-empty? %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (my-lexical/is-eq? func-name "noSqlCreate") (format "(my-lexical/no-sql-create ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlGet") (format "(my-lexical/no-sql-get-vs ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (my-lexical/is-eq? func-name "noSqlInsertTran") (format "(my-lexical/no-sql-insert-tran ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlUpdateTran") (format "(my-lexical/no-sql-update-tran ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlDeleteTran") (format "(my-lexical/no-sql-delete-tran ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlDrop") (format "(my-lexical/no-sql-drop ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlInsert") (format "(my-lexical/no-sql-insert ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlUpdate") (format "(my-lexical/no-sql-update ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "noSqlDelete") (format "(my-lexical/no-sql-delete ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "auto_id") (format "(my-lexical/auto_id ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (my-lexical/is-eq? func-name "trans") (format "(my-smart-db/trans ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "my_view") (format "(smart-func/smart-view ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "rm_view") (format "(smart-func/rm-smart-view ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (my-lexical/is-eq? func-name "add_scenes_to") (format "(smart-func/add-scenes-to ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "rm_scenes_from") (format "(smart-func/rm-scenes-from ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (my-lexical/is-eq? func-name "add_job") (format "(smart-func/add-job ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "remove_job") (format "(smart-func/remove-job ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "job_snapshot") (format "(smart-func/get-job-snapshot ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (my-lexical/is-eq? func-name "add_func") (format "(smart-func/add_func ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "remove_func") (format "(smart-func/remove_func ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "recovery_to_cluster") (format "(smart-func/recovery-to-cluster ignite %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  ; 机器学习
                  (my-lexical/is-eq? func-name "create_train_matrix") (format "(my-ml-train-data/create-train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "has_train_matrix") (format "(my-ml-train-data/has-train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "drop_train_matrix") (format "(my-ml-train-data/drop-train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "train_matrix") (format "(my-ml-train-data/train-matrix ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "train_matrix_single") (format "(my-ml-train-data/train-matrix-single ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "fit") (format "(my-ml-func/ml-fit ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "predict") (format "(my-ml-func/ml-predict ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic))

                  (is-func? ignite func-name) (if-not (empty? lst_ps)
                                                  (format "(my-smart-scenes/my-invoke-func ignite \"%s\" [%s])" func-name (get-lst-ps-vs ignite group_id lst_ps args-dic))
                                                  (format "(my-smart-scenes/my-invoke-func-no-ps ignite \"%s\")" func-name))
                  (is-scenes? ignite group_id func-name) (if-not (empty? lst_ps)
                                                             (format "(my-smart-scenes/my-invoke-scenes ignite group_id \"%s\" [%s])" func-name (get-lst-ps-vs ignite group_id lst_ps args-dic))
                                                             (format "(my-smart-scenes/my-invoke-scenes-no-ps ignite group_id \"%s\")" func-name))
                  (is-call-scenes? ignite group_id func-name) (if-not (empty? lst_ps)
                                                                  (format "(my-smart-scenes/my-invoke-scenes ignite group_id \"%s\" [%s])" func-name (get-lst-ps-vs ignite group_id lst_ps args-dic))
                                                                  (format "(my-smart-scenes/my-invoke-scenes-no-ps ignite group_id \"%s\")" func-name))

                  (my-lexical/is-eq? func-name "loadCode") (.loadSmartSql (.getLoadSmartSql (MyLoadSmartSqlService/getInstance)) ignite group_id (-> (first lst_ps) :item_name))
                  (my-lexical/is-eq? func-name "has_user_token_type") (format "(my-smart-token-clj/has_user_token_type %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic)) ;(.hasUserTokenType (.getSmartFuncInit (MySmartFuncInit/getInstance)) (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "get_user_group") (format "(my-smart-token-clj/get_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic)) ;(.getUserGroup (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite group_id (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "get_user_token") (format "(my-smart-token-clj/get_user_token ignite %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic)) ;(.getUserToken (.getSmartFuncInit (MySmartFuncInit/getInstance)) ignite (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  (my-lexical/is-eq? func-name "add_user_group") (format "(my-smart-token-clj/add_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic));(apply add_user_group (concat [ignite group_id] (eval (read-string (format "[%s]" (get-lst-ps-vs ignite group_id lst_ps args-dic))))))
                  (my-lexical/is-eq? func-name "update_user_group") (format "(my-smart-token-clj/update_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic));(apply update_user_group (concat [ignite group_id] (eval (read-string (format "[%s]" (get-lst-ps-vs ignite group_id lst_ps args-dic))))))
                  (my-lexical/is-eq? func-name "delete_user_group") (format "(my-smart-token-clj/delete_user_group ignite group_id %s)" (get-lst-ps-vs ignite group_id lst_ps args-dic));(delete_user_group ignite group_id (get-lst-ps-vs ignite group_id lst_ps args-dic))
                  :else
                  (throw (Exception. (format "%s 不存在，或没有权限！" func-name)))
                  ))
        ))

; 为每一个参数添加 my-lexical/get-value
(defn get-lst-ps-vs
    ([ignite group_id lst args-dic] (get-lst-ps-vs ignite group_id lst args-dic []))
    ([ignite group_id [f & r] args-dic lst]
     (if (some? f)
         (if-not (contains? f :comma_symbol)
             (let [m-line (my-lexical/my-str-value (token-to-clj ignite group_id f args-dic))]
                 (if (and (contains? f :item_name) (false? (-> f :const)) (not (re-find #"^\(my-lexical/get-value\s" m-line)))
                     (recur ignite group_id r args-dic (conj lst (format "(my-lexical/get-value %s)" m-line)))
                     (recur ignite group_id r args-dic (conj lst m-line))))
             (recur ignite group_id r args-dic lst))
         (str/trim (str/join " " lst)))))

(defn item-to-clj [m args-dic]
    (cond (my-lexical/is-eq? (-> m :item_name) "null") "nil"
          (false? (-> m :const)) (cond (contains? (-> args-dic :dic) (-> m :item_name)) (let [vs-obj (get (-> args-dic :dic) (-> m :item_name))]
                                                                                            (if (= java.lang.String (last vs-obj))
                                                                                                (format "(my-lexical/get-value \"%s\")" (first vs-obj))
                                                                                                (format "(my-lexical/get-value %s)" (first vs-obj)))
                                                                                            )
                                       (contains? (-> args-dic :dic) (str/lower-case (-> m :item_name))) (let [vs-obj (get (-> args-dic :dic) (str/lower-case (-> m :item_name)))]
                                                                                                             (if (= java.lang.String (last vs-obj))
                                                                                                                 (format "(my-lexical/get-value \"%s\")" (first vs-obj))
                                                                                                                 (format "(my-lexical/get-value %s)" (first vs-obj)))
                                                                                                             )
                                       :else
                                       (format "(my-lexical/get-value %s)" (-> m :item_name)))
          (and (= java.lang.String (-> m :java_item_type)) (true? (-> m :const))) (my-lexical/my-str-value (-> m :item_name))
          (and (not (= java.lang.String (-> m :java_item_type))) (true? (-> m :const))) (my-lexical/get-java-items-vs m)
          :else
          (-> m :item_name)
          ))

;(defn judge [ignite group_id lst args-dic]
;    (cond (= (count lst) 3) (format "(%s %s %s)" (str/lower-case (-> (second lst) :and_or_symbol)) (token-to-clj ignite group_id (first lst) args-dic) (token-to-clj ignite group_id (last lst) args-dic))
;          (> (count lst) 3) (let [top (judge ignite group_id (take 3 lst) args-dic)]
;                                (judge ignite group_id (concat [top] (drop 3 lst)) args-dic))
;          :else
;          (throw (Exception. "判断语句错误！"))
;          ))

(defn judge [ignite group_id lst args-dic]
    (let [[f s t & rs] lst]
        (if (and (not (empty? f)) (not (empty? s)) (not (empty? t)))
            (let [rs-line (format "(%s %s %s)" (str/lower-case (-> s :and_or_symbol)) (token-to-clj ignite group_id f args-dic) (token-to-clj ignite group_id t args-dic))]
                (if (nil? rs)
                    rs-line
                    (judge ignite group_id (concat [rs-line] rs) args-dic)))
            (throw (Exception. "判断语句错误！")))))

(defn parenthesis-to-clj [ignite group_id m args-dic]
    (cond (> (count (filter #(and (map? %) (contains? % :comma_symbol)) (-> m :parenthesis))) 0) (loop [[f & r] (filter #(and (map? %) (contains? % :comma_symbol)) (-> m :parenthesis)) lst []]
                                                                                                     (if (some? f)
                                                                                                         (recur r (conj lst (token-to-clj ignite group_id f args-dic)))
                                                                                                         (str/join " " lst)))
          (and (> (count (filter #(and (map? %) (contains? % :comparison_symbol)) (-> m :parenthesis))) 0) (= (count (-> m :parenthesis)) 3)) (format "(%s (my-lexical/get-value %s) (my-lexical/get-value %s))" (-> (second (-> m :parenthesis)) :comparison_symbol) (token-to-clj ignite group_id (first (-> m :parenthesis)) args-dic) (token-to-clj ignite group_id (last (-> m :parenthesis)) args-dic))
          (and (>= (count (-> m :parenthesis)) 3) (contains? (second (-> m :parenthesis)) :and_or_symbol)) (judge ignite group_id (-> m :parenthesis) args-dic)
          :else
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
        (cond (contains? m :func-name) (func-to-clj ignite group_id m args-dic)
              ;(and (contains? m :func-name) (contains? m :lst_ps)) (func-to-clj ignite group_id m args-dic)
              ;(contains? m :func-link) (func-link-to-clj ignite group_id (reverse (-> m :func-link)) args-dic)
              (contains? m :func-link) (func-link-to-clj ignite group_id (-> m :func-link) args-dic)
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
    (loop [[f & r] m sb (StringBuilder.)]
        (if (some? f)
            (if (and (contains? (-> f :key) :item_name) (nil? (-> f :key :java_item_type)) (false? (-> f :key :const)))
                (recur r (doto sb (.append (format "(.put %s %s)" (-> f :key :item_name) (token-to-clj ignite group_id (-> f :value) args-dic)))))
                (recur r (doto sb (.append (format "(.put %s %s)" (token-to-clj ignite group_id (-> f :key) args-dic) (token-to-clj ignite group_id (-> f :value) args-dic)))))
                )
            (format "(doto (Hashtable.)\n    %s)" (.toString sb)))))

;(defn my-item-to-clj [m args-dic]
;    (if (contains? (-> args-dic :dic) (-> m :item_name))
;        (eval (read-string (format "(my-lexical/get-value %s)" (get (-> args-dic :dic) (-> m :item_name)))))
;        (if (my-lexical/is-eq? (-> m :item_name) "null")
;            nil
;            (eval (read-string (-> m :item_name))))))

(defn my-item-to-clj [m args-dic]
    (cond (contains? (-> args-dic :dic) (-> m :item_name))  (let [vs-obj (get (-> args-dic :dic) (-> m :item_name))]
                                                                (if (= java.lang.String (last vs-obj))
                                                                    (eval (read-string (format "(my-lexical/get-value \"%s\")" (first vs-obj))))
                                                                    (eval (read-string (format "(my-lexical/get-value %s)" (first vs-obj)))))
                                                                ) ;(get (-> args-dic :dic) (-> m :item_name))
          (contains? (-> args-dic :dic) (str/lower-case (-> m :item_name))) (let [vs-obj (get (-> args-dic :dic) (str/lower-case (-> m :item_name)))]
                                                                                (if (= java.lang.String (last vs-obj))
                                                                                    (eval (read-string (format "(my-lexical/get-value \"%s\")" (first vs-obj))))
                                                                                    (eval (read-string (format "(my-lexical/get-value %s)" (first vs-obj)))))
                                                                                );(get (-> args-dic :dic) (str/lower-case (-> m :item_name)))
          :else
          (if (my-lexical/is-eq? (-> m :item_name) "null")
              nil
              (my-lexical/get-java-items-vs m))))

(defn get-const-vs [vs]
    (cond (and (= (first vs) \') (= (last vs) \')) (str/join (concat ["\""] (drop-last (rest vs)) ["\""])) ;(str/join (rest (drop-last 1 vs)))
          ;(and (= (first vs) "\"") (= (last vs) "\"")) (str/join (rest (drop-last 1 vs)))
          (and (= (first vs) \') (= (last vs) \')) (str/join (concat ["\""] (drop-last (rest vs)) ["\""])) ;(str/join (drop-last (rest vs)))
          ;(and (= (first vs) \") (= (last vs) \")) (str/join (drop-last (rest vs)))
          :else
          vs
          ))

(defn get-vals
    ([lst] (get-vals lst []))
    ([[f & r] lst]
     (if (some? f)
         (recur r (conj lst (first f)))
         lst)))

; args-dic 的终极使用在这里，通过匿名函数来替换真正的值
(defn func-token-to-clj [ignite group_id m args-dic]
    (if (contains? m :item_name)
        (my-item-to-clj m args-dic)
        (let [fn-line (token-to-clj ignite group_id m args-dic)]
            (if-not (Strings/isNullOrEmpty fn-line)
                (apply (eval (read-string (format "(fn [ignite group_id %s]\n     %s)" (str/join " " (keys (-> args-dic :dic))) fn-line))) (concat [ignite group_id] (get-vals (vals (-> args-dic :dic))))))))
    )

(defn func-link-to-clj [ignite group_id [f & r] args-dic]
    (cond (and (some? f) (or (nil? r) (empty? r))) (token-clj ignite group_id f args-dic)
          (and (some? f) (some? r)) (let [first-item (token-clj ignite group_id f args-dic) next-item (first r)]
                                        (let [m (assoc next-item :lst_ps (concat [{:table_alias "", :item_name first-item, :item_type "", :java_item_type nil, :const false}] (-> next-item :lst_ps)))]
                                            (func-link-to-clj ignite group_id (concat [m] (rest r)) args-dic)))
          ))

(defn invoke-func-link-to-clj [ignite group_id [f & r]]
    (cond (and (some? f) (or (nil? r) (empty? r))) (token-clj ignite group_id f nil)
          (and (some? f) (some? r)) (let [first-item (token-clj ignite group_id f nil) next-item (first r)]
                                        (let [m (assoc next-item :lst_ps (concat [{:table_alias "", :item_name first-item, :item_type "", :java_item_type nil, :const false}] (-> next-item :lst_ps)))]
                                            (func-link-to-clj ignite group_id (concat [m] (rest r)) nil)))
          ))
