(ns org.gridgain.plus.dml.my-smart-clj
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-smart-sql :as my-smart-sql]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.dml.my-smart-token-clj :as my-smart-token-clj]
        [org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
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
             (org.log MyCljLogger)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartClj
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare ast-to-clj body-to-clj token-to-clj token-lst-clj for-seq for-seq-func
         express-to-clj query_sql trans do-express)

(defn do-express [f-express r-express]
    (if (and (some? f-express) (some? r-express) (contains? f-express :expression) (contains? #{"for" "match"} (-> f-express :expression)))
        true false))

(defn query_sql [ignite group_id sql & args]
    (my-smart-db/query_sql ignite group_id sql args))

(defn trans [ignite group_id sql & args]
    (my-smart-db/trans ignite group_id sql args))

(defn myInsert [ignite group_id m]
    (my-smart-db/my-insert ignite group_id m))

(defn myInsertTran [ignite group_id m]
    (my-smart-db/my-insert-tran ignite group_id m))

(defn myUpdate [ignite group_id m]
    (my-smart-db/my-update ignite group_id m))

(defn myUpdateTran [ignite group_id m]
    (my-smart-db/my-update-tran ignite group_id m))

(defn myDelete [ignite group_id m]
    (my-smart-db/my-delete ignite group_id m))

(defn myDeleteTran [ignite group_id m]
    (my-smart-db/my-delete-tran ignite group_id m))

(defn myDrop [ignite group_id m]
    (my-smart-db/my-drop ignite group_id m))

(defn contains-context? [my-context token-name]
    (cond (contains? (-> my-context :input-params) token-name) true
          (some? (my-smart-token-clj/get-let-context token-name my-context)) true
          :else
          (if-not (nil? (-> my-context :up-my-context))
              (contains-context? (-> my-context :up-my-context) token-name)
              false)
          ))

(defn get-my-it [my-context]
    (if-let [m (format "M-F-%s-I-%s-c-Y" (gensym "v") (gensym "Q"))]
        (if (contains-context? my-context m)
            (get-my-it my-context)
            m)))

(defn get-my-let [my-context]
    (if-let [m (format "c-F-%s-w-%s-c-Y" (gensym "W") (gensym "G"))]
        (if (contains-context? my-context m)
            (get-my-let my-context)
            m)))

; 判断 token 中是否 table item name 是否存在
(defn is-exist-in-token? [item_name token]
    (letfn [(is-exist-lst-token? [item_name [f-token & r-token]]
                (if (some? f-token)
                    (if (true? (is-exist-token? item_name f-token))
                        true
                        (recur item_name r-token))
                    false))
            (is-exist-token? [item_name token]
                (cond (map? token) (loop [[f & r] (keys token)]
                                       (if (some? f)
                                           (cond (and (= f :item_name) (my-lexical/is-eq? item_name (-> token :item_name))) true
                                                 (map? (-> token f)) (if (true? (is-exist-token? item_name (-> token f)))
                                                                         true
                                                                         (recur r))
                                                 (my-lexical/is-seq? (-> token f)) (if (true? (is-exist-lst-token? item_name (-> token f)))
                                                                                                                          true
                                                                                                                          (recur r))
                                                 :else
                                                 (recur r)
                                                 )
                                           false))
                      (and (my-lexical/is-seq? token) (not (empty? token))) (if (true? (is-exist-lst-token? item_name token))
                                                                          true
                                                                          false)
                      :else
                      false
                    ))]
        (is-exist-token? item_name token)))

(defn token-to-clj [ignite group_id m my-context]
    (my-smart-token-clj/token-to-clj ignite group_id m my-context))

; 处于同一层就返回 true 否则 false
(defn is-same-layer? [f r]
    (if (false? (is-exist-in-token? (-> f :let-name) r))
        true
        false))

(defn in-same-layer? [[f & r] new-let-token]
    (if (some? f)
        (if (false? (is-same-layer? f new-let-token))
            false
            (recur r new-let-token))
        true))

(defn letLayer-to-clj
    ([^MyLetLayer letLayer] (letLayer-to-clj letLayer [] []))
    ([^MyLetLayer letLayer lst-head lst-tail]
     (if-not (nil? letLayer)
         (recur (.getUpLayer letLayer) (conj lst-head (format "(let [%s]" (str/join " " (.getLst letLayer)))) (conj lst-tail ")"))
         [(str/join " " (reverse lst-head)) (str/join lst-tail)])))

(defn let-to-clj
    ([ignite group_id lst-let my-context] (let-to-clj ignite group_id lst-let my-context (MyLetLayer.) []))
    ([ignite group_id [f & r] my-context letLayer up-lst]
     (if (some? f)
         (if (in-same-layer? up-lst f)
             (recur ignite group_id r (my-smart-token-clj/add-let-to-context (-> f :let-name) (-> f :let-vs) my-context) (.addLet letLayer (format "%s (MyVar. %s)" (-> f :let-name) (token-to-clj ignite group_id (-> f :let-vs) my-context))) (conj up-lst f))
             (recur ignite group_id r (my-smart-token-clj/add-let-to-context (-> f :let-name) (-> f :let-vs) my-context) (MyLetLayer. (doto (ArrayList.) (.add (format "%s (MyVar. %s)" (-> f :let-name) (token-to-clj ignite group_id (-> f :let-vs) my-context)))) letLayer) [f])
             )
         (conj (letLayer-to-clj letLayer) my-context))))

(defn for-seq [ignite group_id f my-context]
    (let [tmp-val-name (-> f :args :tmp_val :item_name) seq-name (-> f :args :seq :item_name) my-it (get-my-it my-context)]
        (let [my-context-1 (assoc my-context :let-params (merge (-> my-context :let-params) {tmp-val-name nil} {my-it nil}) :up-stm "for")]
            (let [for-inner-clj (body-to-clj ignite group_id (-> f :body) my-context-1) loop-r (gensym "loop-r")]
                (format "(cond (instance? Iterator %s) (loop [%s %s]\n
                                                       (if (.hasNext %s)\n
                                                           (let [%s (.next %s)]\n
                                                               %s\n
                                                               (recur %s)\n
                                                               )))\n
                        (my-lexical/is-seq? %s) (loop [[%s & %s] %s]\n
                                         (if (some? %s)\n
                                             (do\n
                                                  %s\n
                                             (recur %s))))\n
                        :else\n
                        (throw (Exception. \"for 循环只能处理列表或者是执行数据库的结果\"))\n
                        )"
                        seq-name my-it seq-name
                        my-it
                        tmp-val-name my-it
                        for-inner-clj
                        my-it
                        seq-name tmp-val-name loop-r seq-name
                        tmp-val-name
                        for-inner-clj
                        loop-r)
                ))
        ))

(defn for-seq-func [ignite group_id f my-context]
    (let [tmp-val-name (-> f :args :tmp_val :item_name) seq-name (get-my-let my-context) my-it (get-my-it my-context) func-clj (token-to-clj ignite group_id (-> f :args :seq) my-context)]
        (let [my-context-1 (assoc my-context :let-params (merge (-> my-context :let-params) {tmp-val-name nil} {my-it nil}) :up-stm "for")]
            (let [for-inner-clj (body-to-clj ignite group_id (-> f :body) my-context-1) loop-r (gensym "loop-r")]
                (format "(let [%s %s]\n
                          (cond (instance? Iterator %s) (loop [%s %s]\n
                                                              (if (.hasNext %s)\n
                                                                  (let [%s (.next %s)]\n
                                                                       %s\n
                                                                       (recur %s)\n
                                                                       )))\n
                                (my-lexical/is-seq? %s) (loop [[%s & %s] %s]\n
                                                  (if (some? %s)\n
                                                      (do\n
                                                           %s\n
                                                      (recur %s))))\n
                                :else\n
                                (throw (Exception. \"for 循环只能处理列表或者是执行数据库的结果\"))\n
                                ))"
                        seq-name func-clj
                        seq-name my-it seq-name
                        my-it
                        tmp-val-name my-it
                        for-inner-clj
                        my-it
                        seq-name tmp-val-name loop-r seq-name
                        tmp-val-name
                        for-inner-clj
                        loop-r)))
        ))

;(defn match-to-clj
;    ([ignite group_id lst-pair my-context] (match-to-clj ignite group_id lst-pair my-context []))
;    ([ignite group_id [f-pair & r-pair] my-context lst]
;     (if (some? f-pair)
;         (cond (contains? f-pair :pair) (if (contains? (-> f-pair :pair) :parenthesis)
;                                            (cond (map? (-> f-pair :pair-vs)) (let [pair-line (format "%s %s" (express-to-clj ignite group_id [(-> f-pair :pair)] my-context) (body-to-clj ignite group_id [(-> f-pair :pair-vs)] my-context))]
;                                                                                  (recur ignite group_id r-pair my-context (conj lst pair-line)))
;                                                  (my-lexical/is-seq? (-> f-pair :pair-vs)) (if (> (count (-> f-pair :pair-vs)) 1)
;                                                                                                (let [pair-line (format "%s (do \n    %s)" (express-to-clj ignite group_id [(-> f-pair :pair)] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
;                                                                                                    (recur ignite group_id r-pair my-context (conj lst pair-line)))
;                                                                                                (let [pair-line (format "%s %s" (express-to-clj ignite group_id [(-> f-pair :pair)] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
;                                                                                                    (recur ignite group_id r-pair my-context (conj lst pair-line))))
;                                                  )
;                                            (cond (map? (-> f-pair :pair-vs)) (let [pair-line (format "%s %s" (express-to-clj ignite group_id [{:parenthesis (-> f-pair :pair)}] my-context) (body-to-clj ignite group_id [(-> f-pair :pair-vs)] my-context))]
;                                                                                  (recur ignite group_id r-pair my-context (conj lst pair-line)))
;                                                  (my-lexical/is-seq? (-> f-pair :pair-vs)) (if (> (count (-> f-pair :pair-vs)) 1)
;                                                                                                (let [pair-line (format "%s (do \n    %s)" (express-to-clj ignite group_id [{:parenthesis (-> f-pair :pair)}] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
;                                                                                                    (recur ignite group_id r-pair my-context (conj lst pair-line)))
;                                                                                                (let [pair-line (format "%s %s" (express-to-clj ignite group_id [{:parenthesis (-> f-pair :pair)}] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
;                                                                                                    (recur ignite group_id r-pair my-context (conj lst pair-line))))
;                                                  )
;                                            )
;               (contains? f-pair :else-vs) (recur ignite group_id r-pair my-context (conj lst (format ":else %s" (body-to-clj ignite group_id (-> f-pair :else-vs) my-context))))
;               )
;         (str/join "\n          " lst))))

(defn match-to-clj
    ([ignite group_id lst-pair my-context] (match-to-clj ignite group_id lst-pair my-context []))
    ([ignite group_id [f-pair & r-pair] my-context lst]
     (if (some? f-pair)
         (cond (contains? f-pair :pair) (cond (and (map? (-> f-pair :pair)) (or (contains? (-> f-pair :pair) :parenthesis) (contains? (-> f-pair :pair) :func-name))) (if (> (count (-> f-pair :pair-vs)) 1)
                                                                                                                                                                          (let [pair-line (format "%s (do \n    %s)" (express-to-clj ignite group_id [(-> f-pair :pair)] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
                                                                                                                                                                              (recur ignite group_id r-pair my-context (conj lst pair-line)))
                                                                                                                                                                          (let [pair-line (format "%s %s" (express-to-clj ignite group_id [(-> f-pair :pair)] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
                                                                                                                                                                              (recur ignite group_id r-pair my-context (conj lst pair-line))))
                                              :else
                                              (if (> (count (-> f-pair :pair-vs)) 1)
                                                  (let [pair-line (format "%s (do \n    %s)" (express-to-clj ignite group_id [{:parenthesis (-> f-pair :pair)}] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
                                                      (recur ignite group_id r-pair my-context (conj lst pair-line)))
                                                  (let [pair-line (format "%s %s" (express-to-clj ignite group_id [{:parenthesis (-> f-pair :pair)}] my-context) (body-to-clj ignite group_id (-> f-pair :pair-vs) my-context))]
                                                      (recur ignite group_id r-pair my-context (conj lst pair-line))))
                                              )
               (contains? f-pair :else-vs) (recur ignite group_id r-pair my-context (conj lst (format ":else %s" (body-to-clj ignite group_id (-> f-pair :else-vs) my-context))))
               )
         (str/join "\n          " lst))))

(defn get-inner-func-name [funcs-express]
    (loop [[f & r] funcs-express lst-func-name []]
        (if (some? f)
            (recur r (conj lst-func-name (-> f :func-name)))
            lst-func-name)))

(defn add-inner-to-context [my-context funcs-express]
    (let [inner-func (-> my-context :inner-func)]
        (assoc my-context :inner-func (apply conj inner-func (get-inner-func-name funcs-express)))))

(defn inner-functions [ignite group_id funcs-express my-context]
    (loop [[f & r] funcs-express lst [] inner-context (add-inner-to-context my-context funcs-express)]
        (if (some? f)
            (recur r (conj lst (ast-to-clj ignite group_id f inner-context)) inner-context)
            (str/join " " lst))))

; 表达式 to clj
(defn express-to-clj
    ([ignite group_id lst-express my-context] (express-to-clj ignite group_id lst-express my-context []))
    ([ignite group_id [f-express & r-express] my-context lst]
     (if (some? f-express)
         (cond (and (contains? f-express :expression) (my-lexical/is-eq? (-> f-express :expression) "for")) (cond (and (contains? (-> f-express :args :tmp_val) :item_name) (contains? (-> f-express :args :seq) :item_name)) (recur ignite group_id r-express my-context (conj lst (for-seq ignite group_id f-express my-context)))
                                                                                                                  (and (contains? (-> f-express :args :tmp_val) :item_name) (contains? (-> f-express :args :seq) :func-name)) (recur ignite group_id r-express my-context (conj lst (for-seq-func ignite group_id f-express my-context)))
                                                                                                                  :else
                                                                                                                  (throw (Exception. "for 语句只能处理数据库结果或者是列表"))
                                                                                                                  )
               (and (contains? f-express :expression) (my-lexical/is-eq? (-> f-express :expression) "match")) (recur ignite group_id r-express my-context (conj lst (format "(cond %s)" (match-to-clj ignite group_id (-> f-express :pairs) my-context))))
               ; 内置方法
               (contains? f-express :functions) (let [inner-func-line (inner-functions ignite group_id (-> f-express :functions) my-context)]
                                                    (format "(letfn [%s] \n    %s)" inner-func-line (express-to-clj ignite group_id r-express (add-inner-to-context my-context (-> f-express :functions)))))
               (contains? f-express :express) (recur ignite group_id r-express my-context (conj lst (token-to-clj ignite group_id (-> f-express :express) my-context)))
               (contains? f-express :let-name) (let [[let-first let-tail let-my-context] (let-to-clj ignite group_id [f-express] my-context)]
                                                   (let [express-line (express-to-clj ignite group_id r-express let-my-context)]
                                                       (format "%s %s %s" let-first express-line let-tail)))
               (contains? f-express :break-vs) (if (contains? my-context :up-stm)
                                                   (recur ignite group_id r-express my-context (conj lst "(recur nil)"))
                                                   (throw (Exception. "break 语句只能用于 for 语句块中！"))
                                                   )
               :else
               (recur ignite group_id r-express my-context (conj lst (token-to-clj ignite group_id f-express my-context)))
               )
         (str/join "\n   " lst))))

(defn body-to-clj
    ([ignite group_id lst my-context] (body-to-clj ignite group_id lst my-context []))
    ([ignite group_id [f & r] my-context lst-rs]
     (if (some? f)
         (cond (contains? f :let-name) (recur ignite group_id r my-context (conj lst-rs f))
               (and (not (empty? lst-rs)) (not (contains? f :let-name))) (if (nil? r)
                                                                             (let [[let-first let-tail let-my-context] (let-to-clj ignite group_id lst-rs my-context)]
                                                                                 (let [express-line (express-to-clj ignite group_id [f] let-my-context)]
                                                                                     (format "%s %s %s" let-first express-line let-tail)))
                                                                             (let [[let-first let-tail let-my-context] (let-to-clj ignite group_id lst-rs my-context)]
                                                                                 (let [express-line (express-to-clj ignite group_id (concat [f] r) let-my-context)]
                                                                                     (format "%s (do\n    %s) %s" let-first express-line let-tail))
                                                                                 ))
               :else
               (let [express-line (express-to-clj ignite group_id (concat [f] r) my-context)]
                   (if (true? (do-express f r))
                       (format "(do\n    %s)" express-line)
                       express-line))
             ))))

; my-context 记录上下文
; :input-params 输入参数
; :let-params 定义变量
; :last-item 上一个 token
; :inner-func inner-func 名字
; :up-my-context 上一层的 my-context
; my-context: {:input-params #{} :let-params {} :last-item nil :up-my-context nil}
(defn ast-to-clj [ignite group_id ast up-my-context]
    (let [{func-name :func-name args-lst :args-lst body-lst :body-lst} ast my-context {:input-params #{} :let-params {} :last-item nil :inner-func #{} :up-my-context up-my-context}]
        (let [func-context (assoc my-context :input-params (apply conj (-> my-context :input-params) args-lst))]
            (if (nil? up-my-context)
                (if-not (empty? args-lst)
                    (format "(defn %s [^Ignite ignite ^Long group_id %s]\n    %s)" func-name (str/join " " args-lst) (body-to-clj ignite group_id body-lst (assoc func-context :top-func func-name)))
                    (format "(defn %s [^Ignite ignite ^Long group_id]\n    %s)" func-name (body-to-clj ignite group_id body-lst (assoc func-context :top-func func-name))))
                (format "(%s [%s]\n    %s)" func-name (str/join " " args-lst) (body-to-clj ignite group_id body-lst func-context)))
            )
        ))

(defn smart-to-clj [^Ignite ignite ^Long group_id ^String smart-sql]
    (let [code (ast-to-clj ignite group_id (first (my-smart-sql/get-ast smart-sql)) nil)]
        (if (re-find #"^\(defn\s*" code)
            code
            (str/replace code #"^\(\s*" "(defn "))
        ))

