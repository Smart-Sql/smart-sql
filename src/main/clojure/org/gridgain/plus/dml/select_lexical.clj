(ns org.gridgain.plus.dml.select-lexical
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.apache.ignite.transactions Transaction)
             (com.google.gson Gson GsonBuilder)
             (org.tools MyConvertUtil KvSql)
             (cn.plus.model MyLogCache SqlType)
             (cn.mysuper.model MyUrlToken)
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (cn.plus.model.db MyCallScenesPk MyCallScenes MyScenesCache MyScenesCachePk MyScenesParams)
             (org.gridgain.myservice MyNoSqlFunService)
             (org.gridgain.jdbc MyJdbc)
             (org.gridgain.smart.view MyViewAstPK)
             (java.util ArrayList Date Iterator Random Hashtable)
             (java.sql Timestamp)
             (org.tools MyTools MyFunction)
             (java.math BigDecimal)
             (com.google.common.base Strings))
    (:gen-class
        ;:implements [org.gridgain.superservice.INoSqlFun]
        ; 生成 class 的类名f
        :name org.gridgain.plus.dml.MyLexical
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_url_tokens [String] cn.mysuper.model.MyUrlToken]
                  ^:static [isJdbcThin [String] Boolean]
                  ^:static [hasConnPermission [String] Boolean]
                  ^:static [getStrValue [String] String]
                  ^:static [lineToList [String] java.util.List]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(declare get-schema)

(defn get-scenes-params [params]
    (if (nil? params)
        (ArrayList.)
        params))

(defn gson [m]
    (cond (or (string? m) (integer? m) (double? m) (boolean? m)) m
          :else (let [gs (.create (.setDateFormat (.enableComplexMapKeySerialization (GsonBuilder.)) "yyyy-MM-dd HH:mm:ss"))]
                    (.toJson gs m))))

; 判断字符串不为空
(defn is-str-not-empty? [^String line]
    (if-not (Strings/isNullOrEmpty line)
        true false))

(defn is-str-empty? [^String line]
    (Strings/isNullOrEmpty line))


; 判断 func
(defn is-func? [^Ignite ignite ^String func-name]
    (.containsKey (.cache ignite "my_func") (str/lower-case func-name)))

; 判断 scenes
(defn is-scenes? [^Ignite ignite group_id ^String scenes-name]
    (.containsKey (.cache ignite "my_scenes") (MyScenesCachePk. (first group_id) (str/lower-case scenes-name))))

(defn is-call-scenes? [^Ignite ignite group_id ^String scenes-name]
    (.containsKey (.cache ignite "call_scenes") (MyCallScenesPk. (first group_id) scenes-name)))

; 添加参数
(defn add-scenes-ps [^Ignite ignite group_id my-method-name index ps-type]
    (let [pk-id (MyScenesCachePk. (first group_id) my-method-name)]
        (let [m (.get (.cache ignite "my_scenes") pk-id)]
            (let [params (doto (get-scenes-params (.getParams m))
                             (.add (MyScenesParams. ps-type (MyConvertUtil/ConvertToInt index))))]
                (.put (.cache ignite "my_scenes") pk-id (doto m (.setParams params)))))))

; 删除参数
(defn remove-scenes-ps [^Ignite ignite group_id my-method-name index]
    (letfn [(remove-params-index [params index]
                (if (or (nil? params) (= (count params) 0))
                    (throw (Exception. "没有参数！"))
                    (loop [[f & r] params lst (ArrayList.)]
                        (if (some? f)
                            (if (= (.getPs_index f) index)
                                (recur r lst)
                                (recur r (doto lst (.add f))))
                            lst)))
                )]
        (let [pk-id (MyScenesCachePk. (first group_id) my-method-name)]
            (let [m (.get (.cache ignite "my_scenes") pk-id)]
                (.put (.cache ignite "my_scenes") pk-id (doto m (.setParams (remove-params-index (.getParams m) (MyConvertUtil/ConvertToInt index))))))))
    )

; 替换参数
(defn replace-scenes-ps [^Ignite ignite group_id my-method-name index ps-type]
    (letfn [(replace-params-index [params index ps-type]
                (if (or (nil? params) (= (count params) 0))
                    (throw (Exception. "没有参数！"))
                    (loop [[f & r] params lst (ArrayList.)]
                        (if (some? f)
                            (if (= (.getPs_index f) index)
                                (recur r (doto lst (.add (MyScenesParams. ps-type index))))
                                (recur r (doto lst (.add f))))
                            lst)))
                )]
        (let [pk-id (MyScenesCachePk. (first group_id) my-method-name)]
            (let [m (.get (.cache ignite "my_scenes") pk-id)]
                (.put (.cache ignite "my_scenes") pk-id (doto m (.setParams (replace-params-index (.getParams m) (MyConvertUtil/ConvertToInt index) ps-type))))))))

; 输入场景（方法的名字）实际参，输出 dic
; 在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是
; dic = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
; lst_ps 是实参的列表 ["吴大富" 123 true] 参数类型 ArrayList
(defn get_scenes_dic [^MyScenesCache sences lst_ps]
    (if-let [lst_paras (.getParams sences)]
        (if (= (count lst_ps) (count lst_paras))
            (loop [[f_ps & r_ps] lst_ps [f_paras & r_paras] lst_paras dic {}]
                (if (and (some? f_ps) (some? f_paras))
                    (recur r_ps r_paras (assoc dic (.getPs_name f_paras) {:value f_ps :java_type (.getPs_type f_paras)}))
                    dic))
            (throw (Exception. "参数输入的个数和要求的参数个数不一致！")))
        )
    )

; 在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
; name 字符串
(defn get_dic_vs [^clojure.lang.PersistentArrayMap dic_paras ^String name]
    (if (and (= (first name) \:) (contains? dic_paras (str/join (rest name))))
        (letfn [(str_item_value [item_value]
                    (if (or (and (= (first item_value) \') (= (last item_value) \')) (and (= (first item_value) \") (= (last item_value) \")))
                        ;(str/join (reverse (rest (reverse (rest item_value)))))
                        (str/join (drop-last (rest item_value)))
                        ))]
            (let [{value :value type :type} (get dic_paras (str/join (rest name)))]
                (cond (= Integer type) (MyConvertUtil/ConvertToInt value)
                      (= String type) (str_item_value value)
                      (= Boolean type) (MyConvertUtil/ConvertToBoolean value)
                      (= Long type) (MyConvertUtil/ConvertToLong value)
                      (= Double type) (MyConvertUtil/ConvertToDouble value)
                      (= Timestamp type) (MyConvertUtil/ConvertToTimestamp value)
                      (= BigDecimal type) (MyConvertUtil/ConvertToDecimal value)
                      (= "byte[]" type) (MyConvertUtil/ConvertToByte value)
                      (= Object type) value
                      )))
        name))

(defn get-value [m]
    (if (instance? MyVar m)
        (get-value (.getVar m))
        m))

(defn get_obj_schema_name [^Ignite ignite group_id my-obj]
    (if-let [vs-obj (get-value my-obj)]
        (if (contains? vs-obj "table_name")
            (let [{schema_name :schema_name table_name :table_name} (get-schema (get vs-obj "table_name"))]
                (if (= schema_name "")
                    {:schema_name (second group_id) :table_name table_name}
                    {:schema_name schema_name :table_name table_name})))))

; 剔除单括号或双括号
(defn get_str_value [^String line]
    (cond (and (= (first line) \') (= (last line) \')) (str/join (concat ["\""] (drop-last (rest line)) ["\""]))
          (and (= (first line) \") (= (last line) \")) (str/join (drop-last (rest line)))
          ;(and (= (first line) \') (= (last line) \')) (str/join (concat ["\""] (drop-last (rest line)) ["\""]))
          ;(and (= (first line) \") (= (last line) \")) (str/join (drop-last (rest line)))
          :else
          line
          ))

(defn my-str-value [^String line]
    (cond (not (nil? (re-find #"^\'[\S\s]+\'$" line))) (str/join (concat [\"] (drop-last 1 (rest line)) [\"]))
          (re-find #"^\'\'$" line) ""
          :else line
          ))

(defn -getStrValue [^String line]
    (cond (and (= (first line) \') (= (last line) \')) (str/join (drop-last 1 (rest line)))
          (and (= (first line) \") (= (first line) \")) (str/join (drop-last 1 (rest line)))
          :else line
          ))

; 去掉 lst 的头和尾，取中间的链表
(defn get-contain-lst
    ([lst] (get-contain-lst (rest lst) []))
    ([[f & r] rs]
     (if (and (some? f) (some? r))
         (recur r (conj rs f))
         rs)))

; 判断两个字符串是否相等
(defn is-eq? [s-one s-two]
    (if (some? s-one)
        (= (str/lower-case (str/trim s-one)) (str/lower-case (str/trim s-two)))
        false))

; 判断 item_name 是否在序列中，大小写不敏感
(defn is-seq-contains? [[f & r] item_name]
    (if (some? f)
        (if (is-eq? f item_name)
            true
            (recur r item_name))
        false))

; 通过表名获取数据集的名称，返回 List
;(defn get_all_ds [^Ignite ignite ^String table_name]
;    (if (true? (.isMyLogEnabled (.configuration ignite)))
;        (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "SELECT DISTINCT m.schema_name FROM my_dataset AS m INNER JOIN my_dataset_table AS mt ON m.id = mt.dataset_id WHERE m.is_real = ? and mt.table_name = ?") (to-array [false table_name]))))))

; 输入列表将列表中的元素，双引号换成单引号
(defn double-to-signal
    ([lst] (double-to-signal lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (and (= (first f) (last f)) (= (first f) \"))
             ;(recur r (conj lst (format "'%s'" (str/join (reverse (rest (reverse (rest f))))))))
             (recur r (conj lst (format "'%s'" (str/join (drop-last (rest f))))))
             (recur r (conj lst f))
             )
         lst)))

; 输入列表将列表中的元素，单引号换成双引号
(defn signal-to-double
    ([lst] (signal-to-double lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (and (= (first f) (last f)) (= (first f) \'))
             ;(recur r (conj lst (format "\"%s\"" (str/join (reverse (rest (reverse (rest f))))))))
             (recur r (conj lst (format "\"%s\"" (str/join (drop-last (rest f))))))
             (recur r (conj lst f))
             )
         lst)))

; 是否是序列
(defn is-seq? [m]
    (or (vector? m) (seq? m) (list? m) (instance? java.util.List m)))

; 是否是字典
(defn is-dic? [m]
    (or (map? m) (instance? java.util.Hashtable m) (instance? java.util.HashMap m) (instance? java.util.Map m)))


; 是否已经存在
(defn is-lst-contains?
    ([lst item] (is-lst-contains? lst item false))
    ([[f & rs] item flag]
     (if (some? f)
         (if (= (is-eq? f item) true) true
                                      (recur rs item flag)) flag)))

(defn is-contains? [lst-ht item]
    (cond (is-seq? lst-ht) (is-lst-contains? lst-ht item)
          (is-dic? lst-ht) (contains? lst-ht item)
          ))

; 执行事务
; lst_cache: [MyCacheEx]
(defn trans [^Ignite ignite lst_cache]
    (if-let [tx (.txStart (.transactions ignite))]
        (try
            (loop [[f & r] lst_cache]
                (if (some? f)
                    (do
                        (cond (= (.getSqlType f) (SqlType/INSERT)) (.put (.getCache f) (.getKey f) (.getValue f))
                              (= (.getSqlType f) (SqlType/UPDATE)) (.replace (.getCache f) (.getKey f) (.getValue f))
                              (= (.getSqlType f) (SqlType/DELETE)) (.remove (.getCache f) (.getKey f)))
                        (recur r))
                    (.commit tx)))
            (catch Exception ex
                (.rollback tx)
                (.getMessage ex))
            (finally (.close tx)))))

(defn is-word? [line]
    (if (some? (re-find #"^(?i)\w+$|^(?i)\"\w+\"$|^(?i)\'\w+\'$" line))
        true))

(defn to-lazy [[f & rs]]
    (if (some? f) (concat [f] (to-lazy rs))))

(defn to_arryList
    ([lst] (to_arryList lst (ArrayList.)))
    ([[f & r] ^ArrayList lst]
     (if (some? f)
         (recur r (doto lst (.add f)))
         lst)))

(defn my-is-iter? [m]
    (cond (instance? Iterator m) true
          (and (instance? MyVar m) (instance? Iterator (get-value m))) true
          :else false))

(defn get-my-iter [m]
    (cond (instance? Iterator m) m
          (and (instance? MyVar m) (instance? Iterator (get-value m))) (get-value m)
          ))

(defn my-is-seq? [m]
    (cond (is-seq? m) true
          (and (instance? MyVar m) (is-seq? (get-value m))) true
          :else false))

(defn get-my-seq [m]
    (cond (is-seq? m) m
          (and (instance? MyVar m) (is-seq? (get-value m))) (get-value m)
          ))

; smart 操作集合的函数
(defn list-add [^ArrayList lst ^Object obj]
    (doto lst (.add obj)))

(defn list-set [^ArrayList lst ^Integer index ^Object obj]
    (doto lst (.set index obj)))

(defn list-remove [dic-lst ^Integer index]
    (cond (instance? java.util.List dic-lst) (MyTools/removeIndex dic-lst index)
          (instance? java.util.Hashtable dic-lst) (.remove dic-lst index)))

(defn list-take [^ArrayList lst ^Integer index]
    (if (> (count lst) index)
        (.subList lst 0 index)))

(defn list-drop [^ArrayList lst ^Integer index]
    (let [my-count (count lst)]
        (.subList lst index my-count)))

(defn list-take-last [^ArrayList lst ^Integer index]
    ;(take-last index lst)
    (let [my-count (count lst)]
        (.subList lst (- my-count index) my-count)))

(defn list-drop-last [^ArrayList lst ^Integer index]
    ;(drop-last index lst)
    (.subList lst 0 (- (count lst) index)))

(defn list-peek [^ArrayList lst]
    ;(.get lst (- (.size lst) 1))
    (last lst))

(defn list-pop [^ArrayList lst]
    (.subList lst 0 (- (count lst) 1)))

(defn my-concat [a & rs]
    (letfn [(my-concat-str [a & rs]
                (str/join (apply conj [a] rs)))
            ]
        (cond (string? a) (if (nil? rs)
                              (my-concat-str a)
                              (apply my-concat-str a rs))
              (is-seq? a) (if (nil? rs)
                              a
                              (apply concat a rs))
              :else
              (if (nil? rs)
                  (my-concat-str a)
                  (apply my-concat-str a rs))
              )))

(defn to-char [m & ps]
    (cond (integer? m) (MyFunction/to_char_int m)
          (or (instance? long m) (instance? Long m)) (MyFunction/to_char_long m)
          (double? m) (MyFunction/to_char_double m)
          (instance? Date m) (MyFunction/to_char_date m (first ps))
          ))

(defn my-sign [m]
    (cond (integer? m) (MyFunction/sign_int m)
          (or (instance? long m) (instance? Long m)) (MyFunction/sign_long m)
          (double? m) (MyFunction/sign_double m)
          (string? m) (MyFunction/sign_str m)
          ))

(defn decode [m]
    (if (is-seq? m)
        (MyFunction/decode (to_arryList m))
        (throw (Exception. "decode 的输入参数只能的序列！"))))

(defn trunc_double [^Double ps num]
    (letfn [(get-front-back [^Double ps]
                (loop [[f & r] (str ps) dot nil front [] back []]
                    (if (some? f)
                        (cond (and (nil? dot) (not (= f \.))) (recur r dot (conj front f) back)
                              (and (nil? dot) (= f \.)) (recur r \. front back)
                              (and (not (nil? dot)) (not (= f \.))) (recur r dot front (conj back f))
                              :else
                              (throw (Exception. "Double 输入格式错误！"))
                              )
                        [front back])))]
        (let [[front back] (get-front-back ps)]
            (cond (and (>= num 0) (<= num (count back))) (MyConvertUtil/ConvertToDouble (str/join (concat front [\.] (drop-last num back))))
                  (and (> num 0) (> num (count back))) ps
                  (and (< num 0) (< (+ (count front) num) 0)) (Double/valueOf 0)
                  (and (< num 0) (> (+ (count front) num) 0)) (let [my-count (Math/abs num)]
                                                                  (loop [index 0 lst []]
                                                                      (if (< index my-count)
                                                                          (recur (+ index 1) (conj lst \0))
                                                                          (MyConvertUtil/ConvertToDouble (str/join (concat (drop-last my-count front) lst))))))
                  :else
                  (throw (Exception. (format "输入参数错误！%s" ps)))
                  )))
    )

(defn trunc [m & ps]
    (cond (and (double? m) (not (nil? ps)) (number? (first ps))) (trunc_double m (first ps))
          (and (instance? Date m) (not (nil? ps)) (string? (first ps))) (MyFunction/trunc_date m (first ps))
          (and (instance? Date m) (nil? ps)) (MyFunction/trunc_single_date m)
          ))

(defn substr [str start count]
    (MyFunction/substr str (MyConvertUtil/ConvertToInt start) (MyConvertUtil/ConvertToInt count)))

(defn least [lst]
    (MyFunction/least (to_arryList lst)))

(defn greatest [lst]
    (MyFunction/greatest (to_arryList lst)))

(defn my-mod [v1 v2]
    (MyFunction/mod (MyConvertUtil/ConvertToInt v1) (MyConvertUtil/ConvertToInt v2)))

(defn my-round [^Double v0 num]
    (MyFunction/round v0 (MyConvertUtil/ConvertToInt num)))

(defn my-substrb [str begin length]
    (MyFunction/substrb str (MyConvertUtil/ConvertToInt begin) (MyConvertUtil/ConvertToInt count)))

(defn my-chr [my-ascii]
    (MyFunction/chr (MyConvertUtil/ConvertToInt my-ascii)))

(defn my-instrb [src dest begin time]
    (MyFunction/instrb src dest (MyConvertUtil/ConvertToInt begin) (MyConvertUtil/ConvertToInt time)))

(defn my-ltrim [str & my-key]
    (if (nil? my-key)
        (MyFunction/ltrim str)
        (MyFunction/ltrim str (first my-key))))

(defn my-rtrim [str & my-key]
    (if (nil? my-key)
        (MyFunction/rtrim str)
        (MyFunction/rtrim str (first my-key))))

(defn my-avg [lst]
    (MyFunction/avg (to_arryList lst)))

(defn my-acos [m]
    (MyFunction/acos (MyConvertUtil/ConvertToDouble m)))

(defn my-asin [m]
    (MyFunction/asin (MyConvertUtil/ConvertToDouble m)))

(defn my-atan [m]
    (MyFunction/atan (MyConvertUtil/ConvertToDouble m)))

(defn my-ceiling [m]
    (MyFunction/ceiling (MyConvertUtil/ConvertToDouble m)))

(defn my-cos [m]
    (MyFunction/cos (MyConvertUtil/ConvertToDouble m)))

(defn my-cosh [m]
    (MyFunction/cosh (MyConvertUtil/ConvertToDouble m)))

(defn my-cot [m]
    (MyFunction/cot (MyConvertUtil/ConvertToDouble m)))

(defn my-degrees [m]
    (MyFunction/degrees (MyConvertUtil/ConvertToDouble m)))

(defn my-exp [m]
    (MyFunction/exp (MyConvertUtil/ConvertToDouble m)))

(defn my-floor [m]
    (MyFunction/floor (MyConvertUtil/ConvertToDouble m)))

(defn my-ln [m]
    (MyFunction/ln (MyConvertUtil/ConvertToDouble m)))

(defn my-log [m n]
    (MyFunction/log (MyConvertUtil/ConvertToDouble m) (MyConvertUtil/ConvertToDouble n)))

(defn my-log10 [m]
    (MyFunction/log10 (MyConvertUtil/ConvertToDouble m)))

(defn my-radians [m]
    (MyFunction/radians (MyConvertUtil/ConvertToLong m)))

(defn my-roundMagic [m]
    (MyFunction/roundMagic (MyConvertUtil/ConvertToDouble m)))

(defn my-char [m]
    (MyFunction/my_char (MyConvertUtil/ConvertToInt m)))

(defn my-length [m]
    (MyFunction/length (MyConvertUtil/ConvertToString m)))

(defn my-ucase [m]
    (MyFunction/ucase (MyConvertUtil/ConvertToString m)))

(defn my-day-name [m]
    (MyFunction/day_name (MyConvertUtil/ConvertToTimestamp m)))

(defn my-to-number [m]
    (MyFunction/to_number (MyConvertUtil/ConvertToString m)))

(defn my-add-months [ps num]
    (MyFunction/add_months (MyConvertUtil/ConvertToTimestamp ps) (MyConvertUtil/ConvertToInt num)))

(defn no-sql-create [ignite group_id m]
    (.myCreate (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-get-vs [ignite group_id m]
    (.myGetValue (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-insert-tran [ignite group_id m]
    (.myInsertTran (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-update-tran [ignite group_id m]
    (.myUpdateTran (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-delete-tran [ignite group_id m]
    (.myDeleteTran (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-drop [ignite group_id m]
    (.myDrop (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-insert [ignite group_id m]
    (.myInsert (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-update [ignite group_id m]
    (.myUpdate (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn no-sql-delete [ignite group_id m]
    (.myDelete (.getNoSqlFun (MyNoSqlFunService/getInstance)) ignite group_id m))

(defn my-show-msg [msg]
    msg)

(defn my-regular [line]
    (re-pattern line))

; (my-str-replace "123wer" "(?i)\\d+" "吴大富")
(defn my-str-replace [line line-re line-rs]
    (str/replace line (re-pattern line-re) line-rs))

(defn my-str-split [line line-re]
    (str/split line (re-pattern line-re)))

(defn my-str-find [line-re line]
    (re-find (re-pattern line-re) line))

(defn show-cache-name [name]
    (str/replace name (re-pattern "^(?i)f_\\w+_") ""))

(defn my-max [lst]
    (apply max lst))

(defn my-min [lst]
    (apply min lst))

(defn my-sin [num]
    (Math/sin num))

(defn my-sinh [num]
    (Math/sinh num))

(defn my-tan [num]
    (Math/tan num))

(defn my-tanh [num]
    (Math/tanh num))

(defn my-atan2 [x y]
    (Math/atan2 x y))

(defn my-sqrt [num]
    (Math/sqrt num))

(defn my-pow [a b]
    (Math/pow a b))

(defn my-rand []
    (.nextDouble (Random.)))

(defn my-bitand [a b]
    (MyFunction/bitand (MyConvertUtil/ConvertToLong a) (MyConvertUtil/ConvertToLong b)))

(defn my-bitget [a b]
    (MyFunction/bitget (MyConvertUtil/ConvertToLong a) (MyConvertUtil/ConvertToInt b)))

(defn my-bitor [a b]
    (MyFunction/bitor (MyConvertUtil/ConvertToLong a) (MyConvertUtil/ConvertToLong b)))

(defn my-bitxor [a b]
    (MyFunction/bitxor (MyConvertUtil/ConvertToLong a) (MyConvertUtil/ConvertToLong b)))

(defn my-random_uuid []
    (MyFunction/random_uuid))

(defn my-insert [s1 start length s2]
    (MyFunction/insert (MyConvertUtil/ConvertToString s1) (MyConvertUtil/ConvertToInt start) (MyConvertUtil/ConvertToInt length) (MyConvertUtil/ConvertToString s2)))

(defn my-left [s count]
    (MyFunction/left (MyConvertUtil/ConvertToString s) (MyConvertUtil/ConvertToInt count)))

(defn my-right [s count]
    (MyFunction/right (MyConvertUtil/ConvertToString s) (MyConvertUtil/ConvertToInt count)))

(defn my-locate [search s start]
    (MyFunction/locate (MyConvertUtil/ConvertToString search) (MyConvertUtil/ConvertToString s) (MyConvertUtil/ConvertToInt start)))

(defn my-position [search s]
    (MyFunction/position (MyConvertUtil/ConvertToString search) (MyConvertUtil/ConvertToString s)))

(defn my-soundex [search]
    (MyFunction/soundex (MyConvertUtil/ConvertToString search)))

(defn my-space [s]
    (MyFunction/space (MyConvertUtil/ConvertToInt s)))

(defn my-stringencode [s]
    (MyFunction/stringencode (MyConvertUtil/ConvertToString s)))

(defn my-stringtoutf8 [s]
    (MyFunction/stringtoutf8 s))

(defn my-current_date []
    (MyFunction/current_date))

(defn my-current_time []
    (MyFunction/current_time))

(defn my-current_timestamp []
    (MyFunction/current_timestamp))

(defn my-add_year [num str-vs]
    (MyFunction/add_year (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-add_quarter [num str-vs]
    (MyFunction/add_quarter (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-add_month [num str-vs]
    (MyFunction/add_month (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-add_date [num str-vs]
    (MyFunction/add_date (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-add_hour [num str-vs]
    (MyFunction/add_hour (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-add_second [num str-vs]
    (MyFunction/add_second (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-add_ms [num str-vs]
    (MyFunction/add_ms (MyConvertUtil/ConvertToLong num) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_year [str-vs-0 str-vs]
    (MyFunction/diff_year (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_quarter [str-vs-0 str-vs]
    (MyFunction/diff_quarter (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_month [str-vs-0 str-vs]
    (MyFunction/diff_month (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_date [str-vs-0 str-vs]
    (MyFunction/diff_date (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_hour [str-vs-0 str-vs]
    (MyFunction/diff_hour (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_second [str-vs-0 str-vs]
    (MyFunction/diff_second (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-diff_ms [str-vs-0 str-vs]
    (MyFunction/diff_ms (MyConvertUtil/ConvertToString str-vs-0) (MyConvertUtil/ConvertToString str-vs)))

(defn my-dayname [str-vs]
    (MyFunction/dayname (MyConvertUtil/ConvertToString str-vs)))

(defn my-day_of_month [str-vs]
    (MyFunction/day_of_month (MyConvertUtil/ConvertToString str-vs)))

(defn my-day_of_week [str-vs]
    (MyFunction/day_of_week (MyConvertUtil/ConvertToString str-vs)))

(defn my-day_of_year [str-vs]
    (MyFunction/day_of_year (MyConvertUtil/ConvertToString str-vs)))

(defn my-hour [str-vs]
    (MyFunction/hour (MyConvertUtil/ConvertToString str-vs)))

(defn my-minute [str-vs]
    (MyFunction/minute (MyConvertUtil/ConvertToString str-vs)))

(defn my-monthname [str-vs]
    (MyFunction/monthname (MyConvertUtil/ConvertToString str-vs)))

(defn my-quarter [str-vs]
    (MyFunction/quarter (MyConvertUtil/ConvertToString str-vs)))

(defn my-year [str-vs]
    (MyFunction/year (MyConvertUtil/ConvertToString str-vs)))

(defn my-month [str-vs]
    (MyFunction/month (MyConvertUtil/ConvertToString str-vs)))

(defn ifnull [a b]
    (if (not (nil? a))
        a b))

(defn nullif [a b]
    (if (= a b)
        nil a))

(defn nvl2 [tv a b]
    (if (nil? tv)
        b a))

(defn auto_id [^Ignite ignite group_id ^String cache-name]
    (if-let [lst (str/split cache-name #"\.")]
        (cond (= (count lst) 1) (if (is-eq? (second group_id) "my_meta")
                                    (.incrementAndGet (.atomicSequence ignite cache-name 0 true))
                                    (.incrementAndGet (.atomicSequence ignite (format "f_%s_%s" (str/lower-case (second group_id)) (str/lower-case cache-name)) 0 true)))
              (= (count lst) 2) (cond (and (is-eq? (second group_id) "my_meta") (is-eq? (first lst) "my_meta")) (.incrementAndGet (.atomicSequence ignite (second lst) 0 true))
                                      (and (is-eq? (second group_id) "my_meta") (not (is-eq? (first lst) "my_meta"))) (.incrementAndGet (.atomicSequence ignite (format "f_%s_%s" (str/lower-case (first lst)) (str/lower-case (second lst))) 0 true))
                                      (and (not (is-eq? (second group_id) "my_meta")) (is-eq? (first lst) "my_meta")) (throw (Exception. "不能对 MY_META 中的表添加数据！"))
                                      :else
                                      (.incrementAndGet (.atomicSequence ignite (format "f_%s_%s" (str/lower-case (first lst)) (str/lower-case (second lst))) 0 true))
                                      )
              :else
              (throw (Exception. (format "输入表名字符串 %s 有误！" cache-name)))
              )))

(defn smart-func [func-name]
    (cond (is-eq? func-name "add") "my-lexical/list-add"
          (is-eq? func-name "set") "my-lexical/list-set"
          (is-eq? func-name "take") "my-lexical/list-take"
          (is-eq? func-name "drop") "my-lexical/list-drop"
          ;(is-eq? func-name "noSqlInsert") "my-lexical/no-sql-insert"
          (is-eq? func-name "nth") "nth"
          (is-eq? func-name "concat") "my-lexical/my-concat"
          (is-eq? func-name "concat_ws") "my-lexical/my-concat"
          (is-eq? func-name "contains?") "my-lexical/is-contains?"
          (is-eq? func-name "put") ".put"
          (is-eq? func-name "get") "my-lexical/map-list-get"
          (is-eq? func-name "remove") "my-lexical/list-remove"
          (is-eq? func-name "pop") "my-lexical/list-pop"
          (is-eq? func-name "peek") "my-lexical/list-peek"
          (is-eq? func-name "takeLast") "my-lexical/list-take-last"
          (is-eq? func-name "dropLast") "my-lexical/list-drop-last"
          (is-eq? func-name "null?") "nil?"
          (is-eq? func-name "notNull?") "my-lexical/not-null?"
          (is-eq? func-name "empty?") "empty?"
          (is-eq? func-name "notEmpty?") "my-lexical/not-empty?"
          (is-eq? func-name "nullOrEmpty?") "my-lexical/null-or-empty?"
          (is-eq? func-name "notNullOrEmpty?") "my-lexical/not-null-or-empty?"
          (is-eq? func-name "str_replace") "my-lexical/my-str-replace"
          (is-eq? func-name "str_split") "my-lexical/my-str-split"
          (is-eq? func-name "str_find") "my-lexical/my-str-find"
          (is-eq? func-name "format") "format"
          (is-eq? func-name "regular") "my-lexical/my-regular"
          (is-eq? func-name "range") "range"
          (is-eq? func-name "length") "my-lexical/my-length"
          (is-eq? func-name "ucase") "my-lexical/my-ucase"
          (is-eq? func-name "day_name") "my-lexical/my-day-name"
          (is-eq? func-name "to_number") "my-lexical/my-to-number"
          (is-eq? func-name "add_months") "my-lexical/my-add-months"
          (is-eq? func-name "to_date") "MyFunction/to_date"
          (is-eq? func-name "last_day") "MyFunction/last_day"
          (is-eq? func-name "trunc") "my-lexical/trunc"
          (is-eq? func-name "substr") "my-lexical/substr"
          (is-eq? func-name "instrb") "my-lexical/my-instrb"
          (is-eq? func-name "chr") "my-lexical/my-chr"
          (is-eq? func-name "months_between") "MyFunction/months_between"
          (is-eq? func-name "replace") "MyFunction/replace"
          (is-eq? func-name "substrb") "my-lexical/my-substrb"
          (is-eq? func-name "lengthb") "MyFunction/lengthb"
          (is-eq? func-name "tolowercase") "str/lower-case"
          (is-eq? func-name "lowercase") "str/lower-case"
          (is-eq? func-name "toUpperCase") "str/upper-case"
          (is-eq? func-name "uppercase") "str/upper-case"
          (is-eq? func-name "show_cache_name") "my-lexical/show-cache-name"
          (is-eq? func-name "show_msg") "my-lexical/my-show-msg"

          (is-eq? func-name "char") "my-lexical/my-char"
          (is-eq? func-name "abs") "MyFunction/abs"
          (is-eq? func-name "acos") "my-lexical/my-acos"
          (is-eq? func-name "asin") "my-lexical/my-asin"
          (is-eq? func-name "atan") "my-lexical/my-atan"
          (is-eq? func-name "ceiling") "my-lexical/my-ceiling"
          (is-eq? func-name "cos") "my-lexical/my-cos"
          (is-eq? func-name "cosh") "my-lexical/my-cosh"
          (is-eq? func-name "cot") "my-lexical/my-cot"
          (is-eq? func-name "degrees") "my-lexical/my-degrees"
          (is-eq? func-name "exp") "my-lexical/my-exp"
          (is-eq? func-name "floor") "my-lexical/my-floor"
          (is-eq? func-name "ln") "my-lexical/my-ln"
          (is-eq? func-name "log") "my-lexical/my-log"
          (is-eq? func-name "log10") "my-lexical/my-log10"
          (is-eq? func-name "pi") "MyFunction/pi"
          (is-eq? func-name "radians") "my-lexical/my-radians"
          (is-eq? func-name "roundMagic") "my-lexical/my-roundMagic"
          (is-eq? func-name "sign") "my-lexical/my-sign"
          (is-eq? func-name "zero") "MyFunction/zero"
          (is-eq? func-name "count") "count"
          (is-eq? func-name "to_char") "my-lexical/to-char"
          (is-eq? func-name "instr") "MyFunction/instr"
          (is-eq? func-name "lpad") "MyFunction/lpad"
          (is-eq? func-name "rpad") "MyFunction/rpad"
          (is-eq? func-name "decode") "my-lexical/decode"
          (is-eq? func-name "ascii") "MyFunction/ascii"
          (is-eq? func-name "least") "my-lexical/least"
          (is-eq? func-name "greatest") "my-lexical/greatest"
          (is-eq? func-name "ceil") "MyFunction/ceil"
          (is-eq? func-name "floor") "MyFunction/floor"
          (is-eq? func-name "mod") "my-lexical/my-mod"
          (is-eq? func-name "max") "my-lexical/my-max"
          (is-eq? func-name "min") "my-lexical/my-min"
          (is-eq? func-name "sin") "my-lexical/my-sin"
          (is-eq? func-name "sinh") "my-lexical/my-sinh"
          (is-eq? func-name "tan") "my-lexical/my-tan"
          (is-eq? func-name "tanh") "my-lexical/my-tanh"
          (is-eq? func-name "atan2") "my-lexical/my-atan2"
          (is-eq? func-name "sqrt") "my-lexical/my-sqrt"
          (is-eq? func-name "pow") "my-lexical/my-pow"
          (is-eq? func-name "rand") "my-lexical/my-rand"
          (is-eq? func-name "round") "my-lexical/my-round"
          (is-eq? func-name "lower") "MyFunction/lower"
          (is-eq? func-name "upper") "MyFunction/upper"
          (is-eq? func-name "ltrim") "my-lexical/my-ltrim"
          (is-eq? func-name "rtrim") "my-lexical/my-rtrim"
          (is-eq? func-name "trim") "MyFunction/trim"
          (is-eq? func-name "translate") "MyFunction/translate"
          (is-eq? func-name "avg") "MyFunction/my-avg"
          (is-eq? func-name "bitand") "my-lexical/my-bitand"
          (is-eq? func-name "bitget") "my-lexical/my-bitget"
          (is-eq? func-name "bitor") "my-lexical/my-bitor"
          (is-eq? func-name "bitxor") "my-lexical/my-bitxor"
          (is-eq? func-name "random_uuid") "my-lexical/my-random_uuid"
          (is-eq? func-name "insert") "my-lexical/my-insert"
          (is-eq? func-name "left") "my-lexical/my-left"
          (is-eq? func-name "right") "my-lexical/my-right"
          (is-eq? func-name "locate") "my-lexical/my-locate"
          (is-eq? func-name "position") "my-lexical/my-position"
          (is-eq? func-name "soundex") "my-lexical/my-soundex"
          (is-eq? func-name "space") "my-lexical/my-space"
          (is-eq? func-name "stringencode") "my-lexical/my-stringencode"
          (is-eq? func-name "stringtoutf8") "my-lexical/my-stringtoutf8"
          (is-eq? func-name "current_date") "my-lexical/my-current_date"
          (is-eq? func-name "current_time") "my-lexical/my-current_time"
          (is-eq? func-name "current_timestamp") "my-lexical/my-current_timestamp"
          (is-eq? func-name "add_year") "my-lexical/my-add_year"
          (is-eq? func-name "add_quarter") "my-lexical/my-add_quarter"
          (is-eq? func-name "add_month") "my-lexical/my-add_month"
          (is-eq? func-name "add_date") "my-lexical/my-add_date"
          (is-eq? func-name "add_hour") "my-lexical/my-add_hour"
          (is-eq? func-name "add_second") "my-lexical/my-add_second"
          (is-eq? func-name "add_ms") "my-lexical/my-add_ms"
          (is-eq? func-name "diff_year") "my-lexical/my-diff_year"
          (is-eq? func-name "diff_quarter") "my-lexical/my-diff_quarter"
          (is-eq? func-name "diff_month") "my-lexical/my-diff_month"
          (is-eq? func-name "diff_date") "my-lexical/my-diff_date"
          (is-eq? func-name "diff_hour") "my-lexical/my-diff_hour"
          (is-eq? func-name "diff_second") "my-lexical/my-diff_second"
          (is-eq? func-name "diff_ms") "my-lexical/my-diff_ms"
          (is-eq? func-name "dayname") "my-lexical/my-dayname"
          (is-eq? func-name "day_of_month") "my-lexical/my-day_of_month"
          (is-eq? func-name "day_of_week") "my-lexical/my-day_of_week"
          (is-eq? func-name "day_of_year") "my-lexical/my-day_of_year"
          (is-eq? func-name "hour") "my-lexical/my-hour"
          (is-eq? func-name "minute") "my-lexical/my-minute"
          (is-eq? func-name "monthname") "my-lexical/my-monthname"
          (is-eq? func-name "quarter") "my-lexical/my-quarter"
          (is-eq? func-name "year") "my-lexical/my-year"
          (is-eq? func-name "month") "my-lexical/my-month"
          (is-eq? func-name "ifnull") "my-lexical/ifnull"
          (is-eq? func-name "nullif") "my-lexical/nullif"
          (is-eq? func-name "nvl2") "my-lexical/nvl2"
          ))

;(defn my-concat [[f & r]]
;    (cond (string? (first f)) (MyFunction/concat lst)))

(defn my-cache-name [^String schema_name ^String table_name]
    (if (is-eq? schema_name "MY_META")
        (str/lower-case table_name)
        (format "f_%s_%s" (str/lower-case schema_name) (str/lower-case table_name))))

(defn not-empty? [lst]
    (and (is-seq? lst) (not (empty? lst))))

(defn not-null? [m]
    (not (nil? m)))


(defn null-or-empty? [m]
    (if (instance? String m)
        (Strings/isNullOrEmpty m)
        (cond (nil? m) true
              (and (is-seq? m) (empty? m)) true
              :else false
              )))

(defn not-null-or-empty? [m]
    (not (null-or-empty? m)))

(defn map-list-get [dic-lst my-key]
    (cond (map? dic-lst) (get dic-lst my-key)
          (and (is-seq? dic-lst) (number? my-key)) (nth dic-lst my-key)
          (and (instance? java.util.List dic-lst) (number? my-key)) (nth dic-lst my-key)
          (instance? java.util.Hashtable dic-lst) (.get dic-lst my-key)
          :else
          (throw (Exception. "数据集没有该方法"))
          ))

; 获取 select sys 的权限 code
(defn get-select-sys-code [ignite schema_name my_group_id]
    ;(get-code ignite schema_name table_name my_group_id "查")
    (.get (.cache ignite "my_sys_view_ast") (MyViewAstPK. schema_name "sys" (first my_group_id)))
    )

; 获取 select 的权限 code
(defn get-select-code [ignite schema_name table_name my_group_id]
    ;(get-code ignite schema_name table_name my_group_id "查")
    (.get (.cache ignite "my_select_view_ast") (MyViewAstPK. schema_name table_name (first my_group_id)))
    )

; 获取 insert 的权限 code
(defn get-insert-code [ignite schema_name table_name my_group_id]
    ;(get-code-insert ignite schema_name table_name my_group_id "增")
    (.get (.cache ignite "my_insert_view_ast") (MyViewAstPK. schema_name table_name (first my_group_id)))
    )

; 获取 delete 的权限 code
(defn get-delete-code [ignite schema_name table_name my_group_id]
    ;(get-code-delete ignite schema_name table_name my_group_id "删")
    (.get (.cache ignite "my_delete_view_ast") (MyViewAstPK. schema_name table_name (first my_group_id)))
    )

; 获取 update 的权限 code
(defn get-update-code [ignite schema_name table_name my_group_id]
    ;(get-code-update ignite schema_name table_name my_group_id "改")
    (.get (.cache ignite "my_update_view_ast") (MyViewAstPK. schema_name table_name (first my_group_id)))
    )

(defn get-schema
    ([lst] (if-let [m (get-schema lst [] [])]
               (cond (= (count m) 1) {:schema_name "" :table_name (str/trim (first m))}
                     (= (count m) 2) {:schema_name (str/trim (first m)) :table_name (str/trim (second m))}
                     :else
                     (throw (Exception. "语句错误，表名不符合要求！"))
                     )
               (throw (Exception. "语句错误，表名不符合要求！"))))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (= f \.) (if-not (empty? stack)
                            (recur r [] (conj lst (str/join stack)))
                            (throw (Exception. "语句错误，表名开始不能有 '.' 符号！")))
               :else
               (recur r (conj stack f) lst)
               )
         (if (empty? stack)
             lst
             (conj lst (str/join stack))))))

; 获取 ast 中 item
(defn my-ast-items [ast]
    (letfn [(has-item-in [[f & r] item]
                (if (some? f)
                    (if (is-eq? (-> f :item_name) (-> item :item_name))
                        true
                        (recur r item))
                    false))
            (get-only-item
                ([lst] (get-only-item lst []))
                ([[f & r] lst-only]
                 (if (some? f)
                     (if (has-item-in lst-only f)
                         (recur r lst-only)
                         (recur r (conj lst-only f)))
                     lst-only)))
            (get-ast-map-items [ast-map]
                (cond (and (contains? ast-map :item_name) (false? (-> ast-map :const))) ast-map
                      :else
                      (loop [[f & r] (keys ast-map) rs []]
                          (if (some? f)
                              (if-let [f-m (get-ast-items (get ast-map f))]
                                  (if (is-seq? f-m)
                                      (recur r (apply conj rs f-m))
                                      (recur r (conj rs f-m)))
                                  (recur r rs))
                              rs))
                      ))
            (get-ast-lst-items
                ([lst] (get-ast-lst-items lst []))
                ([[f & r] lst]
                 (if (some? f)
                     (if-let [f-m (get-ast-items f)]
                         (if (is-seq? f-m)
                             (recur r (apply conj lst f-m))
                             (recur r (conj lst f-m)))
                         (recur r lst))
                     lst)))
            (get-ast-items [ast]
                (cond (map? ast) (get-ast-map-items ast)
                      (is-seq? ast) (get-ast-lst-items ast)
                      ))]
        (if-let [items-lst (get-ast-items ast)]
            (cond (map? items-lst) [items-lst]
                  (is-seq? items-lst) (cond (= (count items-lst) 1) items-lst
                                            (> (count items-lst) 1) (get-only-item (get-ast-items ast)))
                  ))
        ))

; 判断函数里面的参数是否有 ? 号
(defn my-ast-has-ps-items [ast]
    (letfn [(get-ast-map-items [ast-map]
                (cond (and (contains? ast-map :item_name) (= (-> ast-map :item_name) "?")) true
                      :else
                      (loop [[f & r] (keys ast-map)]
                          (if (some? f)
                              (if-let [f-m (get-ast-items (get ast-map f))]
                                  (if (true? f-m)
                                      true
                                      (recur r))
                                  (recur r))
                              ))
                      ))
            (get-ast-lst-items
                [[f & r]]
                (if (some? f)
                    (if-let [f-m (get-ast-items f)]
                        (if (true? f-m)
                            true
                            (recur r))
                        (recur r))
                    ))
            (get-ast-items [ast]
                (cond (map? ast) (get-ast-map-items ast)
                      (is-seq? ast) (get-ast-lst-items ast)
                      ))]
        (get-ast-items ast)
        ))

(defn to-smart-sql-type [f-type]
    (cond (re-find #"^(?i)FLOAT$" f-type) "double"
          (re-find #"^(?i)DATETIME$" f-type) "TIMESTAMP"
          (re-find #"^(?i)REAL$" f-type) "double"
          (re-find #"^(?i)VARBINARY$" f-type) "VARBINARY"
          (re-find #"^(?i)REAL\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)REAL\(\s*\d+\s*\)$|^(?i)REAL$" f-type) (str/replace f-type #"REAL" "double")
          (re-find #"^(?i)BIT$" f-type) "BOOLEAN"
          :else
          f-type
          ))

; 生成 MyLogCache
(defn get_mylogcache [^Ignite ignite ^String table_name ^MyLogCache myLogCache]
    (.build (doto (.builder (.binary ignite) "cn.plus.model.MyLog")
                (.setField "table_name" table_name)
                (.setField "mycacheex" (MyCacheExUtil/objToBytes myLogCache))
                (.setField "create_date" (Timestamp. (.getTime (Date.)))))))

; 返回精度和保留小数位数
(defn to_p_d [column_type]
    (re-seq #"\d+" (re-find #"(?<=^(?i)DECIMAL\()[\s\S]+(?=\))" column_type)))

(defn vs-type [line]
    (if (some? line)
        (cond (some? (re-find #"^(?i)\d+$" line)) Integer
              (some? (re-find #"^(?i)\d+\.\d$" line)) Double
              ;(some? (re-find #"^(?i)\"\w*\"$|^(?i)'\w*'$|^(?i)\"\W*\"$|^(?i)'\W*'$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
              (some? (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$|^\'\'$|^\"\"$" line)) String
              (some? (re-find #"^(?i)\d+D$" line)) Double
              (some? (re-find #"^(?i)\d+L$" line)) Long
              (some? (re-find #"^(?i)\d+F$" line)) Double
              (some? (re-find #"^(?i)true$" line)) Boolean
              (some? (re-find #"^(?i)false$" line)) Boolean
              :else (throw (Exception. (format "%s 不能确定其数据类型！请仔细检查！" line)))
              )))

(defn convert_to_java_type [column_type]
    (cond (re-find #"^(?i)integer$|^(?i)int$" column_type) {:java_type Integer}
          (re-find #"^(?i)SMALLINT$|^(?i)SMALLINT\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)SMALLINT\(\s*\d+\s*\)$" column_type) {:java_type Integer}
          (re-find #"^(?i)float$|^(?i)double$" column_type) {:java_type Double}
          (re-find #"^(?i)TINYINT$|^(?i)TINYINT\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)TINYINT\(\s*\d+\s*\)$" column_type) {:java_type Byte}
          (re-find #"^(?i)varchar$" column_type) {:java_type String :len 0}
          (re-find #"^(?i)varchar\(\d+\)$" column_type) {:java_type String :len (Integer/parseInt (re-find #"(?<=^(?i)varchar\()\d+(?=\))" column_type))}
          (re-find #"^(?i)char$" column_type) {:java_type String :len 0}
          (re-find #"^(?i)char\(\d+\)$" column_type) {:java_type String :len (Integer/parseInt (re-find #"(?<=^(?i)varchar\()\d+(?=\))" column_type))}
          (re-find #"^(?i)BOOLEAN$" column_type) {:java_type Boolean}
          (re-find #"^(?i)BIGINT$|^(?i)long$" column_type) {:java_type Long}
          (re-find #"^(?i)BINARY$" column_type) {:java_type "byte[]"}
          (re-find #"^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$" column_type) {:java_type Timestamp}
          (re-find #"^(?i)REAL$" column_type) {:java_type Double}
          (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$" column_type) (when-let [m (to_p_d column_type)]
                                                                            (if (= (count m) 2)
                                                                                {:java_type BigDecimal :p (Integer/parseInt (nth m 0)) :d (Integer/parseInt (nth m 1))}))
          (re-find #"^(?i)DECIMAL\(\s*\d+\s*\)$" column_type) (when-let [m (to_p_d column_type)]
                                                                  (if (= (count m) 1)
                                                                      {:java_type BigDecimal :p (Integer/parseInt (nth m 0)) :d 0}))
          (re-find #"^(?i)DECIMAL$" column_type) {:java_type BigDecimal :p 0 :d 0}
          :else
          (throw (Exception. "数据类型写法有误请确认正确的写法！"))
          ))

(defn convert_to_type [column_type]
    (cond (re-find #"^(?i)integer$|^(?i)int$|^(?i)long$|^(?i)double$|^(?i)float$|^(?i)blob$|^(?i)tinyblob$|^(?i)mediumblob$|^(?i)longblob$|^(?i)text$|^(?i)tinytext$|^(?i)TINYINT$|^(?i)MEDIUMINT$|^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$|^(?i)nvarchar$|^(?i)nvarchar\(\d+\)$|^(?i)nchar$|^(?i)nchar\(\d+\)$|^(?i)BOOLEAN$|^(?i)BIGINT$|^(?i)BINARY$|^(?i)VARBINARY$|^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$|^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)DECIMAL\(\s*\d+\s*\)$|^(?i)DECIMAL$|^(?i)numeric\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)numeric\(\s*\d+\s*\)$|^(?i)numeric$" column_type) column_type
          (re-find #"^(?i)REAL$" column_type) "double"
          (re-find #"^(?i)VARBINARY$" column_type) "VARBINARY"
          (re-find #"^(?i)SMALLINT$" column_type) "INTEGER"
          (re-find #"^(?i)bit$" column_type) "BOOLEAN"
          :else
          (throw (Exception. (format "数据类型写法有误请确认正确的写法！%s" column_type)))
          ))


(defn get_inner_str_value [line]
                     (if (string? line)
                         (cond (not (nil? (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$" line))) (str/join (drop-last (rest line)))
                               (re-find #"^\'\'$" line) (str/join (drop-last (rest line)))
                               (re-find #"^\"\"$" line) (str/join (drop-last (rest line)))
                               :else line
                               )
                         line)
                     )

(defn get-java-items-vs [item]
    (if (= java.lang.String (-> item :java_item_type))
        (MyConvertUtil/ConvertToString (get_inner_str_value (-> item :item_name)))
        (-> item :item_name)))

(defn get_jave_vs [column_type column_value]
    (letfn [(get_inner_str_value [line]
                (if (string? line)
                    (cond (not (nil? (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$" line))) (str/join (drop-last (rest line)))
                          (re-find #"^\'\'$" line) (str/join (drop-last (rest line)))
                          (re-find #"^\"\"$" line) (str/join (drop-last (rest line)))
                          :else line
                          )
                    line)
                )]
        (cond (re-find #"^(?i)integer$|^(?i)int$" column_type) (MyConvertUtil/ConvertToInt (get_inner_str_value column_value))
              (re-find #"^(?i)float$" column_type) (MyConvertUtil/ConvertToDouble (get_inner_str_value column_value))
              (re-find #"^(?i)double$" column_type) (MyConvertUtil/ConvertToDouble (get_inner_str_value column_value))
              (re-find #"^(?i)SMALLINT\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)SMALLINT\(\s*\d+\s*\)$|^(?i)SMALLINT$" column_type) (MyConvertUtil/ConvertToInt (get_inner_str_value column_value))
              (re-find #"^(?i)TINYINT$" column_type) (MyConvertUtil/ConvertToInt (get_inner_str_value column_value))
              (re-find #"^(?i)varchar$" column_type) (let [vs (get_inner_str_value column_value)]
                                                         (MyConvertUtil/ConvertToString vs))
              (re-find #"^(?i)varchar\(\d+\)$" column_type) (let [vs (get_inner_str_value column_value)]
                                                                (MyConvertUtil/ConvertToString vs))
              (re-find #"^(?i)char$" column_type) (let [vs (get_inner_str_value column_value)]
                                                      (MyConvertUtil/ConvertToString vs))
              (re-find #"^(?i)char\(\d+\)$" column_type) (let [vs (get_inner_str_value column_value)]
                                                             (MyConvertUtil/ConvertToString vs))
              (re-find #"^(?i)BOOLEAN$" column_type) (MyConvertUtil/ConvertToBoolean (get_inner_str_value column_value))
              (re-find #"^(?i)BIGINT$|^(?i)long$" column_type) (MyConvertUtil/ConvertToLong (get_inner_str_value column_value))
              (re-find #"^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$" column_type) (MyConvertUtil/ConvertToTimestamp (get_inner_str_value column_value))
              (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$" column_type) (MyConvertUtil/ConvertToDecimal (get_inner_str_value column_value))
              (re-find #"^(?i)DECIMAL\(\s*\d+\s*\)$" column_type) (MyConvertUtil/ConvertToDecimal (get_inner_str_value column_value))
              (re-find #"^(?i)DECIMAL$" column_type) (MyConvertUtil/ConvertToDecimal (get_inner_str_value column_value))
              (re-find #"^(?i)VARBINARY$" column_type) (get_inner_str_value column_value)
              :else
              (throw (Exception. (format "数据类型写法有误请确认正确的写法！%s %s" column_type column_value)))
              ))
    )

(defn get_jave_vs_ex [column_type column_value scale]
    (if (not (nil? scale))
        (cond (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$" column_type) (MyConvertUtil/ConvertToDecimal (get_inner_str_value column_value) scale)
              (re-find #"^(?i)DECIMAL\(\s*\d+\s*\)$" column_type) (MyConvertUtil/ConvertToDecimal (get_inner_str_value column_value) scale)
              (re-find #"^(?i)DECIMAL$" column_type) (MyConvertUtil/ConvertToDecimal (get_inner_str_value column_value) scale)
              :else (get_jave_vs column_type column_value))
        (get_jave_vs column_type column_value))
    )

; 预处理字符串
; 1、按空格和特殊字符把字符串转换成数组
; 2、特殊字符：#{"(" ")" "," "=" ">" "<" "+" "-" "*" "/" ">=" "<="}

; 1、按空格和特殊字符把字符串转换成数组
;(defn to-back
;    ([line] (to-back line [] [] [] []))
;    ([[f & rs] stack-str stack-zhushi-1 stack-zhushi-2 lst]
;     (if (some? f)
;         (cond (and (= f \space) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst)] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 []))
;               (and (= f \() (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "("] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["("] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \)) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ")"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [")"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;
;               (and (= f \/) (= (first rs) \*) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back (rest rs) stack-str (conj stack-zhushi-1 1) stack-zhushi-2 lst)
;               (and (= f \*) (= (first rs) \/) (= (count stack-str) 0) (> (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back (rest rs) stack-str (pop stack-zhushi-1) stack-zhushi-2 [])
;               (and (= f \-) (= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back (rest rs) stack-str stack-zhushi-1 (conj stack-zhushi-2 1) lst)
;
;               (and (= f \,) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ","] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [","] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \+) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "+"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["+"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \-) (not= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "-"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["-"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \*) (not= (first rs) \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "*"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["*"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "/"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["/"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "="] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["="] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [">="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) ">"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat [">"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<="] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (not= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<"] (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 [])))
;               (and (= f \<) (some? (first rs)) (= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (concat [(str/join lst) "<>"] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])) (concat ["<>"] (to-back (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [])))
;
;               (and (= f \") (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back rs (conj stack-str [f "双"]) stack-zhushi-1 stack-zhushi-2 (conj lst f))
;               (and (= f \") (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
;                                                                                                                    (cond (= (nth t 1) "双") (to-back rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f))
;                                                                                                                          :else
;                                                                                                                          (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f)))
;                                                                                                                    )
;               (and (= f \') (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (to-back rs (conj stack-str [f "单"]) stack-zhushi-1 stack-zhushi-2 (conj lst f))
;               (and (= f \') (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
;                                                                                                                    (cond (= (nth t 1) "单") (to-back rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f))
;                                                                                                                          :else
;                                                                                                                          (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f)))
;                                                                                                                    )
;               (and (= f \newline) (= (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (to-back (concat [\space] rs) stack-str stack-zhushi-1 stack-zhushi-2 lst)
;               (and (= f \newline) (> (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (to-back rs stack-str stack-zhushi-1 (pop stack-zhushi-2) [])
;               :else (to-back rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f))
;               )
;         (if (> (count lst) 0) [(str/join lst)])
;         )))

(defn to-back-0
    ([^String line] (to-back-0 line [] [] [] [] []))
    ([[f & rs] stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result]
     (if (some? f)
         (cond (and (= f \space) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst)])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] lst_result))
               (and (= f \() (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "("])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["("])))
               (and (= f \)) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ")"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [")"])))

               (and (= f \/) (= (first rs) \*) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (conj stack-zhushi-1 1) stack-zhushi-2 lst lst_result)
               (and (= f \*) (= (first rs) \/) (= (count stack-str) 0) (> (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (pop stack-zhushi-1) stack-zhushi-2 [] lst_result)
               (and (= f \-) (= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str stack-zhushi-1 (conj stack-zhushi-2 1) lst lst_result)

               (and (= f \,) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ","])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [","])))
               (and (= f \+) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "+"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["+"])))
               (and (= f \-) (not= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "-"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["-"])))
               (and (= f \*) (not= (first rs) \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "*"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["*"])))
               (and (= f \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "/"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["/"])))
               (and (= f \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "="])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["="])))
               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ">="])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [">="])))
               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ">"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [">"])))
               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<="])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<="])))
               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (not= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<"])))
               (and (= f \<) (some? (first rs)) (= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<>"])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<>"])))

               (and (= f \") (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "双"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \") (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "双") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \') (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "单"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \') (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "单") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \newline) (= (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur (concat [\space] rs) stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result)
               (and (= f \newline) (> (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur rs stack-str stack-zhushi-1 (pop stack-zhushi-2) [] lst_result)
               :else (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               )
         (if (> (count lst) 0) (concat lst_result [(str/join lst)]) lst_result)
         )))

(defn to-back
    ([^String line] (to-back line [] [] [] [] []))
    ([[f & rs] stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result]
     (if (some? f)
         (cond (and (= f \.) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "."])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["."])))
               (and (= f \;) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ";"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [";"])))
               (and (= f \:) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ":"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [":"])))
               (and (= f \[) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "["])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["["])))
               (and (= f \]) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "]"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["]"])))
               (and (= f \{) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "{"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["{"])))
               (and (= f \}) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "}"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["}"])))

               (and (= f \space) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst)])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] lst_result))
               (and (= f \() (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "("])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["("])))
               (and (= f \)) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ")"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [")"])))

               (and (= f \/) (= (first rs) \*) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (conj stack-zhushi-1 1) stack-zhushi-2 lst lst_result)
               (and (= f \*) (= (first rs) \/) (= (count stack-str) 0) (> (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str (pop stack-zhushi-1) stack-zhushi-2 [] lst_result)
               (and (= f \-) (= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur (rest rs) stack-str stack-zhushi-1 (conj stack-zhushi-2 1) lst lst_result)

               (and (= f \,) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ","])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [","])))
               (and (= f \+) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "+"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["+"])))
               (and (= f \-) (not= (first rs) \-) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "-"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["-"])))
               (and (= f \*) (not= (first rs) \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "*"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["*"])))
               (and (= f \/) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "/"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["/"])))
               (and (= f \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "="])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["="])))
               (and (= f \>) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ">="])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [">="])))
               (and (= f \>) (some? (first rs)) (not= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) ">"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [">"])))
               (and (= f \<) (some? (first rs)) (= (first rs) \=) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<="])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<="])))
               (and (= f \<) (some? (first rs)) (not= (first rs) \=) (not= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<"])) (recur rs stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<"])))
               (and (= f \<) (some? (first rs)) (= (first rs) \>) (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (if (> (count lst) 0) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result [(str/join lst) "<>"])) (recur (rest rs) stack-str stack-zhushi-1 stack-zhushi-2 [] (concat lst_result ["<>"])))

               (and (= f \") (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "双"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \") (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "双") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \') (= (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (recur rs (conj stack-str [f "单"]) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               (and (= f \') (> (count stack-str) 0) (= (count stack-zhushi-1) 0) (= (count stack-zhushi-2) 0)) (let [t (peek stack-str)]
                                                                                                                    (cond (= (nth t 1) "单") (recur rs (pop stack-str) stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
                                                                                                                          :else
                                                                                                                          (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result))
                                                                                                                    )
               (and (= f \newline) (= (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur (concat [\space] rs) stack-str stack-zhushi-1 stack-zhushi-2 lst lst_result)
               (and (= f \newline) (> (count stack-zhushi-2) 0) (= (count stack-zhushi-1) 0)) (recur rs stack-str stack-zhushi-1 (pop stack-zhushi-2) [] lst_result)
               :else (recur rs stack-str stack-zhushi-1 stack-zhushi-2 (conj lst f) lst_result)
               )
         (if (> (count lst) 0) (concat lst_result [(str/join lst)]) lst_result)
         )))

(defn -lineToList [^String line]
    (to_arryList (to-back line)))

; 按逗号切分 query
; stack 记录 （）
; lst 记录 query-item
(defn query-items-line
    ([lst] (let [{query-items :query-items rs-lst :rs-lst} (query-items-line lst [] [])]
               (if (some? query-items) {:query-items (concat [(to-lazy (rest (first query-items)))] (rest query-items)) :rs-lst rs-lst})))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0)
                                                       (let [{query-items :query-items rs-lst :rs-lst} (query-items-line rs stack [])]
                                                           {:query-items (concat [(to-lazy lst) ","] query-items) :rs-lst rs-lst})
                                                       )

               (and (is-eq? f "from") (= (count stack) 0)) (if (> (count lst) 0)
                                                               {:query-items [(to-lazy lst)] :rs-lst rs}
                                                               )
               (= f "(") (query-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (query-items-line rs (pop stack) (conj lst f)))
               :else
               (query-items-line rs stack (conj lst f))
               ))))

; 获取 table 的定义
(defn tables-items-line
    ([lst] (let [m (tables-items-line lst [] [])]
               (if (some? m) m {:table-items [(to-lazy lst)] :rs-lst nil :my-type nil})))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0)
                                                       (let [table_items_obj (tables-items-line rs stack [])]
                                                           (if (nil? table_items_obj)
                                                               {:table-items (concat [(to-lazy lst) ","] [rs]) :rs-lst nil :my-type nil}
                                                               {:table-items (concat [(to-lazy lst) ","] (get table_items_obj :table-items)) :rs-lst (get table_items_obj :rs-lst) :my-type (get table_items_obj :my-type)})))

               (and (is-eq? f "where") (= (count stack) 0)) (if (> (count lst) 0)
                                                                {:table-items [(to-lazy lst)] :rs-lst rs :my-type "where"}
                                                                )
               (and (is-eq? f "order") (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:table-items [(to-lazy lst)] :rs-lst (rest rs) :my-type "order"})
               (and (is-eq? f "group") (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:table-items [(to-lazy lst)] :rs-lst (rest rs) :my-type "group"})
               (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:table-items [(to-lazy lst)] :rs-lst rs :my-type "limit"})
               (= f "(") (tables-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (tables-items-line rs (pop stack) (conj lst f)))
               :else
               (tables-items-line rs stack (conj lst f))
               )
         )))

; 获取 where 的定义
(defn where-items-line
    ([lst] (where-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "group") (some? (first rs)) (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:where-items (to-lazy lst) :rs-lst (concat ["group"] rs)} (throw (Exception. "where 语句后不能直接跟 group by")))
               (and (is-eq? f "order") (some? (first rs)) (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:where-items (to-lazy lst) :rs-lst (concat ["order"] rs)} (throw (Exception. "where 语句后不能直接跟 order by")))
               (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:where-items (to-lazy lst) :rs-lst (concat ["limit"] rs)} (throw (Exception. "where 语句后不能直接跟 limit")))
               (= f "(") (recur rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (recur rs (pop stack) (conj lst f)))
               :else
               (recur rs stack (conj lst f))
               )
         (if (> (count lst) 0) {:where-items (to-lazy lst) :rs-lst []}))))


; 获取 group by
; 如果在 group by 字段中有 having 就返回 {:group-by [] :having true :rs-lst rs}
(defn group-by-items-line
    ([lst] (group-by-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "having") (= (count stack) 0) (> (count lst) 0)) {:group-by (to-lazy lst) :having true :rs-lst rs}
               (= f "(") (recur rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (recur rs (pop stack) (conj lst f)))
               :else
               (recur rs stack (conj lst f))
               )
         )))

; 返回： {:having [] :rs-lst []}
(defn group-by-having-items-line
    ([lst] (group-by-having-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "order") (some? (first rs)) (is-eq? (first rs) "by") (= (count stack) 0)) (if (> (count lst) 0) {:group-having (to-lazy lst) :rs-lst (concat ["order"] rs)})
               (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:group-having (to-lazy lst) :rs-lst (concat ["limit"] rs)})
               (and (is-eq? f "limit") (= (count stack) 0)) (if (= (count lst) 0) {:group-having nil :rs-lst (concat ["limit"] rs)})
               (= f "(") (recur rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (recur rs (pop stack) (conj lst f)))
               :else
               (recur rs stack (conj lst f))
               )
         (if (and (> (count lst) 0) (= (count stack) 0)) {:group-having (to-lazy lst) :rs-lst []})
         )))

; group by
(defn sql-group-by [lst]
    (let [m (group-by-items-line lst)]
        (if (empty? m)
            (let [{group-having :group-having rs-lst :rs-lst} (group-by-having-items-line lst)]
                {:group-by group-having :rs-lst rs-lst})
            (let [{group-having :group-having rs-lst :rs-lst} (group-by-having-items-line (get m :rs-lst))]
                {:group-by (get m :group-by) :having group-having :rs-lst rs-lst}))))

; 获取 order by
; 返回： {:order-by-obj {:order-by [] :desc-asc "desc"} :rs-lst []}
; 如果返回为 nil 证明里面没有 order by
(defn order-by-items-line
    ([lst] (order-by-items-line lst [] []))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (is-eq? f "limit") (= (count stack) 0)) (if (> (count lst) 0) {:order (to-lazy lst) :rs-lst (concat ["limit"] rs)})
               (= f "(") (order-by-items-line rs (conj stack f) (conj lst f))
               (= f ")") (if (> (count stack) 0) (order-by-items-line rs (pop stack) (conj lst f)))
               :else
               (order-by-items-line rs stack (conj lst f))
               )
         (if (and (> (count lst) 0) (= (count stack) 0)) {:order (to-lazy lst) :rs-lst []})
         )))

(defn my-comma
    ([lst] (my-comma lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

(defn is-operate?
    ([lst] (if (and (= (first lst) "(") (= (last lst) ")")) (let [m (is-operate? lst [] [] [])]
                                                                (if (and (some? m) (= (count m) 1)) (take (- (count (nth m 0)) 2) (rest (nth m 0)))))))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond
             (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
             (= f ")") (if (= (count stack) 1) (recur rs (pop stack) [] (concat result-lst [(conj lst f)])) (if (> (count stack) 0) (recur rs (pop stack) (conj lst f) result-lst) (recur rs [] (conj lst f) result-lst)))
             :else
             (recur rs stack (conj lst f) result-lst)
             ) (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

(defn arithmetic
    ([lst] (arithmetic lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (contains? #{"+" "-" "*" "/"} f) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

(defn is-arithmetic? [lst]
    (if (some? lst)
        (if-let [m (arithmetic lst)]
            (if (> (count m) 1) true false) false) false))

(defn is-float-int [lst]
    (if (and (= (count lst) 3) (re-find #"^\d+$" (first lst)) (= (second lst) ".") (= (last lst) "0"))
        true false))

(defn is-express [lst]
    (if (some? lst)
        (cond (= (count lst) 1) true
              (not (nil? (is-operate? lst))) true
              (true? (is-arithmetic? lst)) true
              (not (nil? (is-operate? (rest lst)))) true
              (true? (is-arithmetic? (rest lst))) true
              (true? (is-float-int lst)) true
              :else
              false
              ) false))

(defn is-limit? [lst]
    (if-let [m (my-comma lst)]
        (if (and (= (count m) 3) (true? (is-express (nth m 0))) (= (nth m 1) ",") (true? (is-express (nth m 2))))
            true
            false) false))

; 获取 group by, order by, limit
(defn where-extend-line [[f & rs]]
    (if (some? f)
        (cond (and (is-eq? f "group") (is-eq? (first rs) "by"))
              (let [{group-by :group-by having :having rs-lst :rs-lst} (sql-group-by (rest rs))]
                  (let [m (where-extend-line rs-lst)]
                      (if (some? having) (assoc m :group-by group-by :having having) (assoc m :group-by group-by))))

              (and (is-eq? f "order") (is-eq? (first rs) "by"))
              (let [{order :order rs-lst :rs-lst} (order-by-items-line (rest rs))]
                  (let [m (where-extend-line rs-lst)] (assoc m :order-by order)))

              (is-eq? f "limit")
              (if (true? (is-limit? rs)) {:limit rs} (throw (Exception. "where 语句错误！")))

              :else
              (throw (Exception. "where 语句错误！"))
              )))

; 获取 select 语句对应的字符
(defn get-segments [select-sql]
    (when-let [lst (to-back select-sql)]
        (let [{query-items :query-items rs-lst-query :rs-lst} (query-items-line lst)]
            (if (some? rs-lst-query)
                (let [{table-items :table-items rs-lst-tables :rs-lst} (tables-items-line rs-lst-query)]
                    (if (some? rs-lst-tables)
                        (let [{where-items :where-items rs-lst-where :rs-lst} (where-items-line rs-lst-tables)]
                            (if (some? rs-lst-where)
                                (let [{group-by :group-by having :having order-by :order-by limit :limit} (where-extend-line rs-lst-where)]
                                    {:query-items query-items :table-items table-items :where-items where-items :group-by group-by :having having :order-by order-by :limit limit})
                                {:query-items query-items :table-items table-items :where-items where-items}))
                        {:query-items query-items :table-items rs-lst-query}))))))

(defn get-segments-list [lst]
    (if (some? lst)
        (let [{query-items :query-items rs-lst-query :rs-lst} (query-items-line lst)]
            (if (some? rs-lst-query)
                (let [{table-items :table-items rs-lst-tables :rs-lst my-type :my-type} (tables-items-line rs-lst-query)]
                    (cond
                        (and (empty? my-type) (empty? rs-lst-tables)) {:query-items query-items :table-items table-items}
                        (= my-type "where") (let [{where-items :where-items rs-lst-where :rs-lst} (where-items-line rs-lst-tables)]
                                                (if (some? rs-lst-where)
                                                    (let [{group-by :group-by having :having order-by :order-by limit :limit} (where-extend-line rs-lst-where)]
                                                        {:query-items query-items :table-items table-items :where-items where-items :group-by group-by :having having :order-by order-by :limit limit})
                                                    {:query-items query-items :table-items table-items :where-items where-items}))
                        (= my-type "order") (let [{order :order rs-lst :rs-lst} (order-by-items-line rs-lst-tables)]
                                                (if (> (count rs-lst) 0)
                                                    {:query-items query-items :table-items table-items :where-items nil :group-by nil :having nil :order-by order :limit rs-lst}
                                                    {:query-items query-items :table-items table-items :where-items nil :group-by nil :having nil :order-by order :limit nil}))
                        (= my-type "group") (let [{group-by :group-by having :having rs-lst :rs-lst} (sql-group-by rs-lst-tables)]
                                                (let [m (where-extend-line rs-lst)]
                                                    (if (some? having)
                                                        {:query-items query-items :table-items table-items :where-items nil :group-by group-by :having having :order-by (get m :order-by) :limit (get m :limit)}
                                                        {:query-items query-items :table-items table-items :where-items nil :group-by group-by :having nil :order-by (get m :order-by) :limit (get m :limit)})))
                        (= my-type "limit") {:query-items query-items :table-items table-items :where-items nil :group-by nil :having nil :order-by nil :limit rs-lst-tables}
                        ))))))

;(defn get-segments-list [lst]
;    (if (some? lst)
;        (let [{query-items :query-items rs-lst-query :rs-lst} (query-items-line lst)]
;            (if (some? rs-lst-query)
;                (let [{table-items :table-items rs-lst-tables :rs-lst} (tables-items-line rs-lst-query)]
;                    (if (some? rs-lst-tables)
;                        (let [{where-items :where-items rs-lst-where :rs-lst} (where-items-line rs-lst-tables)]
;                            (if (some? rs-lst-where)
;                                (let [{group-by :group-by having :having order-by :order-by limit :limit} (where-extend-line rs-lst-where)]
;                                    {:query-items query-items :table-items table-items :where-items where-items :group-by group-by :having having :order-by order-by :limit limit})
;                                {:query-items query-items :table-items table-items :where-items where-items}))
;                        {:query-items query-items :table-items rs-lst-query}))))))

; 切分 union 和 union all
(defn sql-union
    ([lst] (let [m (sql-union lst [] [] false)]
               (if (some? m) m [lst])))
    ([[f & rs] stack lst flag]
     (if (some? f)
         (cond (and (is-eq? f "union") (is-eq? (first rs) "all") (= (count stack) 0) (> (count lst) 0)) (concat [lst "union all"] (sql-union (rest rs) [] [] true))
               (and (is-eq? f "union") (not (is-eq? (first rs) "all")) (= (count stack) 0) (> (count lst) 0)) (concat [lst f] (sql-union rs [] [] true))
               (and (is-eq? f "MINUS") (= (count stack) 0) (> (count lst) 0)) (concat [lst f] (sql-union rs [] [] true))
               (and (is-eq? f "EXCEPT") (= (count stack) 0) (> (count lst) 0)) (concat [lst f] (sql-union rs [] [] true))
               (and (is-eq? f "INTERSECT") (= (count stack) 0) (> (count lst) 0)) (concat [lst f] (sql-union rs [] [] true))
               (= f "(") (sql-union rs (conj stack f) (conj lst f) flag)
               (= f ")") (if (> (count stack) 0) (sql-union rs (pop stack) (conj lst f) flag))
               :else
               (sql-union rs stack (conj lst f) flag)
               )
         (if (and (> (count lst) 0) (= flag true)) [lst])
         )))
;(def line "select \n                            (select emp_name from staff_info where empno=a.empno) as emp_name,\n                            c.description as description,\n                                           a.region_code\n                                      from lcs_dept_hiberarchy_trace b,\n                                           agent_info a,\n                                           agent_rank_tbl c,\n                                           (select emp_name from staff_info where empno=a.empno) as d\n                                     where a.empno = {c_empno}\n                                       and exists (select emp_name from staff_info where empno=a.empno)\n                                       GROUP BY b.authorid  HAVING my_count = 2 \n                                       order by c.region_grade desc, a.age asc  LIMIT 0, 2")
;(println (get-segments line))

(defn get-tokens [^String url]
    (letfn [(get-url-token
                ([lst] (get-url-token lst [] []))
                ([[f & r] stack lst]
                 (if (some? f)
                     (cond (and (= f \?) (> (count stack) 0)) (recur r [] (concat lst [(str/join stack) "?"]))
                           (and (= f \;) (> (count stack) 0)) (recur r [] (concat lst [(str/join stack) ";"]))
                           (contains? #{\space \( \) \* \- \, \+ \> \< \" \' \[ \] \{ \}} f) (throw (Exception. (format "连接字符串，不能包含字符 %s！" f)))
                           :else
                           (recur r (conj stack f) lst)
                           )
                     (if (> (count stack) 0)
                         (concat lst [(str/join stack)])
                         lst))))
            (url-tokens
                ([lst] (url-tokens lst [] [] []))
                ([[f & r] stack lst lst_rs]
                 (if (some? f)
                     (cond (and (is-eq? f "userToken") (= (count stack) 0)) (recur r (conj stack f) lst lst_rs)
                           (and (> (count stack) 0) (< (count stack) 3)) (if (= (count stack) 2)
                                                                             (recur r [] (conj stack f) lst_rs)
                                                                             (recur r (conj stack f) lst lst_rs))
                           (and (= f ";") (= (peek lst_rs) ";")) (recur r stack lst lst_rs)
                           (and (= f ";") (= (peek lst_rs) "?")) (recur r stack lst lst_rs)
                           (and (= f ";") (= (peek lst_rs) "/")) (recur r stack lst lst_rs)
                           :else
                           (recur r stack lst (conj lst_rs f))
                           )
                     (if (= (count lst) 3)
                         (if (contains? #{";" "?"} (last lst_rs))
                             ;{:userToken (peek lst) :url (str/join (reverse (rest (reverse lst_rs))))}
                             {:userToken (peek lst) :url (str/join (drop-last lst_rs))}
                             {:userToken (peek lst) :url (str/join lst_rs)})
                         (if (contains? #{";" "?"} (last lst_rs))
                             ;{:userToken "" :url (str/join (str/join (reverse (rest (reverse lst_rs)))))}
                             {:userToken "" :url (str/join (str/join (drop-last lst_rs)))}
                             {:userToken "" :url (str/join lst_rs)})))))]
        (loop [[f & r] (to-back url) lst []]
            (if (some? f)
                (recur r (concat lst (get-url-token f)))
                (url-tokens lst)))))

; url : "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true;userToken=abde123"
; 结果：{:userToken "abde123", :url "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true;"}
(defn -my_url_tokens [^String url]
    (try
        (if-let [{userToken :userToken my-url :url} (get-tokens url)]
            (MyUrlToken. userToken my-url))
        (catch Exception e nil))
    )

; 判断 jdbc thin
(defn -isJdbcThin [^String url]
    (if (re-find #"^(?i)jdbc\:ignite\:thin\://((25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.){3}(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\:10800\/" url)
        true
        false))

; 判断连接权限
(defn -hasConnPermission [^String sql]
    (if (re-find #"^(?i)hasConnPermission\([\s\S]+\)$" sql)
        true
        false))

; 输入 func-link 的 ast , 返回 func-link 字符串 和 参数列表
(defn my-func-line-code [m]
    (letfn [(token-to-code [token-ast]
                (if (some? token-ast)
                    (cond (is-seq? token-ast) (lst-token-to-code token-ast)
                          (map? token-ast) (map-token-to-code token-ast))))
            (lst-token-to-code [m]
                (loop [[f & r] m lst-line [] lst-ps []]
                    (if (some? f)
                        (let [{sql :sql args :args} (token-to-code f)]
                            (recur r (concat lst-line [sql]) (concat lst-ps args)))
                        {:sql (str/join lst-line) :args lst-ps})))
            (map-token-to-code [m]
                (if (some? m)
                    (cond (contains? m :func-link) (func-link-to-code m)
                          (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-code m)
                          (contains? m :operation) (operation-to-code m)
                          (contains? m :operation_symbol) {:sql (get m :operation_symbol) :args nil}
                          (contains? m :comma_symbol) {:sql (get m :comma_symbol) :args nil}
                          (contains? m :parenthesis) (parenthesis-to-code m)
                          (contains? m :item_name) (item-to-code m)
                          )))
            (re-str [line]
                (if (and (= (first line) \') (= (last line) \'))
                    (str/join (concat ["\""] (drop-last (rest line)) ["\""]))
                    line))
            (item-to-code [m]
                (let [{table_alias :table_alias item_name :item_name my-const :const java_item_type :java_item_type} m]
                    (cond (and (true? my-const) (= java_item_type java.lang.String)) {:sql (re-str item_name) :args nil}
                          (false? my-const) (if-not (Strings/isNullOrEmpty table_alias)
                                                {:sql (format "%s.%s" table_alias item_name) :args [(format "%s.%s" table_alias item_name)]}
                                                {:sql item_name :args [item_name]})
                          :else
                          {:sql item_name :args nil}
                          )))
            (operation-to-code [op-ast]
                (if (some? op-ast)
                    (loop [[f & r] (-> op-ast :operation) lst [] lst-args []]
                        (if (some? f)
                            (let [{sql :sql args :args} (token-to-code f)]
                                (recur r (concat lst [sql]) (concat lst-args args)))
                            {:sql (str/join lst) :args lst-args}))))
            (parenthesis-to-code [p-ast]
                (if (some? p-ast)
                    (loop [[f & r] (-> p-ast :parenthesis) lst [] lst-args []]
                        (if (some? f)
                            (let [{sql :sql args :args} (token-to-code f)]
                                (recur r (concat lst [sql]) (concat lst-args args)))
                            {:sql (str/join (str/join (concat ["("] lst [")"]))) :args lst-args}
                            ))))
            (func-to-code [func-ast]
                (loop [[f & r] (-> func-ast :lst_ps) lst [] lst-args []]
                    (if (some? f)
                        (let [{sql :sql args :args} (token-to-code f)]
                            (recur r (concat lst [sql]) (concat lst-args args)))
                        {:sql (str/join (concat [(-> func-ast :func-name) "("] lst [")"])) :args lst-args})))
            (func-link-to-code [func-link-ast]
                (loop [[f & r] (-> func-link-ast :func-link) lst [] lst-args []]
                    (if (some? f)
                        (let [{sql :sql args :args} (func-to-code f)]
                            (recur r (concat lst [sql]) (concat lst-args args)))
                        {:sql (str/join "." lst) :args lst-args})))]
        (token-to-code m)))
















