(ns org.gridgain.plus.user.my-user
    (:require
        [org.gridgain.plus.tools.my-cache :as my-cache]
        [org.gridgain.plus.tools.my-util :as my-util]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.tools MyDbUtil MyTools)
             (java.nio.file Files Paths)
             java.net.URI
             org.gridgain.dml.util.MyCacheExUtil)
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.user.MyUser
        ; 是否生成 class 的 main 方法
        :main false
        :state state
        ; init 构造函数
        :init init
        ; 构造函数
        :constructors {[org.apache.ignite.Ignite] []}
        ; 生成 java 调用的方法
        :methods [[userLogin [String String] java.util.List]
                  [rootLogin [String String String] java.util.List]]
        ))

; 构造函数
(defn -init [^Ignite ignite]
    [[] (atom {:ignite ignite})])

; 如果在表中没有查询到用户名和密码，那么就查询是否是超级管理员
(defn get-root-user [cfgPath]
    (-> cfgPath (URI.) (Paths/get) (Files/readAllBytes) (MyCacheExUtil/restore)))

(defn -userLogin [this user_name pass_word]
    (when-let [rs (MyDbUtil/runMetaSql (@(.state this) :ignite) "select m.id, m.group_id from my_user as m where m.user_name = ? and m.pass_word = ?" [user_name pass_word])]
        (first rs)))

(defn -rootLogin [this user_name pass_word cfgPath]
    (when-let [root_user (get-root-user cfgPath)]
        (if (some? root_user)
            (if (and (.equals user_name (.getRootName root_user)) (.equals pass_word (.getPassWord root_user)))
                [0 -1])
            )))

; 输入 user_name 和 pass world 获取 ID
;(defn -login [this user_name pass_word cfgPath]
;    (when-let [rs (MyDbUtil/runMetaSql (@(.state this) :ignite) "select m.id, m.group_id from my_user as m where m.user_name = ? and m.pass_word = ?" [user_name pass_word])]
;        (if (some? rs)
;            (first rs)
;            (let [root_user (get-root-user cfgPath)]
;                (if (some? root_user)
;                    (do [0 -1] (println root_user))
;                    nil)))))

; 判断是否是 user
;(defn user-login [ignite user_name pass_word]
;    (when-let [rs (MyDbUtil/runMetaSql ignite "select m.id, m.group_id from my_user as m where m.user_name = ? and m.pass_word = ?" [user_name pass_word])]
;        (first rs)))
;
;(defn root-login [user_name pass_word cfgPath]
;    (when-let [root_user (get-root-user cfgPath)]
;        (if (some? root_user)
;            (if (and (.equals user_name (.getRootName root_user)) (.equals pass_word (.getPassWord root_user)))
;                [0 -1])
;            )))

;(defn -login [this user_name pass_word cfgPath]
;    (let [u (user-login (@(.state this) :ignite) user_name pass_word)]
;        (if (some? u)
;            u
;            (root-login user_name pass_word cfgPath))))







































