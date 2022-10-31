(ns org.gridgain.plus.init.smart-init
    (:require
        [org.gridgain.plus.tools.my-user-group :as my-user-group]
        [org.gridgain.plus.dml.my-load-smart-sql :as my-load-smart-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.configuration CacheConfiguration)
             (cn.plus.model.ddl MyCachePK MyCaches)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.init.SmartInit
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [mySmartInit [org.apache.ignite.Ignite] Object]]
        ))

; 1、判断 user_group_cache 是否存在，如果不存在就初始化
;(defn my-smart-init [^Ignite ignite]
;    (if-let [userToken (.getRoot_token (.configuration ignite))]
;        (if-let [user-group-cache (.cache ignite "my_caches")]
;            (if-not (.containsKey user-group-cache (MyCachePK. "MY_META" "user_group_cache"))
;                (my-load-smart-sql/load-smart-sql ignite (my-user-group/get_user_group ignite userToken) "-- 扩展新的需求，对 user_group 添加缓存。当用 user_token 查询的时候，首先查询缓存，如果缓存不存在，才查询数据库，并将数据库中的缓存放入缓存\n-- 1、创建查询用户组的方法 get_user_group\n-- 4、在添加 add_user_group_ex 用户组的程序中，给这个用户赋予访问 get_user_group 的权限\n-- 1、创建查询用户组的方法 get_user_group\n-- 首先、查询缓存，如果缓存中存在，则返回\n-- 如果没有查询到，就查询数据库并将查询的结果保存到，缓存中\n\n-- 创建一个 Cache\nnoSqlCreate({\"table_name\": \"user_group_cache\", \"is_cache\": true, \"mode\": \"replicated\", \"maxSize\": 10000});\n\n-- 判断是否存在 DDL, DML, ALL 这三个字符\nfunction has_user_token_type(user_token_type:string)\n{\n    let flag = false;\n    -- user_token_type 只能取\n    let lst = [\"ddl\", \"dml\", \"all\"];\n    match {\n        lst.contains?(user_token_type.toLowerCase()): flag = true;\n    }\n    flag;\n}\n\n-- 1、创建查询用户组的方法 get_user_group\n-- 首先、查询缓存，如果缓存中存在，则返回\n-- 如果没有查询到，就查询数据库并将查询的结果保存到，缓存中\nfunction get_user_group(user_token:string)\n{\n    let vs = noSqlGet({\"table_name\": \"user_group_cache\", \"key\": user_token});\n    match {\n        notEmpty?(vs): vs;\n        else let rs = query_sql(\"select g.id, g.schema_name, g.group_type from my_users_group as g where g.user_token = ?\", [user_token]);\n             let result;\n             for (r in rs)\n             {\n                -- 如果存在就保存在缓存中，并且返回\n                noSqlInsert({\"table_name\": \"user_group_cache\", \"key\": user_token, \"value\": r});\n                result = r;\n             }\n             result;\n    }\n}\n\nfunction get_user_token(group_name:string)\n{\n     let group_token;\n     let rs = query_sql(\"select m.id, m.user_token from my_users_group m where m.group_name = ?\", [group_name]);\n     for (r in rs)\n     {\n        group_token = r;\n     }\n     group_token;\n}\n\n-- 2、添加用户组，同时为用户组的 get_user_group\nfunction add_user_group(group_name:string, user_token:string, group_type:string, schema_name:string)\n{\n    match {\n        has_user_token_type(group_type): match {\n                                            notNullOrEmpty?(get_user_group(user_token)): \"已经存在了该 user_token 不能重复插入！\";\n                                            else\n                                                 let user_group_id = auto_id(\"my_users_group\");\n                                                 let lst = [[\"insert into my_users_group (id, group_name, schema_name, user_token, group_type) values (?, ?, ?, ?, ?)\", [user_group_id, group_name, schema_name, user_token, group_type]]];\n                                                 -- 同时对 user_group_id 赋予 get_user_group 的访问权限\n                                                 lst.add([\"insert into call_scenes (group_id, to_group_id, scenes_name) values (?, ?, ?)\", [0, user_group_id, \"get_user_group\"]]);\n                                                 -- 执行事务\n                                                 trans(lst);\n                                                 -- 添加访问权限\n                                                 -- 用户组只能访问自己数据集里面的 cache\n                                                 -- 用户组只能访问自己数据集里面的 cache\n                                                 my_view(group_name, format(\"select * from my_meta.my_caches where schema_name = '%s'\", schema_name));\n                                                 -- 用户只能查看别人赋予他的方法\n                                                 my_view(group_name, format(\"select * from my_meta.call_scenes where to_group_id = %s\", user_group_id));\n                                                 -- 用户组只能访问自己用户组里面的定时任务\n                                                 my_view(group_name, format(\"select * from my_meta.my_cron where group_id = '%s'\", user_group_id));\n                                                 -- 用户组只能访问自己用户组里面的 delete 权限视图\n                                                 my_view(group_name, format(\"select * from my_meta.my_delete_views where group_id = %s\", user_group_id));\n                                                 -- 用户组只能访问自己用户组里面的 insert 权限视图\n                                                 my_view(group_name, format(\"select * from my_meta.my_insert_views where group_id = %s\", user_group_id));\n                                                 -- 用户组只能访问自己用户组里面的 update 权限视图\n                                                 my_view(group_name, format(\"select * from my_meta.my_update_views where group_id = %s\", user_group_id));\n                                                 -- 用户组只能访问自己用户组里面的 select 权限视图\n                                                 my_view(group_name, format(\"select * from my_meta.my_select_views where group_id = %s\", user_group_id));\n                                                 -- 不能访问用户组，其它用户的 user_token\n                                                 my_view(group_name, \"select id, group_name, schema_name, group_type from my_meta.my_users_group\");\n                                         }\n    }\n}\n\n-- 修改用户组，在这里我们要修改的是 group_type\nfunction update_user_group(group_name:string, group_type:string)\n{\n    let user_token = get_user_token(group_name);\n    let vs = noSqlGet({\"table_name\": \"user_group_cache\", \"key\": user_token.last()});\n    match {\n       null?(vs): query_sql(\"update my_users_group set group_type = ? where group_name = ?\", [group_type, group_name]);\n       else let new_vs = vs.set(2, group_type);\n            let lst = [[\"update my_users_group set group_type = ? where group_name = ?\", [group_type, group_name]]];\n            lst.add([noSqlUpdateTran({\"table_name\": \"user_group_cache\", \"key\": user_token.last(), \"value\": new_vs})]);\n            -- 执行事务\n            trans(lst);\n    }\n}\n\n-- 删除用户组\nfunction delete_user_group(group_name:string)\n{\n    let user_token = get_user_token(group_name);\n\n    rm_view(group_name, 'my_meta.my_caches', 'select');\n    rm_view(group_name, 'my_meta.call_scenes', 'select');\n    rm_view(group_name, 'my_meta.my_cron', 'select');\n    rm_view(group_name, 'my_meta.my_delete_views', 'select');\n    rm_view(group_name, 'my_meta.my_insert_views', 'select');\n    rm_view(group_name, 'my_meta.my_update_views', 'select');\n    rm_view(group_name, 'my_meta.my_select_views', 'select');\n    rm_view(group_name, 'my_meta.my_users_group', 'select');\n\n    let lst = [[\"delete from my_users_group where group_name = ?\", [group_name]]];\n    lst.add([\"delete from call_scenes where to_group_id = ?\", [user_token.first()]]);\n    lst.add(noSqlDeleteTran({\"table_name\": \"user_group_cache\", \"key\": user_token.last()}));\n    --lst.add([noSqlDeleteTran({\"table_name\": \"user_group_cache\", \"key\": user_token})]);\n    -- 执行事务\n    trans(lst);\n\n}\n")))))

(defn my-smart-init [^Ignite ignite]
    (if-let [userToken (.getRoot_token (.configuration ignite))]
        (if-let [user-group-cache (.cache ignite "my_caches")]
            (if-not (.containsKey user-group-cache (MyCachePK. "MY_META" "user_group_cache"))
                (my-load-smart-sql/load-smart-sql ignite (my-user-group/get_user_group ignite userToken) "noSqlCreate({\"table_name\": \"user_group_cache\", \"is_cache\": true, \"mode\": \"replicated\", \"maxSize\": 10000});")))))


(defn -mySmartInit [^Ignite ignite]
    (my-smart-init ignite))
































