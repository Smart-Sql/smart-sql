-- 扩展新的需求，对 user_group 添加缓存。当用 user_token 查询的时候，首先查询缓存，如果缓存不存在，才查询数据库，并将数据库中的缓存放入缓存
-- 1、创建查询用户组的方法 get_user_group
-- 4、在添加 add_user_group_ex 用户组的程序中，给这个用户赋予访问 get_user_group 的权限
-- 1、创建查询用户组的方法 get_user_group
-- 首先、查询缓存，如果缓存中存在，则返回
-- 如果没有查询到，就查询数据库并将查询的结果保存到，缓存中

-- 创建一个 Cache
--noSqlCreate({"table_name": "user_group_cache", "is_cache": true, "mode": "replicated", "maxSize": 10000});

-- 输入 data set name 获取 data set 的 id
function get_data_set_id(name:string)
{
    let id;
    -- 使用 query_sql 访问数据库读取 id
    for(r in query_sql("select m.id from my_dataset m where m.dataset_name = ?", name))
    {
        -- 读取出来的为序列，因为只有一列，所以我们就只取第一个
        id = r.first();
    }
    -- SmartSql 默认最后一条语句为返回值，所以必要有 id;
    id;
}

-- 判断是否存在 DDL, DML, ALL 这三个字符
function has_user_token_type(user_token_type:string)
{
    let flag = false;
    -- user_token_type 只能取
    let lst = ["ddl", "dml", "all"];
    match {
        lst.contains?(user_token_type.toLowerCase()): flag = true;
    }
    flag;
}

-- 1、创建查询用户组的方法 get_user_group
-- 首先、查询缓存，如果缓存中存在，则返回
-- 如果没有查询到，就查询数据库并将查询的结果保存到，缓存中
function get_user_group(user_token:string)
{
    let vs = noSqlGet({"table_name": "user_group_cache", "key": user_token});
    match {
        notEmpty?(vs): vs;
        else let rs = query_sql("select g.id, m.dataset_name, g.group_type, m.id from my_users_group as g, my_dataset as m where g.data_set_id = m.id and g.user_token = ?", [user_token]);
             for (r in rs)
             {
                -- 如果存在就保存在缓存中，并且返回
                noSqlInsert({"table_name": "user_group_cache", "key": user_token, "value": r});
                r;
             }
    }
}

-- 2、添加用户组，同时为用户组的 get_user_group
function add_user_group(group_name:string, user_token:string, group_type:string, data_set_name:string)
{
    match {
        has_user_token_type(group_type): match {
                                            notNullOrEmpty?(get_user_group(user_token)): "已经存在了该 user_token 不能重复插入！";
                                            else  -- 通过 data set name 获取 data set 的 id
                                                  let data_set_id = get_data_set_id(data_set_name);
                                                  match {
                                                      data_set_id > 0:
                                                                       -- data set id 大于 0 时，插入到 my_users_group 表中
                                                                       let user_group_id = auto_id("my_users_group");
                                                                       let lst = [["insert into my_users_group (id, group_name, data_set_id, user_token, group_type) values (?, ?, ?, ?, ?)", [user_group_id, group_name, data_set_id, user_token, group_type]]];
                                                                       -- 同时对 user_group_id 赋予 get_user_group 的访问权限
                                                                       lst.add(["insert into call_scenes (group_id, to_group_id, scenes_name) values (?, ?, ?)", [0, user_group_id, "get_user_group"]]);
                                                                       -- 执行事务
                                                                       trans(lst);
                                                                       -- 添加访问权限
                                                                       -- 用户组只能访问自己数据集里面的 cache
                                                                       -- 用户组只能访问自己数据集里面的 cache
                                                                       my_view(group_name, format("select * from my_caches where dataset_name = '%s'", data_set_name));
                                                                       -- 用户只能查看别人赋予他的方法
                                                                       my_view(group_name, format("select * from call_scenes where to_group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的定时任务
                                                                       my_view(group_name, format("select * from my_cron where group_id = '%s'", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 delete 权限视图
                                                                       my_view(group_name, format("select * from my_delete_views where group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 insert 权限视图
                                                                       my_view(group_name, format("select * from my_insert_views where group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 update 权限视图
                                                                       my_view(group_name, format("select * from my_update_views where group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 select 权限视图
                                                                       my_view(group_name, format("select * from my_select_views where group_id = %s", user_group_id));
                                                                       -- 不能访问用户组，其它用户的 user_token
                                                                       my_view(group_name, "select id, group_name, data_set_id, group_type from my_users_group");
                                                      else false;
                                                  }
                                         }
    }
}

-- 修改用户组，在这里我们要修改的是 group_type
function update_user_group(user_token:string, group_type:string)
{
    match {
       has_user_token_type(group_type): let vs = noSqlGet({"table_name": "user_group_cache", "key": user_token});
                                        let new_vs = vs.set(2, group_type);
                                        let lst = [["update my_users_group set group_type = ? where group_type = ?", [group_type, user_token]]];
                                        lst.add([noSqlUpdateTran({"table_name": "user_group_cache", "key": user_token, "value": new_vs})]);
                                        -- 执行事务
                                        trans(lst);
    }
}

-- 删除用户组
function delete_user_group(user_token:string, group_type:string)
{
    match {
       has_user_token_type(group_type): let lst = [["delete from my_users_group where group_type = ?", [user_token]]];
                                        lst.add([noSqlDeleteTran({"table_name": "user_group_cache", "key": user_token})]);
                                        -- 执行事务
                                        trans(lst);
    }
}
