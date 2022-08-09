-- 扩展新的需求，对 user_group 添加缓存。当用 user_token 查询的时候，首先查询缓存，如果缓存不存在，才查询数据库，并将数据库中的缓存放入缓存
-- 1、创建查询用户组的方法 get_user_group
-- 4、在添加 add_user_group_ex 用户组的程序中，给这个用户赋予访问 get_user_group 的权限

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
                noSqlGet({"table_name": "user_group_cache", "key": user_token, "value": r});
                r;
             }
    }
}

-- 扩展新的需求
-- 添加 user_group 的时候更新缓存
function add_user_group_ex(group_name:string, user_token:string, group_type:string, data_set_name:string)
{
    -- 通过 data set name 获取 data set 的 id
    let data_set_id = get_data_set_id(data_set_name);
    match {
       has_user_token_type(group_type): match {
                                            -- data set id 大于 0 时，插入到 my_users_group 表中
                                            data_set_id > 0: query_sql("insert into my_users_group (id, group_name, data_set_id, user_token, group_type) values (?, ?, ?, ?, ?)", [auto_id("my_users_group"), group_name, data_set_id, user_token, group_type]);
                                            else false;
                                        }
    }
}