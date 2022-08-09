-- 创建一个 Cache
noSqlCreate({"table_name": "user_group_cache", "is_cache": true, "mode": "replicated", "maxSize": 10000});

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

-- 添加用户组
function add_user_group(group_name:string, user_token:string, group_type:string, data_set_name:string)
{
    -- 通过 data set name 获取 data set 的 id
    let data_set_id = get_data_set_id(data_set_name);
    match {
        -- data set id 大于 0 时，插入到 my_users_group 表中
        data_set_id > 0: query_sql("insert into my_users_group (id, group_name, data_set_id, user_token, group_type) values (auto_id(?), ?, ?, ?, ?)", ["my_users_group", group_name, data_set_id, user_token, group_type]);
        else false;
    }
}

-- 判断是否存在 DDL, DML, ALL 这三个字符
function has_user_token_type(user_token_type:string)
{
    let flag = false;
    -- user_token_type 只能取
    let lst = ["ddl", "dml", "all"];
    match {
        lst.contains(user_token_type.toLowerCase()): flag = true;
    }
    flag;
}

/**
作者：陈飞
添加用户组
*/
function add_user_group_1(group_name:string, user_token:string, group_type:string, data_set_name:string)
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
