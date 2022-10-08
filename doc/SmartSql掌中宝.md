## Smart 中的函数

### 1、访问数据库

| 函数名    | 说明                                               |
| --------- | -------------------------------------------------- |
| query_sql | 在 Smart Sql 访问数据库的方法，只能执行 DML 语句。 |

**用法有三种：**

```sql
-- 1、输入 sql 和参数
query_sql("select m.id from my_dataset m where m.dataset_name = ?", name);

-- 2、输入 sql 和 参数列表
query_sql("select m.id from my_dataset m where m.dataset_name = ?", [name]);

-- 3、输入 sql 没有参数
query_sql("select m.id from my_dataset m where m.dataset_name = '吴大富'");
```

| 函数名 | 说明                                             |
| ------ | ------------------------------------------------ |
| trans  | 在 Smart Sql 执行事务的语句，支持 sql 和 noSql。 |

**trans 只接受 List 对象**
List 中的 sql 和参数也是列表的：[sql语句1, [参数列表]], [sql语句2, [参数列表]], ..., [sql语句n, [参数列表]]

如果是 no sql 的，就直接调用 no sql 执行事务的方法。
例如：[sql语句1, [参数列表]], noSqlUpdateTran(), ..., [sql语句n, [参数列表]]

```sql
-- 1、对多个 sql 执行事务
let user_group_id = auto_id("my_users_group");
let lst = [["insert into my_users_group (id, group_name, data_set_id, user_token, group_type) values (?, ?, ?, ?, ?)", [user_group_id, group_name, data_set_id, user_token, group_type]]];
-- 同时对 user_group_id 赋予 get_user_group 的访问权限
lst.add(["insert into call_scenes (group_id, to_group_id, scenes_name) values (?, ?, ?)", [0, user_group_id, "get_user_group"]]);
-- 执行事务
trans(lst);

-- 2、对多个 no sql 执行事务
let lst = [noSqlUpdateTran({"table_name": "user_group_cache", "key": user_token.last(), "value": new_vs})];
lst.add(noSqlDeleteTran({"table_name": "user_group_cache", "key": user_token.first()}));
-- 执行事务
trans(lst);

-- 3、对 sql 和 no sql 同时执行事务
let lst = [["update my_users_group set group_type = ? where group_name = ?", [group_type, group_name]]];
lst.add(noSqlUpdateTran({"table_name": "user_group_cache", "key": user_token.last(), "value": new_vs}));
-- 执行事务
trans(lst);
```

### 2、No Sql 的方法
No Sql 是依 key-value 形式存在的，也是按照 key-value 来操作的。

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlCreate | 创建 No Sql 的函数 |

No Sql 分为两类：
1. 纯内存模式。这个用于需要高性能的场景。用户可以根据业务的要求来设置纯内存模式的内存大小，退出机制等。**纯内存模式，它也是事务性的，也就是说 Smart Sql 不存在，缓存和数据库双写一致性的问题**
2. 持久化模式。持久化模式是把数据全部持久化到磁盘，要用的时候，在放到内存中，当查询数据时，现在内存中找，找不到，在到磁盘中找。

**在开发高性能应用程序时，我们要经历使用 No Sql 这种模式，因为它是 key-value 形式的，通过 key 来直接查找的性能要远高于 SQL**

1. 创建纯内存模式的 No Sql
```sql
-- 创建一个表名为 user_group_cache 的存内存模式
-- is_cache: 设置为 true 表示纯内存模式，
-- mode: replicated 表示复制模式，partitioned 表示分区模式
-- maxSize: 最多能放多少条数据
noSqlCreate({"table_name": "user_group_cache", "is_cache": true, "mode": "replicated", "maxSize": 10000});
```

2. 创建持久化模式的 No Sql
```sql
-- 创建一个表名为 user_group_cache 的存内存模式
-- is_cache: 设置为 false 表示纯内存模式，
-- mode: replicated 表示复制模式，partitioned 表示分区模式
-- 因为是持久化所以不需要设置 maxSize
noSqlCreate({"table_name": "user_group_cache", "is_cache": true, "mode": "replicated"});
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlGet | 获取 no sql 的值 |

```sql
-- 查询 user_group_cache 表 key = wudafu_token 的值
noSqlGet({"table_name": "user_group_cache", "key": "wudafu_token"});
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlInsert | 插入 no sql 的数据 |

```sql
-- 为表 my_cache 插入 key = "000A", value = {"name": "吴大富", "age": 100} 的数据
noSqlInsert({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlInsert | 插入 no sql 的数据 |

```sql
-- 为表 my_cache 插入 key = "000A", value = {"name": "吴大富", "age": 100} 的数据
noSqlInsert({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlInsertTran | 插入 no sql 的数据，在事务中使用 |

```sql
-- 为表 my_cache 插入 key = "000A", value = {"name": "吴大富", "age": 100} 的数据
let lst = [];
lst.add(...);
lst.add(noSqlInsertTran({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}}));
lst.add(...);
trans(lst);
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlUpdate | 更新 no sql 的数据 |

```sql
-- 为表 my_cache 修改 key = "000D", value = {"name": "吴大贵", "age": 600} 的数据
noSqlUpdate({"table_name": "my_cache", "key": "000D", "value": {"name": "吴大贵", "age": 600}});
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlUpdateTran | 更新 no sql 的数据，在事务中使用 |

```sql
-- 为表 my_cache 插入 key = "000A", value = {"name": "吴大富", "age": 100} 的数据
let lst = [];
lst.add(...);
lst.add(noSqlUpdateTran({"table_name": "my_cache", "key": "000D", "value": {"name": "吴大贵", "age": 600}}));
lst.add(...);
trans(lst);
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlDelete | 删除 no sql 的数据 |

```sql
-- 为表 my_cache 删除 key = "000D" 的数据
noSqlDelete({"table_name": "my_cache", "key": "000D"});
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlDeleteTran | 删除 no sql 的数据，在事务中使用 |

```sql
-- 为表 my_cache 插入 key = "000A", value = {"name": "吴大富", "age": 100} 的数据
let lst = [];
lst.add(...);
lst.add(noSqlDeleteTran({"table_name": "my_cache", "key": "000D"}));
lst.add(...);
trans(lst);
```

| 函数        | 说明               |
| ----------- | ------------------ |
| noSqlDrop | 删除 no sql 的数据表 |

```sql
-- 删除 my_cache 表
noSqlDrop({"table_name": "my_cache"});
```

### 3、将一个数据集中的场景，添加或删除给另一用户组
```sql
-- 将用户组ID为 0 的 get_data_set_id 调用权限转移给，用户组ID 为 1 的
add_scenes_to(0, 'get_data_set_id', 1);
```

```sql
-- 将用户组ID为 0 的 get_data_set_id 调用权限从，用户组ID 为 1 的收回
rm_scenes_from(0, 'get_data_set_id', 1);
```

### 4、权限视图
添加权限视图
```sql
-- 为用户组添加访问 my_meta.my_caches 的权限。当用户组，要读取 my_meta.my_caches 表的信息是，只能在 dataset_name = 'my_ds' 的数据中查找
my_view(group_name, "select * from my_meta.my_caches where dataset_name = 'my_ds'");
```

删除权限视图

```sql
-- 删除用户组添加访问 my_meta.my_caches 的限制
rm_view(group_name, 'my_meta.my_caches', 'select');
```

### 5、定时任务的方法
```sql
-- 添加定时任务
add_job('get_cron_time', [], '{2,30} * * * * *');
-- 删除定时任务
remove_job('get_cron_time');
-- 获取定时任务快照
job_snapshot('get_cron_time');
```

### 6、注册和卸载扩展的方法
**只有 root 用户才有权限**
```sql
-- 注册扩展方法到 Smart Sql
add_func({"method_name":"my_println","java_method_name":"myPrintln","cls_name":"org.example.plus.MyPulsTime","return_type":"void","descrip":"打印输入结果","lst":[{"ps_index":1,"ps_type":"String"}]});

-- 从 Smart Sql 中删除扩展方法
remove_func("my_println");
```

### 7、恢复函数
**只有 root 用户才有权限**
```sql
-- 这个方法建议在 jdbc 中使用
recovery_to_cluster(二进制对象);
```

### 8、加载 Smart Sql 的脚本
```sql
-- 这个方法建议在 jdbc 中使用
loadCode('Smart Sql 的脚本');
```

### 9、Smart Sql 语言常用方法
#### 9.1、操作集合的方法

| 函数                        | 说明                                                         |
| --------------------------- | ------------------------------------------------------------ |
| add(item);                  | 为序列添加 item                                              |
| set(index, item);           | 将序列中第 index 项的值修改成 item                           |
| take(len);                  | 获取序列中 0 到 len 项的数据                                 |
| drop(index);                | 删除序列中 0 到 len 项的数据，并返回剩余的                   |
| nth(index);                 | 获取序列中 index 项的数据                                    |
| count();                    | 返回序列大小                                                 |
| concat(lst, lst, .., lstn); | 如果输入的是序列，序列合并成一个。如果是字符串，就连接在一起 |
| contains?(item)             | 判断序列有 item 值，或者是hashtable 是否有 item 的key        |
| put(key, item);             | 将 key -item 保存到 hashtable                                |
| get(item);                  | 获取序列中的 item索引，或者是获取 hashtable 的 key = item 的数据。 |
| remove(item);               | 删除序列中的 item数据，或者是删除 hashtable 的 key = item 的数据。 |
| pop();                      | 弹出序列最后一个 item                                        |
| peek(); 和 last(); 一样     | 获取序列最后一个 item                                        |
| takeLast(num);              | 获取序列最后 num 个 item，后返回剩余的的                     |
| dropLast(num);              | 删除序列最后 num 个 item，后返回剩余的的                     |

#### 9.2、判断的函数

| 函数                | 说明                                                  |
| ------------------- | ----------------------------------------------------- |
| null?(m)            | 判断 m 是否为空                                       |
| notNull?(m)         | 判断 m 是否不为空                                     |
| empty?(list)        | 判断 list 序列是否为空                                |
| notEmpty?(list)     | 判断 list 序列是否不为空                              |
| nullOrEmpty?(m);    | 判断对象 m 是否为空。m 可以是序列，字符串或其它对象   |
| notNullOrEmpty?(m); | 判断对象 m 是否不为空。m 可以是序列，字符串或其它对象 |

#### 9.3、字符串和正则式函数

| 函数名      | 说明                                                         |
| ----------- | ------------------------------------------------------------ |
| format      | 字符串的 format 方法，用法：format('%s是大帅哥！', '吴大富'); |
| str_replace | 替换字符串。将 red 替换成 blue：str_replace('The color is red', regular('red'), 'blue'); 输入正则式需要 regular 函数，例如：regular('red')。 |
| str_split   | 分割字符串。例如：按数字将字符串分割 "abc12de3fg5h"：str_split('abc12de3fg5h', regular('\\d+'));  结果：["abc" "de" "fg" "h"] |
| str_find    | 找字符串，str_find(regular('\\d+'), 'abc12de3fg5h');         |
| lower       | 将字符串全部转换为小写：lower('Abc');                        |
| upper       | 将字符串全部转换为大写：upper('Abc');                        |

#### 9.4、常用方法

| 函数名 | 说明                                                         |
| ------ | ------------------------------------------------------------ |
| range  | 根据输入的数字生成相应的序列，这个主要用于，for 循环中需求序列下标的场景 |
|        | range(int);  例如： range(5);                                |
|        | range(int, int);  例如： range(5, 10);                       |





















