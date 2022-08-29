##  Smart Sql 的设计
SmartSql 是一个超融合的新函数式理念，它要实现的功能相当于 分布式(mysql/pg) + 大数据体系 + 分布式缓存 + 应用程序。

### Smart Sql 的设置

#### 1、设置多用户组

![multiUserGroup](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/multiUserGroup.jpg)

设置多用户组为 true 后，就可以设置不同的数据集 (data set)。数据集是数据表的集合，也就是说数据集包含了一个或多个表。一个用户组只属于一个 DataSet。另外还有一个 public 数据集为公共数据集。
*数据集可以理解为一个子系统的数据库*
**具体的操作**

#### 1.1、添加数据集（Date Set）
```sql
-- 1、
-- 新增数据集 myy
create dataset if not exists myy;
-- 或者
create dataset myy;

-- 2、
-- 新增数据集 wudafu
create dataset if not exists wudafu;
-- 或者
create dataset wudafu;

-- 3、
-- 新增数据集 wudagui
create dataset if not exists wudagui;
-- 或者
create dataset wudagui;
```

如果要删除数据集
```sql
-- 1、
-- 删除数据集 myy
drop dataset if exists myy;
-- 或者
drop dataset myy;

-- 2、
-- 删除数据集 wudafu
drop dataset if exists wudafu;
-- 或者
drop dataset wudafu;

-- 3、
-- 删除数据集 wudagui
drop dataset if exists wudagui;
-- 或者
drop dataset wudagui;
```

#### 1.2、添加用户组。(需要 root 权限)
```sql
add_user_group(group_name, user_token, group_type, data_set_name);
```

内置方法 add_user_group 的实现：
```sql
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
             let result;
             for (r in rs)
             {
                -- 如果存在就保存在缓存中，并且返回
                noSqlInsert({"table_name": "user_group_cache", "key": user_token, "value": r});
                result = r;
             }
             result;
    }
}

function get_user_token(group_name:string)
{
     let group_token;
     let rs = query_sql("select m.id, m.user_token from my_users_group m where m.group_name = ?", [group_name]);
     for (r in rs)
     {
        group_token = r;
     }
     group_token;
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
                                                                       my_view(group_name, format("select * from my_meta.my_caches where dataset_name = '%s'", data_set_name));
                                                                       -- 用户只能查看别人赋予他的方法
                                                                       my_view(group_name, format("select * from my_meta.call_scenes where to_group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的定时任务
                                                                       my_view(group_name, format("select * from my_meta.my_cron where group_id = '%s'", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 delete 权限视图
                                                                       my_view(group_name, format("select * from my_meta.my_delete_views where group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 insert 权限视图
                                                                       my_view(group_name, format("select * from my_meta.my_insert_views where group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 update 权限视图
                                                                       my_view(group_name, format("select * from my_meta.my_update_views where group_id = %s", user_group_id));
                                                                       -- 用户组只能访问自己用户组里面的 select 权限视图
                                                                       my_view(group_name, format("select * from my_meta.my_select_views where group_id = %s", user_group_id));
                                                                       -- 不能访问用户组，其它用户的 user_token
                                                                       my_view(group_name, "select id, group_name, data_set_id, group_type from my_meta.my_users_group");
                                                      else false;
                                                  }
                                         }
    }
}

-- 修改用户组，在这里我们要修改的是 group_type
function update_user_group(group_name:string, group_type:string)
{
    let user_token = get_user_token(group_name);
    let vs = noSqlGet({"table_name": "user_group_cache", "key": user_token.last()});
    match {
       null?(vs): query_sql("update my_users_group set group_type = ? where group_name = ?", [group_type, group_name]);
       else let new_vs = vs.set(2, group_type);
            let lst = [["update my_users_group set group_type = ? where group_name = ?", [group_type, group_name]]];
            lst.add([noSqlUpdateTran({"table_name": "user_group_cache", "key": user_token.last(), "value": new_vs})]);
            -- 执行事务
            trans(lst);
    }
}

-- 删除用户组
function delete_user_group(group_name:string)
{
    let user_token = get_user_token(group_name);

    rm_view(group_name, 'my_meta.my_caches', 'select');
    rm_view(group_name, 'my_meta.call_scenes', 'select');
    rm_view(group_name, 'my_meta.my_cron', 'select');
    rm_view(group_name, 'my_meta.my_delete_views', 'select');
    rm_view(group_name, 'my_meta.my_insert_views', 'select');
    rm_view(group_name, 'my_meta.my_update_views', 'select');
    rm_view(group_name, 'my_meta.my_select_views', 'select');
    rm_view(group_name, 'my_meta.my_users_group', 'select');

    let lst = [["delete from my_users_group where group_name = ?", [group_name]]];
    lst.add(["delete from call_scenes where to_group_id = ?", [user_token.first()]]);
    lst.add(noSqlDeleteTran({"table_name": "user_group_cache", "key": user_token.last()}));
    --lst.add([noSqlDeleteTran({"table_name": "user_group_cache", "key": user_token})]);
    -- 执行事务
    trans(lst);

}
```
*实际上用户可以自己实现这些方法*

**注意**
**如果是使用 DBeaver 可以使用一个特有的函数 loadFromNative(本地 sql 文件的地址) 将 sql 文件的里面的代码，全部放到服务器端执行。**

![loadFromNative](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/loadFromNative.jpg)
user_ds.sql 里面的内容：

![user_ds](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/user_ds.jpg)

**执行 loadFromNative 程序后，可以在 MY_META 数据集的 MY_SCENES 表中查看是否已经运行成功了。例如：上面代码的运行结果：
**
![smart_sql_my_scenes](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/smart_sql_my_scenes.jpg)

例如：我们添加三个数据集，并且给它们添加用户组，这里用 add_user_group 方法来添加用户组。add_user_group 需要输入四个参数：group_name: 用户组的名称，user_token：用户组连接数据库的 token ，
```sql
create dataset wudafu;
add_user_group('myy_group', 'myy_token', 'all', 'myy');

create dataset wudafu;
add_user_group('wudafu_group', 'wudafu_token', 'all', 'wudafu');

create dataset wudagui;
add_user_group('wudagui_group', 'wudagui_token', 'all', 'wudagui');
```

#### 1.3、为用户组添加访问数据集和表的权限。(需要 root 权限)
```sql
-- 为用户组：wudafu_group，添加插入 public.Categories 表的权限。
-- 让其只能添加 CategoryName 和 Description 字段的数据
my_view('wudafu_group', 'INSERT INTO public.Categories(CategoryName, Description)');

-- 为用户组：wudafu_group，添加修改 public.Categories 表的权限。
-- 让其只能修改 CategoryName = 'Produce' 的 Description 字段
my_view('wudafu_group', "UPDATE public.Categories set Description where CategoryName = 'Produce'");

-- 为用户组：wudafu_group，添加删除 public.Categories 表的权限。
-- 让其只能删除 CategoryName <> 'Seafood' 的数据
my_view('wudafu_group', "DELETE from public.Categories where CategoryName <> 'Seafood'");

-- 为用户组：wudafu_group，添加查询 public.Categories 表的权限。
-- 让其只能查询 CategoryName <> 'Seafood' 且只能查询列 CategoryName, Description 的数据
my_view('wudafu_group', "SELECT CategoryName, Description from public.Categories where CategoryName <> 'Seafood'");

-- 为用户组：wudafu_group，添加查询 public.Categories 表的权限。
-- 让其只能查询 CategoryName <> 'Seafood' 且只能查询列 CategoryName, Description 的数据，并且将 CategoryName 的数据转换成 f(CategoryName) 
-- f 为函数
my_view('wudafu_group', "SELECT convert_to(CategoryName, f(CategoryName)), Description from public.Categories where CategoryName <> 'Seafood'");
```

#### 1.4、为用户组添加访问数据集和表的权限。(需要 root 权限)
```sql
-- 删除用户组 wudafu_group 对表 public.Categories 插入的权限
rm_view('wudafu_group', 'public.Categories', 'insert');

-- 删除用户组 wudafu_group 对表 public.Categories 修改的权限
rm_view('wudafu_group', 'public.Categories', 'update');

-- 删除用户组 wudafu_group 对表 public.Categories 删除的权限
rm_view('wudafu_group', 'public.Categories', 'delete');

-- 删除用户组 wudafu_group 对表 public.Categories 查询的权限
rm_view('wudafu_group', 'public.Categories', 'select');
```
**用户组权限设置的原理：用户组读取或写数据的时候，sql 语句首先会被解析成语法树，在和用户操作该表的权限语法树合并成一个新的语法树**

#### 2、不设置多用户组
不设置用户组，所有创建的表都在 public 数据集中。不设置多用户组下面的功能就不能使用。
> 1、不能创建数据集
> 2、不能创建其它用户组
> 3、用户组都不存在了，也就没有办法为用户组设置权限
> 

#### 3、设置 Log 保存的程序

![myLogCls](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/myLogCls.jpg)

**用于两地三中心，这种高可用场景。这个程序需要用户自己来实现。因为不同的用户对这个问题有不同的需求。**

#### 3.1、具体的实现过程：
>1、实现接口 cn.smart.service.IMyLogTransaction
>2、打包这个程序，将 jar 包放到集群中，各个安装文件的 cls 文件夹中即可
>

**SmartSql 是分布式数据库，程序可能在每台机器上运行，所以必须在每台机器，SmartSql 安装文件的 cls 文件夹中，放入实现了 IMyLogTransaction 接口的自定义 Log 程序。这个程序的作用是将 insert、update、delete、ddl 的操作数据转换为二进制，根据自定义的程序来保存起来。在 SmartSql 运维中，我们会给出一个例子。**

### 4、对 sql 的支持
#### 4.1、DLL 的支持
##### 创建表

```sql
CREATE TABLE [IF NOT EXISTS] tableName (tableColumn [, tableColumn]...
[, PRIMARY KEY (columnName [,columnName]...)])
[WITH "paramName=paramValue [,paramName=paramValue]..."]

tableColumn := columnName columnType [DEFAULT(defaultValue)] [PRIMARY KEY] [auto] [comment("注释")]
```
参数：<br/>
***
1. tableName：表的名字

2. tableColumn：在新表中创建列的名称和类型

3. columnName：先前定义列的名称

4. DEFAULT：指定列的默认值。 仅接受常量值。

5. IF NOT EXISTS：仅当不存在具有相同名称的表时才创建表。

6. PRIMARY KEY：指定主键，主键可以有单列和多列组成。(主键具有唯一性)

7. auto：标识主键为自增长。(只有一个主键，且主键数据类型，必须是 int, long 否则会失效)

8. with:  附加参数非 ANSI-99 SQL 标准。两个值 template=XXX,affinity_key=YYY。

   XXX 取值是通过配置文件设置的。这个根据实际项目的情况来设置。
   例如：
   

![create_table_template](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/create_table_template.jpg)


| 值   | 说明                                         |
| ---- | -------------------------------------------- |
| base | 表示表为复制模式，既每个节点都有一份完整的表 |
| manage | 表示表为分区模式，既表中的数据平均到集群中的每个节点，<br/>为了保证数据的完整性和集群的可用性，这种模式会备份三份 |

**推荐例子**
<span style="color: red; font-weight: bolder;">强烈建议在开发中，直接复制例子中的代码，在此基础上修改。<br/>因为 SmartSql 的一些细微的写法和标准 sql 有少许的区别！</span>
**创建表**
```sql
/*
  创建 Person 表，id 和 city_id 为主键，且 city_id 为关联键
*/
CREATE TABLE IF NOT EXISTS Person (
  id int,
  city_id int,
  name varchar,
  age int,
  company varchar,
  PRIMARY KEY (id, city_id)
) WITH "template=manage,affinity_key=city_id";
```

```sql
/*
  创建 Person 表，id 为主键。并且设置 id 为自增长。
  自增长的意思就是插入的时候，不用插入这个值，有数据库自己来插入唯一的值。
  只有一个主键就没有关联键了。
*/
CREATE TABLE IF NOT EXISTS Person (
  id int auto comment('主键'),
  name varchar comment('名字'),
  /*
    年龄的默认值为 0，并且添加了注释
  */
  age int default(0) comment('年龄'),
  company varchar comment('公司'),
  PRIMARY KEY (id)
) WITH "template=manage";
```

<span style="color: red; font-weight: bolder;">
在创建表时，标准 SQL 和 Smart Sql 的区别有三点：
<br/>
1、必须添加 with，因为 SMart Sql 是分布式数据库，所以必须要指明它在集群中的样子。<br/>
2、如果是在本数据集，创建表可以不加数据集的名字，如果不是就要添加上，例如：添加公共数据集的表 public.table_name<br/>
3、可以为每列添加注释<br/>
</span>

##### 修改表
推荐写法：
```sql
   /*
   1、添加 city 列到表 Person
   */
   ALTER TABLE Person ADD COLUMN city varchar;
   
   /*
   2、添加多个列到表
   */
   ALTER TABLE Person ADD COLUMN (code varchar, gdp double);
   
   /*
   3、删除一列
   */
   ALTER TABLE Person DROP COLUMN city;
   
   /*
   4、删除多列
   */
   ALTER TABLE Person DROP COLUMN (code, gdp);
```
##### 删除表
```sql
DROP TABLE IF EXISTS Person;
```
##### 创建索引
```sql
-- 在 persons 表的 firstName 字段加索引，指定降序排序
CREATE INDEX name_idx ON persons (firstName DESC);

-- 添加组合索引
CREATE INDEX city_idx ON sales (country, city);

-- 添加空间索引
CREATE SPATIAL INDEX idx_person_address ON Person (address);
```
##### 删除索引
```sql
DROP INDEX idx_person_name;
```
#### 4.2、对 DML 的支持
Smart Sql 完全支持标准 SQL。同时它还支持对 函数的联级调用。
例如：
```sql
-- get_range_id 是一个函数返回一个序列，所以 id 等于这个序列的第一个
select * from person where id = get_range_id(1, 5).first();
```
### 5、对 noSql 的支持
noSql 的数据结构为 key-value 形式的 hash map。<br/>
它分为两种类型：<br/>
1、纯内存 cahe，它有缓存大小的现在，有缓存退出的策略。默认我们选择 RANDOM_2_LRU 作为退出策略。相应的参数可以在配置文件中配置<br/>

![cache_lru](/Users/chenfei/Documents/资料/SupperSql/SmartSql/smart_sql_img/cache_lru.jpg)

2、可持久化的 cache。这种 cache 一部分在内存中，全部数据在磁盘上。

**二者的区别：1、纯内存 cache 如果在数据超过最大的限度，它就直接通过过期策略直接抛弃。而可持久化就不会。2、纯内存 cache 的性能更高一些**

#### 5.1、创建 cache
```sql
-- 创建一个纯 cache
noSqlCreate({"table_name": "user_group_cache", "is_cache": true, "mode": "replicated", "maxSize": 10000});

-- 创建一个分布式缓存带持久化的
noSqlCreate({"table_name": "my_cache", "is_cache": false, "mode": "partitioned"});
```

#### 5.2、删除 cache
```sql
-- 删除 cache
noSqlDrop({"table_name": "my_cache"});
```

#### 5.3、插入数据  cache
```sql
-- 插入数据  cache
noSqlInsert({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});
```

#### 5.4、修改数据  cache
```sql
-- 修改数据  cache
noSqlUpdate({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 200}});
```

#### 5.5、删除数据  cache
```sql
-- 删除数据  cache
noSqlDelete({"table_name": "my_cache", "key": "000A"});
```

#### 5.6、插入数据的事务 cache
```sql
-- 插入数据的事务  cache
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlInsertTran({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});
```

#### 5.7、修改数据的事务 cache
```sql
-- 修改数据的事务  cache
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlUpdateTran({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 200}});
```

#### 5.8、删除数据的事务 cache
```sql
-- 删除数据的事务  cache
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlDeleteTran({"table_name": "my_cache", "key": "000A"});
```

### 6、分布式定时任务
分布式定时任务在 Smart Sql 中可以，可以利用这个功能，快速的实现一个比 Airflow 功能更强大的分布式任务调度平台。
#### 6.1、添加定时任务
定时任务使用 cron4j 的表达式
```sql
-- 添加定时任务
-- 需要输入三个参数，第一个是任务名，这个任务必须存在于 MY_META.MY_SCENES 中且有访问权限
-- 参数列表，如果没有就设置为 null 或者 []
-- cron 描述，例如：{1,3} * * * * *   每隔一分钟，重复执行三次
add_job('任务名', ['参数1', '参数2', ...], 'cron 描述');
```

```sql
-- 例如：我们要开发一个定时显示时间的任务，每隔两分钟，重复执行 30 次

-- 显示时间的任务：get_cron_time
-- 添加定时任务
function get_cron_time()
{
    my_println(concat("第一个任务：", get_now()));
}
-- get_now() 和 my_println() 来自自定义方法。(在 7 自定义方法中会有详细的介绍)

-- 添加这个定时任务
add_job('get_cron_time', [], '{2,30} * * * * *');
```

#### 6.2、删除定时任务
```sql
-- 删除定时任务
-- 输入任务名
remove_job('任务名');

-- 例如：上面的例子，删除定时任务 get_cron_time
remove_job('get_cron_time');
```

#### 6.3、查看定时任务的快照
```sql
-- 查看定时任务快照
-- 输入任务名
job_snapshot('任务名');

-- 例如：上面的例子，删除定时任务 get_cron_time
job_snapshot('get_cron_time');
```

### 7、自定义扩展的方法
在使用 Smart Sql 中，用户自己可以扩展 Smart Sql 中的方法。具体做法分两步：
1. 用户自己把程序打包成 jar 包，放到安装文件夹 lib 目录下面的 cls 文件夹下。
2. 调用 add_func 将 jar 包中的方法，注册到 Smart Sql。
完成这两步后，注册的方法就会像 Smart Sql 自己的方法一样，可以在标准 SQL 和 Smart Sql 脚本中使用。

**删除方法：remove_func(方法名称)**
**需要特别注意一点：只有 root 权限才能添加和删除方法，因为添加的方法是供整个系统使用的！**

例如：用 java 实现上面需要的两个方法：get_now();  my_println("字符串");
1. java 实现的方法
```java
package org.example.plus;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyPulsTime {

    public String getNow()
    {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return simpleDateFormat.format(new Date(timestamp.getTime()));
    }

    public void myPrintln(String line)
    {
        System.out.println(line);
    }
}
```
2. 调用 add_func 将 jar 包中的方法，注册到 Smart Sql。
```sql
-- 注册的参数是 MyUserFunc 的 json 对象
add_func({"method_name":"my_println","java_method_name":"myPrintln","cls_name":"org.example.plus.MyPulsTime","return_type":"void","descrip":"打印输入结果","lst":[{"ps_index":1,"ps_type":"String"}]});
```

**下面两个类是 MyUserFunc 和 MyFuncPs。当 root 用户执行 add_func 方法的时候，可以先用 Gson 将 MyUserFunc 序列化为 json 对象，在传递到 add_func 方法中**

```java
// 参数描述
public class MyFuncPs implements Serializable {
    private static final long serialVersionUID = -2727827280051252693L;
    // 参数的 index
    private Integer ps_index;
    // 参数的类型，名称为 String
    private String ps_type;

    public MyFuncPs(final Integer ps_index, final String ps_type)
    {
        this.ps_index = ps_index;
        this.ps_type = ps_type;
    }

    public MyFuncPs()
    {}

    public Integer getPs_index() {
        return ps_index;
    }

    public void setPs_index(Integer ps_index) {
        this.ps_index = ps_index;
    }

    public String getPs_type() {
        return ps_type;
    }

    public void setPs_type(String ps_type) {
        this.ps_type = ps_type;
    }

    @Override
    public String toString() {
        return "MyFuncPs{" +
                ", ps_index=" + ps_index +
                ", ps_type='" + ps_type + '\'' +
                '}';
    }
}


// add_func 输入参数的 java 对象
public class MyUserFunc implements Serializable {
    private static final long serialVersionUID = 8331935187125596376L;

    // smart sql 中的方法
    private String method_name;
    // Java 中的函数名
    private String java_method_name;
    // java 命名空间.类名
    private String cls_name;
    // java 返回类型
    private String return_type;
    // 方法的描述
    private String descrip;
    // 参数列表
    private List<MyFuncPs> lst;

    public MyUserFunc(final String method_name, final String java_method_name, final String cls_name, final String return_type, List<MyFuncPs> lst, final String descrip)
    {
        this.method_name = method_name;
        this.java_method_name = java_method_name;
        this.cls_name = cls_name;
        this.return_type = return_type;
        this.descrip = descrip;
        //lst = new ArrayList<>();
        if (lst != null) {
            this.lst = lst;
        }
    }

    public String getMethod_name() {
        return method_name;
    }

    public void setMethod_name(String method_name) {
        this.method_name = method_name;
    }

    public String getJava_method_name() {
        return java_method_name;
    }

    public void setJava_method_name(String java_method_name) {
        this.java_method_name = java_method_name;
    }

    public String getCls_name() {
        return cls_name;
    }

    public void setCls_name(String cls_name) {
        this.cls_name = cls_name;
    }

    public String getReturn_type() {
        return return_type;
    }

    public void setReturn_type(String return_type) {
        this.return_type = return_type;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    public List<MyFuncPs> getLst() {
        return lst;
    }

    public void setLst(List<MyFuncPs> lst) {
        this.lst = lst;
    }

    @Override
    public String toString() {
        return "MyUserFunc{" +
                "method_name='" + method_name + '\'' +
                ", java_method_name='" + java_method_name + '\'' +
                ", cls_name='" + cls_name + '\'' +
                ", return_type='" + return_type + '\'' +
                ", descrip='" + descrip + '\'' +
                ", lst=" + lst +
                '}';
    }
}
```

```sql
-- 删除方法 my_println
remove_func(my_println)；
```

**扩展方法的应用场景：
1. 整合现有系统，将现有系统的服务完美的整合进 Smart Sql。
2. 扩展 Smart Sql 的基础功能。例如：可以将机器学习的方法整合进去，让 Smart Sql 可以用简单的 Sql 来完成机器学习的模型。这个例子，我们会逐步的给出来。
**

**扩展方法的实现原理：Smart Sql 识别出 method_name 标识后，会找到对应 java method 的描述，在通过反射来调用这个 java method。（当然，不一定非的是 java，其它能编译成 class 的语言实现也是可以的，例如：scala, kotlin, clojure 等）**

### 8、高性能程序的开发
要开发稳定，高性能的应用程序需要遵循以下的规则：
1. 在读取数据的时候，尽可能的读取 key-value 形式的 cache，而不是复杂的 SQL
2. 尽量将所有业务逻辑都用 Smart Sql 来实现，外部程序可能通过 JDBC ，直接调用其方法。
3. 尽量用函数式架构思想来，设计和实现程序。

### 9、如何用 jdbc 访问 Smart Sql 数据库
Smart Sql 扩展了 JDBC 的用法。让 jdbc 可以直接调用里面的方法，除此之外，用 JDBC 访问 Smart Sql 和访问其它数据库一样。
例如：使用 root 用户的 jdbc 链接可以访问 get_user_group 内置方法，输入用户 token 查询结果
```java
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true&userToken=dafu";
        Connection conn = DriverManager.getConnection(url);
        
        PreparedStatement stmt = conn.prepareStatement("get_user_group(?)");
        stmt.setObject(1, "wudafu_token");

        ResultSet rs = stmt.executeQuery();

        String line = "";
        while (rs.next()) {
            line = rs.getString(1);
            System.out.println(line);
        }
        rs.close();
        stmt.close();
        conn.close();
```
或者
```java
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true&userToken=dafu";
        Connection conn = DriverManager.getConnection(url);
        
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("get_user_group('wudafu_token')")) {
                while (rs.next())
                    System.out.println(rs.getString(1));
            }
        }
```

































