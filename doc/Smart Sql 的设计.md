##  Smart Sql 的设计
SmartSql 是一个超融合的新的函数式理念，它要实现的功能相当于 分布式(mysql/pg) + 大数据体系 + 分布式缓存 + 应用程序。

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

### 6、定时任务
#### 6.1、添加定时任务
```sql
-- 添加定时任务
-- 需要输入三个参数，第一个是任务名，这个任务必须存在于 MY_META.MY_SCENES 中且有访问权限
-- 参数列表，如果没有就设置为 null 或者 []
-- cron 描述，例如：{1,3} * * * * *
add_job('任务名', ['参数1', '参数2', ...], 'cron 描述');
```
#### 6.2、删除定时任务
```sql
-- 删除定时任务
-- 输入任务名
remove_job('任务名');
```

### 7、高性能程序的开发
要开发稳定，高性能的应用程序需要遵循以下的规则：
1. 在读取数据的时候，尽可能的读取 key-value 形式的 cache，而不是复杂的 SQL
2. 尽量将所有业务逻辑都用 Smart Sql 来实现，外部程序可能通过 JDBC ，直接调用其方法。
3. 尽量用函数式架构思想来，设计和实现程序。

































