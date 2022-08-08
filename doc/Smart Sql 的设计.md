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
































