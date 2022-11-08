# NoSql 的支持

noSql 的数据结构为 key-value 形式的 hash map。<br/>
它分为两种类型：<br/>
1、纯内存 cahe，它有缓存大小的现在，有缓存退出的策略。默认我们选择 RANDOM_2_LRU 作为退出策略。相应的参数可以在配置文件中配置<br/>

![cache_lru](/Users/chenfei/Documents/资料/SupperSql/SmartSql/smart_sql_img/cache_lru.jpg)

**注意：纯内存模式，默认是不开启的！开启纯内存模式之前一定要做严格的 POC 来确定项目中要开启纯内存模式所需的内存大小，避免在项目中频繁生成大量的纯内存 cache 使得划定的内存不够用，造成的错误！**

2、可持久化的 cache。这种 cache 一部分在内存中，全部数据在磁盘上。

**二者的区别：1、纯内存 cache 如果在数据超过最大的限度，它就直接通过过期策略直接抛弃。而可持久化就不会。2、纯内存 cache 的性能更高一些**

## 1、创建 cache

```sql
-- 创建一个纯 cache
noSqlCreate({"table_name": "user_group_cache", "is_cache": true, "mode": "replicated", "maxSize": 10000});

-- 创建一个分布式缓存带持久化的
noSqlCreate({"table_name": "my_cache", "is_cache": false, "mode": "partitioned"});

-- 如果是 root 用户为其它 schema 创建缓存，需要添加 schema_name
noSqlCreate({'table_name': 'myy.emp_cache',  'is_cache': true, 'mode': 'replicated', 'maxSize': 10000})
```

## 2、删除 cache

```sql
-- 删除 cache
noSqlDrop({"table_name": "my_cache"});

-- 如果是 root 用户删除其它 schema 的缓存，需要添加 schema_name
noSqlDrop({'table_name': 'my_cache', 'schema_name': 'myy'});
```

## 3、插入数据  cache

```sql
-- 插入数据  cache
noSqlInsert({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});

-- 如果是 root 用户插入数据到其它 schema 的缓存，需要添加 schema_name
noSqlInsert({"table_name": "myy.my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});
```

## 4、修改数据  cache

```sql
-- 修改数据  cache
noSqlUpdate({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 200}});

-- 如果是 root 用户要修改其它 schema 的缓存数据，需要添加 schema_name
noSqlUpdate({"table_name": "myy.my_cache", "key": "000A", "value": {"name": "吴大富", "age": 200}});
```

## 5、删除数据  cache

```sql
-- 删除数据  cache
noSqlDelete({"table_name": "my_cache", "key": "000A"});

-- 如果是 root 用户要删除其它 schema 的缓存数据，需要添加 schema_name
noSqlDelete({"table_name": "myy.my_cache", "key": "000A"});
```

## 6、插入数据的事务 cache

```sql
-- 插入数据的事务  cache
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlInsertTran({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});

-- 如果是 root 用户插入数据到其它 schema 的缓存，需要添加 schema_name
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlInsertTran({"table_name": "myy.my_cache", "key": "000A", "value": {"name": "吴大富", "age": 100}});
```

## 7、修改数据的事务 cache

```sql
-- 修改数据的事务  cache
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlUpdateTran({"table_name": "my_cache", "key": "000A", "value": {"name": "吴大富", "age": 200}});

-- 如果是 root 用户要修改其它 schema 的缓存数据，需要添加 schema_name
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlUpdateTran({"table_name": "myy.my_cache", "key": "000A", "value": {"name": "吴大富", "age": 200}});
```

## 8、删除数据的事务 cache

```sql
-- 删除数据的事务  cache
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlDeleteTran({"table_name": "my_cache", "key": "000A"});

-- 如果是 root 用户要删除其它 schema 的缓存数据，需要添加 schema_name
-- 这个方法的返回参数要输入到 trans 方法中才会执行事务
noSqlDeleteTran({"table_name": "myy.my_cache", "key": "000A"});
```
