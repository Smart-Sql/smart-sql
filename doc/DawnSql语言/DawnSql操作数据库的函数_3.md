#DawnSql操作数据库的函数
## 1、执行 Sql 的函数 query_sql 三种调用方式
### 1.1、query_sql(sql语句)
例如：
```sql
-- query_sql(sql语句)
-- 查询 select * from public.Customers limit 0, 5
query_sql("select * from public.Customers limit 0, 5");
```

### 1.2、query_sql(sql语句, 参数序列)
例如：
```sql
-- query_sql(sql语句, 参数序列)
-- 查询： select * from public.Customers limit 0, 5
-- 参数序列：[1, "吴大富", "吴大贵"]
query_sql("INSERT INTO public.Categories (CategoryID, CategoryName, Description, Picture) VALUES(100+?,concat(?, '是大帅哥！'),?, '')", [1, "吴大富", "吴大贵"]);
```

### 1.3、query_sql(sql语句, 参数1, ..., 参数n)
例如：
```sql
-- query_sql(sql语句, 参数序列)
-- 查询： select * from public.Customers limit 0, 5
-- 参数序列：[1, "吴大富", "吴大贵"]
query_sql("INSERT INTO public.Categories (CategoryID, CategoryName, Description, Picture) VALUES(100+?,concat(?, '是大帅哥！'),?, '')", [1, "吴大富", "吴大贵"]);
```

## 2、trans(Sql或者NoSql 的序列) 事务函数
```sql
-- trans 接收一个序列，使得执行序列中的语句拥有事务性
-- 要么全部执行正确，要么不执行
let lst = [];
lst.add([sql语句]);
lst.add([sql语句, [参数]]);
lst.add(noSqlUpdateTran({}));
trans(lst);
```
例如：
```sql
let lst = [["INSERT INTO public.Categories (CategoryID, CategoryName, Description, Picture) VALUES(? + 1+ ?,?,'Seaweed and fish', '')", [a, b, "吴大富"]]];
   lst.add(noSqlInsertTran({"table_name": "wudafu", "key": "吴大富", "value": "大帅哥"}));
   trans(lst);
```

**注意：如果是 sql 语句事务函数接受的是 [sql] 或者是 [sql, [参数列表]] 的序列。如果是 NoSql 一定用到函数 noSqlInsertTran， noSqlUpdateTran， noSqlDeleteTran**