## Smart 中的函数
### 1、访问数据库

| 函数名    | 说明                                               |
| --------- | -------------------------------------------------- |
| query_sql | 在 Smart Sql 访问数据库的方法，只能执行 DML 语句。 |

**用法有三种：:**
```sql
-- 1、输入 sql 和参数
query_sql("select m.id from my_dataset m where m.dataset_name = ?", name);

-- 2、输入 sql 和 参数列表
query_sql("select m.id from my_dataset m where m.dataset_name = ?", [name]);

-- 3、输入 sql 没有参数
query_sql("select m.id from my_dataset m where m.dataset_name = '吴大富'");
```

| 函数名    | 说明                                               |
| --------- | -------------------------------------------------- |
| trans | 在 Smart Sql 执行事务的语句，支持 sql 和 noSql。 |