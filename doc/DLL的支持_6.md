#DLL 的支持

## 创建表

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
   

![create_table_template](https://gitee.com/wltz/Dawn-sql/raw/master/doc/Dawn_sql_img/create_table_template.jpg)


| 值   | 说明                                         |
| ---- | -------------------------------------------- |
| base | 表示表为复制模式，既每个节点都有一份完整的表 |
| manage | 表示表为分区模式，既表中的数据平均到集群中的每个节点，<br/>为了保证数据的完整性和集群的可用性，这种模式会备份三份 |

**推荐例子**
<span style="color: red; font-weight: bolder;">强烈建议在开发中，直接复制例子中的代码，在此基础上修改。<br/>因为 DawnSql 的一些细微的写法和标准 sql 有少许的区别！</span>
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
在创建表时，标准 SQL 和 Dawn Sql 的区别有三点：
<br/>
1、必须添加 with，因为 Dawn Sql 是分布式数据库，所以必须要指明它在集群中的样子。<br/>
2、如果是在本数据集，创建表可以不加数据集的名字，如果不是就要添加上，例如：添加公共数据集的表 public.table_name<br/>
3、可以为每列添加注释<br/>
</span>

## 修改表
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
## 删除表
```sql
DROP TABLE IF EXISTS Person;
```
## 创建索引

```sql
-- 在 persons 表的 firstName 字段加索引，指定降序排序
CREATE INDEX name_idx ON myy.persons (firstName DESC);

-- 添加组合索引
CREATE INDEX city_idx ON myy.sales (country, city);

-- 添加空间索引
CREATE SPATIAL INDEX idx_person_address ON myy.Person (address);
```
## 删除索引
```sql
DROP INDEX myy.idx_person_name;
```