#对 DML 的支持
## 1、select 语句
从一个或多个表中检索数据
```sql
SELECT
    [DISTINCT | ALL] selectExpression [,...]
    FROM tableExpression [,...] [WHERE expression]
    [GROUP BY expression [,...]] [HAVING expression]
    [{UNION [ALL] | MINUS | EXCEPT | INTERSECT} select]
    [ORDER BY order [,...]]
    [{ LIMIT expression, expression}]
```
参数

1. DISTINCT：配置从结果集中删除重复数据；
2. GROUP BY：通过给定的表达式对结果集进行分组；
3. HAVING：分组后过滤；
4. ORDER BY：使用给定的列或者表达式对结果集进行排序；
5. LIMIT ：对查询返回的结果集的数量进行限制（如果为null或者小于0则无限制）
6. UNION、INTERSECT、MINUS、EXPECT：将多个查询的结果集进行组合；
7. tableExpression：表联接。联接表达式目前不支持交叉联接和自然联接，自然联接是一个内联接,其条件会自动加在同名的列上；

```sql
tableExpression = [[LEFT | RIGHT]{OUTER}] | INNER]
JOIN tableExpression
[ON expression]
```
1. `LEFT`：左联接执行从第一个（最左边）表开始的联接，然后匹配任何第二个（最右边）表的记录；
2. `RIGHT`：右联接执行从第二个（最右边）表开始的联接，然后匹配任何第一个（最左边）表的记录；
3. `OUTER`：外联接进一步细分为左外联接、右外联接和完全外联接，这取决于哪些表的行被保留（左、右或两者）；
4. `INNER`：内联接要求两个合并表中的每一行具有匹配的列值；
5. `CROSS`：交叉连接返回相关表数据的笛卡尔积；
6. `NATURAL`：自然联接是等值联接的一个特殊情况；
7. `ON`：要联接的条件或者值。

<span style="font-size: larger;font-weight: bolder;">描述</span>

`SELECT`查询可以在`分区`和`复制`模式的数据上执行。当在全复制的数据上执行的时候，会将查询发送到某一个节点上，然后在其本地数据上执行。
如果查询在分区数据上执行，那么执行流程如下：
1. 查询会被解析，然后拆分为多个映射查询和一个汇总查询；
2. 所有的映射查询会在请求数据所在的所有节点上执行；
3. 所有节点将本地执行的结果集返回给查询发起方（汇总），然后在将数据正确地合并之后完成汇总阶段。

<span style="font-size: larger;font-weight: bolder;">分组和排序优化</span>
带有ORDER BY子句的SQL查询不需要将整个结果集加载到查询发起节点来进行完整的排序，而是查询映射的每个节点先对各自的结果集进行排序，然后汇总过程会以流式进行合并处理。

<span style="font-size: larger;font-weight: bolder;">查询 or 的优化</span>
在 where 关键词后，如果用到多个 or，应该换成一个 in ()

<span style="font-size: larger;font-weight: bolder;">示例</span>
检索 public 中 Categories 表的数据
```sql
select * from public.Categories
```

```sql
-- 1、求每位员工的每个月份的业绩
-- 按照员工分组是第一个条件，其次是按照月份来分组，这时我们就要考虑到，如果只按月分组就会出现不同年份的某个月份被分到了一个组里，
-- 这种情况并不是我们想看到的，所以我们应该按照年月来分组 （划重点：按照年月来分组，不要只按照日期分组）

-- 选择我们想要输出的姓名、年月、以及业绩
SELECT concat(e.FirstName, ' ', e.LastName) name, YEAR(o.OrderDate) my_year,
MONTH(o.OrderDate),SUM(od.UnitPrice*od.Quantity*(1-od.Discount))
-- 连接需要的表格
FROM wudafu.Employees e
JOIN wudagui.Orders o ON e.EmployeeID=o.EmployeeID
JOIN wudagui.ORDERDETAILS od ON o.OrderID = od.OrderID 
-- 按照姓名和年月进行分组
GROUP BY e.LastName,e.FirstName,YEAR(o.OrderDate),MONTH(o.OrderDate);

-- 2、求最近一个月销量排行前十的产品
-- 选择我们想要输出的产品名和销量
SELECT p.ProductName, sum(od.Quantity) 
--连接需要的表
FROM public.Products p,
JOIN wudagui.OrderDetails od ON p.ProductID = od.ProductID
JOIN wudagui.Orders o ON od.OrderID = o.OrderID
-- 筛选条件（年份=最近有订单的年份，月份=最近有订单的月份）
-- 此处用到了嵌套查询
WHERE MONTH(o.OrderDate) = (SELECT MONTH(OrderDate) FROM Orders ORDER BY OrderDate DESC LIMIT 0, 1)
and YEAR(o.OrderDate) = (SELECT YEAR(OrderDate) FROM Orders ORDER BY OrderDate DESC LIMIT 0, 1)
--按照产品名称进行分组
GROUP BY p.ProductName
--按照总销量进行排序
--ORDER BY 为升序
--ORDER BY DESC为降序
ORDER BY SUM(od.Quantity) DESC;
```

## 2、INSERT
往表里插入数据
```sql
INSERT INTO tableName
  {[( columnName [,...])]
  {VALUES {({DEFAULT | expression} [,...])} [,...] | [DIRECT] [SORTED] select}}
  | {SET {columnName = {DEFAULT | expression}} [,...]}
```
<span style="font-size: larger;font-weight: bolder;">参数</span>
1. tableName：要更新的表名
2. columnName：VALUES子句中的值对应的列名

<span style="font-size: larger;font-weight: bolder;">描述</span>
在 Dawn Sql 中，会将 insert 语句转换成 key-value 的形式来保存。

<span style="font-size: larger;font-weight: bolder;">示例</span>
```sql
INSERT INTO public.Categories (CategoryName, Description, Picture) VALUES('Seafood','Seaweed and fish', '');
```

## 3、UPDATE
修改表里的数据
```sql
UPDATE tableName [[AS] newTableAlias]
  SET {{columnName = {DEFAULT | expression}} [,...]} 
  [WHERE expression][LIMIT expression]
```
<span style="font-size: larger;font-weight: bolder;">参数</span>
1. tableName：要修改的表名
2. columnName：要修改的列名

<span style="font-size: larger;font-weight: bolder;">描述</span>
在 Dawn Sql 中，会将 update 语句转换成 key-value 的形式来保存。

<span style="font-size: larger;font-weight: bolder;">示例</span>
```sql
UPDATE public.Categories SET CategoryName = '鱼' WHERE CategoryID = 2;
```
**注意：主键是不能更新的！**

## 4、DELETE
从表里删除数据
```sql
DELETE
  FROM tableName
  [WHERE expression]
  [LIMIT term]
```
<span style="font-size: larger;font-weight: bolder;">参数</span>
1. tableName：要删除的表名
2. LIMIT：指定要删除的数据的数量（如果为null或者小于0则无限制）

<span style="font-size: larger;font-weight: bolder;">描述</span>
在 Dawn Sql 中，会将 delete 语句转换成 key-value 的形式来保存。

<span style="font-size: larger;font-weight: bolder;">示例</span>
```sql
DELETE FROM public.Categories WHERE CategoryID = 2;
```

**注意：Dawn Sql 的使用原则是：尽量用简单的 SQL 的组合来描述业务。这样的好处：学习成本低，维护成本低**


































