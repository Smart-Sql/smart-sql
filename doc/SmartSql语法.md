## Smart Sql
### Smart Sql 函数的定义：
用 function 来定义函数
```sql
function 函数名 (参数:参数类型, 参数:参数类型)
{
   -- 方法体
}
```
**注意：**
**参数中间用 , 隔开（英文字符的 , 哈）**
**参数类型的作用是，当传入的参数类型不是声明中类型的时候，将自动转化为参数类型。这个很重要，因为在 sql 语句中，有自动转化类型的功能！所以在函数定义中必须声明参数的类型！**
**Smart Sql 中的函数将方法体中最后一个执行的语言作为返回值！！！**
**Smart Sql 中每条语句以 ; 作为结束，这个是必须的**

### Smart Sql 中的数据类型

| 数据类型   | 对应 java 中的类型 |
| ------ | ----- |
| varchar | String    |
| char | String    |
| int | int    |
| double | double    |
| decimal | BigDecimal    |
| bigint | long    |
| long | long    |
| boolean | boolean    |
| timespamp | Timestamp    |
| varbinary | byte[]    |
| date | Timestamp    |
| [] | List    |
| {} | Hashtable    |

**总结：SmartSql 支持的数据类型 varchar、int、double、bigint、boolean、decimal、timespamp、varbinary、[]、{} 分别对应于 java 的：String、int、double、long、boolean、BigDecimal、Timestamp、byte[]、List、Hashtable**


### 函数体中变量的定义
```sql
-- 定义变量 c, 初始值为 20，类型 int
let c = 20;

-- 定义变量 d, 初始值为 "ok"，类型为 string
let d = "ok";

-- 定义变量 a, 初始值为 [1, 2+3*4, "yes", true] 类型的 vector
let a = [1, 2+3*4, "yes", true];

-- 定义变量 e, 初始值为 {} 类型是字典
let e = {};
```

### 函数中循环的定义
```sql
for (参数 in 序列或者Iterator)
{
   -- for 中的表达式列表
   表达式列表;
   表达式列表;
   ...
}
```
#### 只有序列和 Iterator(迭代) 的数据类型才能使用 for 循环
**特别注意：对于 C/C++， java 程序员来说，for 循环是可以有数字的，但是 smart sql 中只能是序列或者迭代**
**如果需要达到向数字一样的结果，smart sql 用 range 函数先生成这个序列，在来循环其中的每一项**
例如：
java 程序是这么做
```java
// 输入一个数字，做累加
public int add(int n)
{
    int rs = 0;
    for (int i = 0; i <= n; i++)
    {
        rs += i;
    }
    return rs;
}
```

```sql
function add(n:int)
{
    let rs = 0;
    for (i in range(n + 1))
    {
        rs = rs + i;
    }
    rs;
}
```

例子：用 Smart Sql 实现冒泡排序
```sql
-- 用这个做例子的原因是演示 range 的用法，因为这个和 Java 有区别
-- smart sql 中 for 只接受序列和迭代
function bubble_sort(lst:list)
{
    let my_lst = lst;
    let tmp = 0;
    let my_count = count(lst);
    for (i in range(my_count - 1))
    {
        for(j in range(i + 1, my_count))
        {
            match{
                my_lst.nth(i) > my_lst.nth(j):
                                           tmp = my_lst.nth(i);
                                           my_lst.set(i, my_lst.nth(j));
                                           my_lst.set(j, tmp);
            }
        }
    }
    -- 最后返回的序列
    my_lst;
}
```

#### break 语句的用法
**中断循环，使用 break; 语句**
例如：定义一个函数输入一个值，当值小于等于 5 的时候，就
```sql
function break_case(n:int)
{
    for (r in range(n))
    {
        match {
            r > 5: break;
            else println(r);
        }
    }
}
```
**break 是立刻中断循环**

### 函数中判断的定义
如果没有 else 就不写，match 中的表达式要成对。表达式: 表达式列表;
```sql
match {
   表达式: 表达式列表;
   表达式: 表达式列表;
   else 表达式列表;
}
```

### innerFunction 内部函数的定义
如果一个算法或者是函数需要调用一些它私有的函数，就可以将其定义成 innerFunction。
我们可以定义函数 getBigAndSmall 输入一个值和一个序列，返回比它小和比它大的两个集合。
例子：快速排序
```sql
function quickSort(lst:list)
{
    innerFunction {
        function getBigAndSmall(vs, seq)
        {
            let big = [];
            let small = [];
            for (m in seq)
            {
                match {
                    m >= vs: big.add(m);
                    m < vs: small.add(m);
                }
            }
            [small, big];
        }
    }
    let rs = getBigAndSmall(lst.first(), lst.rest());
    match {
        (notEmpty?(rs.nth(0)) and notEmpty?(rs.nth(1))): concat(quickSort(rs.nth(0)), [lst.first()], quickSort(rs.nth(1)));
        (notEmpty?(rs.nth(0)) and empty?(rs.nth(1))): concat(quickSort(rs.nth(0)), [lst.first()]);
        (empty?(rs.nth(0)) and notEmpty?(rs.nth(1))): concat([lst.first()], quickSort(rs.nth(1)));
        else [lst.first()];
    }
}
```

### SmartSql 对数据库的访问
#### 1、执行 Sql 的函数 query_sql 三种调用方式
##### 1.1、query_sql(sql语句)
例如：
```sql
-- query_sql(sql语句)
-- 查询 select * from public.Customers limit 0, 5
query_sql("select * from public.Customers limit 0, 5");
```

##### 1.2、query_sql(sql语句, 参数序列)
例如：
```sql
-- query_sql(sql语句, 参数序列)
-- 查询： select * from public.Customers limit 0, 5
-- 参数序列：[1, "吴大富", "吴大贵"]
query_sql("INSERT INTO public.Categories (CategoryID, CategoryName, Description, Picture) VALUES(100+?,concat(?, '是大帅哥！'),?, '')", [1, "吴大富", "吴大贵"]);
```

##### 1.3、query_sql(sql语句, 参数1, ..., 参数n)
例如：
```sql
-- query_sql(sql语句, 参数序列)
-- 查询： select * from public.Customers limit 0, 5
-- 参数序列：[1, "吴大富", "吴大贵"]
query_sql("INSERT INTO public.Categories (CategoryID, CategoryName, Description, Picture) VALUES(100+?,concat(?, '是大帅哥！'),?, '')", [1, "吴大富", "吴大贵"]);
```

#### 2、trans(Sql或者NoSql 的序列) 事务函数

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

### 实例：
任意给出一组有序序列合并成一个有序序列
```sql
function seq_sort(lst:list)
{
    innerFunction {
        function getMix(lst:list)
        {
            /**
            获取集合 lst 中第一个元素最小的值，并记录下集合的 index
            */
            let min_vs = null;
            for (index in range(count(lst)))
            {
                match {
                    nil?(min_vs): min_vs = [lst.nth(index).first(), index];
                    lst.nth(index).first() < min_vs.get(0): min_vs = [lst.nth(index).first(), index];
                }
            }

            /**
            返回最小的值和重构的集合
            */
            let index = min_vs.get(1);
            let m = lst.get(index);
            match {
                empty?(m.rest()): lst.remove(index);
                notEmpty?(m.rest()): lst.set(index, m.rest());
            }
            [min_vs.first(), lst];
        }
    }
    let min_vs = getMix(lst);
    match {
        empty?(min_vs): [];
        min_vs.count() == 2 and empty?(min_vs.last()): [min_vs.first()];
        min_vs.count() == 2 and notEmpty?(min_vs.last()): match {
            min_vs.last().count() == 1: concat([min_vs.first()], min_vs.last().first());
            else concat([min_vs.first()], seq_sort(min_vs.last()));
        }
    }
}
```



































