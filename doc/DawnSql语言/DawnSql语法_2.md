#DawnSql语法
DawnSql 只有函数的概念！函数就是 DawnSql 的核心。
**函数是最接近人类思维的表达方式。既解决一个复杂的业务问题，我们可以把它分拆成几个子问题，在通过组合这几个子问题来解决。所以整个过程中，是不需要软件架构和设计能力的。这样就进一步降低了使用者的能力要求，提升了工作产出。**

## DawnSql 函数的定义
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
**Dawn Sql 中的函数将方法体中最后一个执行的语言作为返回值！！！**
**Dawn Sql 中每条语句以 ; 作为结束，这个是必须的**

<span style="font-size: larger;font-weight: bolder;">例如：</span>
定义函数 hello_world 输入字符串
![dawn_sql_1](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/dawn_sql_1.jpg)
<br/>
定义函数 hello_world 输入整数类型
![dawn_sql_2](/Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/smart_sql_img/dawn_sql_2.jpg)

## Dawn Sql 中的数据类型

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

**总结：Dawn Sql 支持的数据类型 varchar、int、double、bigint、boolean、decimal、timespamp、varbinary、[]、{} 分别对应于 java 的：String、int、double、long、boolean、BigDecimal、Timestamp、byte[]、List、Hashtable**

## 函数体中变量的定义
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

## 函数中循环的定义
```sql
for (参数 in 序列或者Iterator)
{
   -- for 中的表达式列表
   表达式列表;
   表达式列表;
   ...
}
```
### 只有序列和 Iterator(迭代) 的数据类型才能使用 for 循环

**特别注意：对于 C/C++， java 程序员来说，for 循环是可以有数字的，但是 smart sql 中只能是序列或者迭代**
**如果需要达到向数字一样的结果，dawn sql 用 range 函数先生成这个序列，在来循环其中的每一项。**
*range(end) 生成一个从 0 到这个end的序列*
*range(start, end) 生成一个从 start 到这个end的序列*
*range(start, end, step) 生成一个从 start 到这个end，步长为step的序列*
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
### break 语句的用法
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

## 函数中模式匹配的定义
如果没有 else 就不写，match 中的表达式要成对。表达式: 表达式列表;
```sql
match {
   表达式: 表达式列表;
   表达式: 表达式列表;
   else 表达式列表;
}
```

## innerFunction 内部函数的定义
如果一个算法或者是函数需要调用一些它私有的函数，就可以将其定义成 innerFunction
例如：
```sql
function my_func(n:int)
{
    innerFunction {
       function my_func_1()
       {}
       ...
    }
}
```
my_func_1 只在 my_func 中起作用。

## 总结：
1. 输入参数必须要声明数据类型
2. 函数体最后一条语句为函数的返回值。
3. let 用来定义变量不用声明数据类型。
4. for 用来迭代序列
5. match 用来做模式匹配
6. innerFunction 用来定义内部函数