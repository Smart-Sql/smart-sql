# 链接数据库
## 1、DBeaver 连接分布式数据库
创建新建连接

![dbeaver_1](https://gitee.com/wltz/smart-sql/raw/master/doc/smart_sql_img/dbeaver_1.jpg)

*SmartSql 在 Apache Ignite 上修改的，所以这里选择 Apache Ignite *

![dbeaver_2](https://gitee.com/wltz/smart-sql/raw/master/doc/smart_sql_img/dbeaver_2.jpg)

点击**编辑驱动设置**

![dbeaver_3](https://gitee.com/wltz/smart-sql/raw/master/doc/smart_sql_img/dbeaver_3.jpg)

1、url 模板必须添加 userToken 否则是不能连接数据库的。SmartSql 是通过 userToken 来获取用户或者应用程序权限的。
例子中的 userToken=dafu。 dafu 这个值是在配置文件中配置的 root 权限的 token。用户权限的 token 通过 root 用户来生成。

![root_token](https://gitee.com/wltz/smart-sql/raw/master/doc/smart_sql_img/root_token.jpg)

2、添加文件，最简单的就是把安装文件夹下面的 jar 添加进来

3、点击**找到类** 选择：org.apache.ignite.Ignite.JdbcDriver 。如图上所示

在 DBeaver 中输入如下代码：
```sql
function helloWorld(msg:String)
{
    show_msg(msg);
}
helloWorld('Welcome to Dawn Sql！');
```
运行将会得到如下结果：

![hello_world](https://gitee.com/wltz/smart-sql/raw/master/doc/smart_sql_img/hello_world.jpg)

定义一个 helloWorld 的方法，输入字符串，在打印出来。
**链接成功后，执行第一个方法需要初始化**