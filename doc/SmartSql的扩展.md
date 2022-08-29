### 7、自定义扩展的方法
在使用 Smart Sql 中，用户自己可以扩展 Smart Sql 中的方法。具体做法分两步：
1. 用户自己把程序打包成 jar 包，放到到安装文件夹 lib 目录下的下面的 cls 文件夹下。
2. 调用 add_func 将 jar 包中的方法，注册到 Smart Sql。
完成这两步后，注册的方法就会像 Smart Sql 自己的方法一样，可以在标准 SQL 和 Smart Sql 脚本中使用。

**删除方法：remove_func(方法名称)**
**需要特别注意一点：只有 root 权限才能添加和删除方法，因为添加的方法是供整个系统使用的！**

例如：用 java 实现上面需要的两个方法：get_now();  my_println("字符串");
1. java 实现的方法
```java
package org.example.plus;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyPulsTime {

    public String getNow()
    {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return simpleDateFormat.format(new Date(timestamp.getTime()));
    }

    public void myPrintln(String line)
    {
        System.out.println(line);
    }
}
```
2. 调用 add_func 将 jar 包中的方法，注册到 Smart Sql。
```sql
-- 注册的参数是 MyUserFunc 的 json 对象
add_func({"method_name":"my_println","java_method_name":"myPrintln","cls_name":"org.example.plus.MyPulsTime","return_type":"void","descrip":"打印输入结果","lst":[{"ps_index":1,"ps_type":"String"}]});
```

```java
// 参数描述
public class MyFuncPs implements Serializable {
    private static final long serialVersionUID = -2727827280051252693L;
    // 参数的 index
    private Integer ps_index;
    // 参数的类型，名称为 String
    private String ps_type;

    public MyFuncPs(final Integer ps_index, final String ps_type)
    {
        this.ps_index = ps_index;
        this.ps_type = ps_type;
    }

    public MyFuncPs()
    {}

    public Integer getPs_index() {
        return ps_index;
    }

    public void setPs_index(Integer ps_index) {
        this.ps_index = ps_index;
    }

    public String getPs_type() {
        return ps_type;
    }

    public void setPs_type(String ps_type) {
        this.ps_type = ps_type;
    }

    @Override
    public String toString() {
        return "MyFuncPs{" +
                ", ps_index=" + ps_index +
                ", ps_type='" + ps_type + '\'' +
                '}';
    }
}


// add_func 输入参数的 java 对象
public class MyUserFunc implements Serializable {
    private static final long serialVersionUID = 8331935187125596376L;

    // smart sql 中的方法
    private String method_name;
    // Java 中的函数名
    private String java_method_name;
    // java 命名空间.类名
    private String cls_name;
    // java 返回类型
    private String return_type;
    // 方法的描述
    private String descrip;
    // 参数列表
    private List<MyFuncPs> lst;

    public MyUserFunc(final String method_name, final String java_method_name, final String cls_name, final String return_type, List<MyFuncPs> lst, final String descrip)
    {
        this.method_name = method_name;
        this.java_method_name = java_method_name;
        this.cls_name = cls_name;
        this.return_type = return_type;
        this.descrip = descrip;
        //lst = new ArrayList<>();
        if (lst != null) {
            this.lst = lst;
        }
    }

    public String getMethod_name() {
        return method_name;
    }

    public void setMethod_name(String method_name) {
        this.method_name = method_name;
    }

    public String getJava_method_name() {
        return java_method_name;
    }

    public void setJava_method_name(String java_method_name) {
        this.java_method_name = java_method_name;
    }

    public String getCls_name() {
        return cls_name;
    }

    public void setCls_name(String cls_name) {
        this.cls_name = cls_name;
    }

    public String getReturn_type() {
        return return_type;
    }

    public void setReturn_type(String return_type) {
        this.return_type = return_type;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    public List<MyFuncPs> getLst() {
        return lst;
    }

    public void setLst(List<MyFuncPs> lst) {
        this.lst = lst;
    }

    @Override
    public String toString() {
        return "MyUserFunc{" +
                "method_name='" + method_name + '\'' +
                ", java_method_name='" + java_method_name + '\'' +
                ", cls_name='" + cls_name + '\'' +
                ", return_type='" + return_type + '\'' +
                ", descrip='" + descrip + '\'' +
                ", lst=" + lst +
                '}';
    }
}
```

```sql
-- 删除方法 my_println
remove_func(my_println)；
```

**扩展方法的应用场景：
1. 整合现有系统，将现有系统的服务完美的整合进 Smart Sql。
2. 扩展 Smart Sql 的基础功能。例如：可以将机器学习的方法整合进去，让 Smart Sql 可以用简单的 Sql 来完成机器学习的模型。这个例子，我们会逐步的给出来。
**

**扩展方法的实现原理：Smart Sql 识别出 method_name 标识后，会找到对应 java method 的描述，在通过反射来调用这个 java method。（当然，不一定非的是 java，其它能编译成 class 的语言实现也是可以的，例如：scala, kotlin, clojure 等）**

