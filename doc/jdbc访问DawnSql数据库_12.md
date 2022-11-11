#jdbc 访问 Dawn Sql 数据库
Dawn Sql 扩展了 JDBC 的用法。让 jdbc 可以直接调用里面的方法，除此之外，用 JDBC 访问 Dawn Sql 和访问其它数据库一样。
例如：使用 root 用户的 jdbc 链接可以访问 get_user_group 内置方法，输入用户 token 查询结果

```java
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true&userToken=dafu";
        Connection conn = DriverManager.getConnection(url);
        
        PreparedStatement stmt = conn.prepareStatement("get_user_group(?)");
        stmt.setObject(1, "wudafu_token");

        ResultSet rs = stmt.executeQuery();

        String line = "";
        while (rs.next()) {
            line = rs.getString(1);
            System.out.println(line);
        }
        rs.close();
        stmt.close();
        conn.close();
```

或者

```java
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true&userToken=dafu";
        Connection conn = DriverManager.getConnection(url);
        
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("get_user_group('wudafu_token')")) {
                while (rs.next())
                    System.out.println(rs.getString(1));
            }
        }
```
