package cn.plus.model;

import java.io.Serializable;

// 记录 DLL 的 类用户传二进制 log
public class MySmartDll implements Serializable {
    private static final long serialVersionUID = 2245809474118906110L;

    private String sql;

    public MySmartDll()
    {}

    public MySmartDll(final String sql)
    {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return "MySmartDll{" +
                "sql='" + sql + '\'' +
                '}';
    }
}


























































