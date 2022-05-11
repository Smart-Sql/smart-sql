package cn.plus.model.nosql;

import java.io.Serializable;

public class MyCacheValue implements Serializable {
    private static final long serialVersionUID = -2607915782013976476L;

    private String sql_line;
    private String data_regin;

    public MyCacheValue(final String sql_line, final String data_regin)
    {
        this.sql_line = sql_line;
        this.data_regin = data_regin;
    }

    public MyCacheValue()
    {}

    public String getSql_line() {
        return sql_line;
    }

    public void setSql_line(String sql_line) {
        this.sql_line = sql_line;
    }

    public String getData_regin() {
        return data_regin;
    }

    public void setData_regin(String data_regin) {
        this.data_regin = data_regin;
    }

    @Override
    public String toString() {
        return "MyCacheValue{" +
                "sql_line='" + sql_line + '\'' +
                ", data_regin='" + data_regin + '\'' +
                '}';
    }
}
