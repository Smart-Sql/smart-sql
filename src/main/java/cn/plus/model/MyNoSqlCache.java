package cn.plus.model;

import java.io.Serializable;
import java.util.List;

public class MyNoSqlCache implements Serializable {
    private static final long serialVersionUID = 909743180859364754L;

    /**
     * cache name
     * */
    private String cache_name;
    private String schema_name;
    private String table_name;
    private Object key;
    private Object value;
    private SqlType sqlType;

    public MyNoSqlCache(final String cache_name, final String schema_name, final String table_name, final Object key, final Object value, final SqlType sqlType)
    {
        this.cache_name = cache_name;
        this.schema_name = schema_name;
        this.table_name = table_name;
        this.key = key;
        this.value = value;
        this.sqlType = sqlType;
    }

    public MyNoSqlCache()
    {}

    public String getCache_name() {
        return cache_name;
    }

    public void setCache_name(String cache_name) {
        this.cache_name = cache_name;
    }

    public String getSchema_name() {
        return schema_name;
    }

    public void setSchema_name(String schema_name) {
        this.schema_name = schema_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public String toString() {
        return "MyNoSqlCache{" +
                "cache_name='" + cache_name + '\'' +
                ", schema_name='" + schema_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", key=" + key +
                ", value=" + value +
                ", sqlType=" + sqlType +
                '}';
    }
}
