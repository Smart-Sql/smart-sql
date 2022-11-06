package cn.plus.model;

import java.io.Serializable;
import java.util.List;

public class MyLogCache implements Serializable {
    private static final long serialVersionUID = -5518188600794814902L;

    /**
     * cache name
     * */
    private String cache_name;
    private String schema_name;
    private String table_name;
    /**
     * key 如果是单独的主键，则是基础类型，如果是联合主键，则是 List<MyKeyValue> 类型
     * */
    private Object key;
    //private List<MyKeyValue> value;
    private Object value;
    private SqlType sqlType;

    public MyLogCache(final String cache_name, final String schema_name, final String table_name, final Object key, final Object value, final SqlType sqlType)
    {
        this.cache_name = cache_name;
        this.schema_name = schema_name;
        this.table_name = table_name;
        this.key = key;
        this.value = value;
        this.sqlType = sqlType;
    }

    public MyLogCache(final String cache_name, final Object key, final Object value, final SqlType sqlType)
    {
        this.cache_name = cache_name;
        this.key = key;
        this.value = value;
        this.sqlType = sqlType;
    }

    public MyLogCache()
    {}

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

    public String getCache_name() {
        return cache_name;
    }

    public void setCache_name(String cache_name) {
        this.cache_name = cache_name;
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

    public void setValue(List<MyKeyValue> value) {
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
        return "MyLogCache{" +
                "cache_name='" + cache_name + '\'' +
                ", schema_name='" + schema_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", key=" + key +
                ", value=" + value +
                ", sqlType=" + sqlType +
                '}';
    }
}
