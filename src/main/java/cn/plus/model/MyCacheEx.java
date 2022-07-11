package cn.plus.model;

import org.apache.ignite.IgniteCache;

public class MyCacheEx extends MyCache {
    private static final long serialVersionUID = 5598794793181437905L;

    private SqlType sqlType;

    private Object data;

    public MyCacheEx(IgniteCache cache, Object key, Object value, SqlType sqlType, Object data)
    {
        this.setCache(cache);
        this.setKey(key);
        this.setValue(value);
        this.setSqlType(sqlType);
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public String toString() {
        return "MyCacheEx{" +
                "sqlType=" + sqlType +
                ", cache=" + getCache() +
                ", key=" + getKey() +
                ", value=" + getValue() +
                '}';
    }
}
