package cn.plus.model;

import java.io.Serializable;

/**
 * 创建或者删除 cache
 * */
public class MySmartCache implements Serializable {
    private static final long serialVersionUID = -616250986367186899L;

    private String table_name;
    private Boolean is_cache;
    private String mode;
    private Integer maxSize;
    private CacheDllType cacheDllType;

    public MySmartCache(final String table_name, final Boolean is_cache, final String mode, final Integer maxSize, final CacheDllType cacheDllType)
    {
        this.table_name = table_name;
        this.is_cache = is_cache;
        this.mode = mode;
        this.maxSize = maxSize;
        this.cacheDllType = cacheDllType;
    }

    public MySmartCache(final String table_name, final Boolean is_cache, final String mode, final Integer maxSize)
    {
        this.table_name = table_name;
        this.is_cache = is_cache;
        this.mode = mode;
        this.maxSize = maxSize;
    }

    public MySmartCache()
    {}

    public CacheDllType getCacheDllType() {
        return cacheDllType;
    }

    public void setCacheDllType(CacheDllType cacheDllType) {
        this.cacheDllType = cacheDllType;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Boolean getIs_cache() {
        return is_cache;
    }

    public void setIs_cache(Boolean is_cache) {
        this.is_cache = is_cache;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Integer getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(Integer maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public String toString() {
        return "MySmartCache{" +
                "table_name='" + table_name + '\'' +
                ", is_cache=" + is_cache +
                ", mode='" + mode + '\'' +
                ", maxSize=" + maxSize +
                ", cacheDllType=" + cacheDllType +
                '}';
    }
}








































