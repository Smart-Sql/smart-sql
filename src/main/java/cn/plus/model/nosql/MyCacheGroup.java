package cn.plus.model.nosql;

import java.io.Serializable;

public class MyCacheGroup implements Serializable {
    private static final long serialVersionUID = -3709871329585480749L;

    private String cache_name;
    private Long group_id;

    public MyCacheGroup(final String cache_name, final Long group_id)
    {
        this.cache_name = cache_name;
        this.group_id = group_id;
    }

    public MyCacheGroup()
    {}

    public String getCache_name() {
        return cache_name;
    }

    public void setCache_name(String cache_name) {
        this.cache_name = cache_name;
    }

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }

    @Override
    public String toString() {
        return "MyCacheGroup{" +
                "cache_name='" + cache_name + '\'' +
                ", group_id=" + group_id +
                '}';
    }
}
