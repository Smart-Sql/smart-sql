package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyMlCaches implements Serializable {
    private static final long serialVersionUID = 670783655671444992L;

    private String schema_name;
    private String table_name;
    private String describe;
    private Boolean is_clustering;

    public MyMlCaches(final String schema_name, final String table_name, final String describe, final Boolean is_clustering)
    {
        this.schema_name = schema_name.toLowerCase();
        this.table_name = table_name.toLowerCase();
        this.describe = describe;
        this.is_clustering = is_clustering;
    }

    public MyMlCaches()
    {}

    public Boolean getIs_clustering() {
        return is_clustering;
    }

    public void setIs_clustering(Boolean is_clustering) {
        this.is_clustering = is_clustering;
    }

    public String getSchema_name() {
        return schema_name;
    }

    public void setSchema_name(String schema_name) {
        if (!Strings.isNullOrEmpty(schema_name))
        {
            this.schema_name = schema_name.toLowerCase();
        }
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        if (!Strings.isNullOrEmpty(table_name))
        {
            this.table_name = table_name.toLowerCase();
        }
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    @Override
    public String toString() {
        return "MyMlCaches{" +
                "schema_name='" + schema_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", describe='" + describe + '\'' +
                ", is_clustering=" + is_clustering +
                '}';
    }
}









































