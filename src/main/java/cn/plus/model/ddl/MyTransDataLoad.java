package cn.plus.model.ddl;

import java.io.Serializable;

public class MyTransDataLoad implements Serializable {
    private static final long serialVersionUID = 956267319728553059L;

    private String schema_name;
    private String table_name;
    private Object value;
    private Boolean is_clustering;

    public MyTransDataLoad(String schema_name, String table_name, Object value, Boolean is_clustering)
    {
        this.schema_name = schema_name;
        this.table_name = table_name;
        this.value = value;
        this.is_clustering = is_clustering;
    }

    public MyTransDataLoad()
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
        this.schema_name = schema_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyTransDataLoad{" +
                "schema_name='" + schema_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", value=" + value +
                ", is_clustering=" + is_clustering +
                '}';
    }
}












































