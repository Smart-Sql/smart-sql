package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyViewsPk implements Serializable {
    private static final long serialVersionUID = -7849786428496484825L;
    /**
     * 限定权限的 group_id
     * */
    private Long group_id;
    private String table_name;
    private String schema_name;

    public MyViewsPk(final Long group_id, final String table_name, final String schema_name)
    {
        this.group_id = group_id;
        if (!Strings.isNullOrEmpty(table_name)) {
            this.table_name = table_name.toLowerCase();
        }

        if (!Strings.isNullOrEmpty(schema_name)) {
            this.schema_name = schema_name.toLowerCase();
        }
    }

    public MyViewsPk()
    {}

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getSchema_name() {
        return schema_name;
    }

    public void setSchema_name(String schema_name) {
        this.schema_name = schema_name;
    }

    @Override
    public String toString() {
        return "MyViewsPk{" +
                "group_id=" + group_id +
                ", table_name='" + table_name + '\'' +
                ", schema_name=" + schema_name +
                '}';
    }
}
