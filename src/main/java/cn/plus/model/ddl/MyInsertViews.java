package cn.plus.model.ddl;

import java.io.Serializable;

/**
 * 对应表 my_insert_views
 * */
public class MyInsertViews implements Serializable {

    private static final long serialVersionUID = 2876406698594322699L;

    /**
     * 限定权限的 group_id
     * */
    private Long group_id;
    private String table_name;
    private String schema_name;
    /**
     * 限定代码
     * */
    private String code;

    public MyInsertViews(final Long group_id, final String table_name, final String schema_name, final String code)
    {
        this.group_id = group_id;
        this.table_name = table_name;
        this.schema_name = schema_name;
        this.code = code;
    }

    public MyInsertViews()
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

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "MyInsertViews{" +
                "group_id=" + group_id +
                ", table_name='" + table_name + '\'' +
                ", schema_name=" + schema_name +
                ", code='" + code + '\'' +
                '}';
    }
}

















































