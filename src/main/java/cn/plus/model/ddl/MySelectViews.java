package cn.plus.model.ddl;

import java.io.Serializable;

/**
 * 对应表 my_select_views
 * */
public class MySelectViews implements Serializable {
    private static final long serialVersionUID = 6402785453662372283L;

    /**
     * 限定权限的 group_id
     * */
    private Long group_id;
    private String table_name;
    private Long data_set_id;

    private String code;

    public MySelectViews(final Long group_id, final String table_name, final Long data_set_id, final String code)
    {
        this.group_id = group_id;
        this.table_name = table_name;
        this.data_set_id = data_set_id;
        this.code = code;
    }

    public MySelectViews()
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

    public Long getData_set_id() {
        return data_set_id;
    }

    public void setData_set_id(Long data_set_id) {
        this.data_set_id = data_set_id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "MySelectViews{" +
                "group_id=" + group_id +
                ", table_name='" + table_name + '\'' +
                ", data_set_id=" + data_set_id +
                ", code='" + code + '\'' +
                '}';
    }
}

















































