package cn.plus.model.ddl;

import java.io.Serializable;

/**
 * 对应表 my_update_views
 * */
public class MyUpdateViews implements Serializable {

    private static final long serialVersionUID = -163160139218281626L;

    private Long id;
    private String table_name;
    private Long data_set_id;
    private String code;

    public MyUpdateViews(final Long id, final String table_name, final Long data_set_id, final String code)
    {
        this.id = id;
        this.table_name = table_name;
        this.data_set_id = data_set_id;
        this.code = code;
    }

    public MyUpdateViews()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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
        return "MyUpdateViews{" +
                "id=" + id +
                ", table_name='" + table_name + '\'' +
                ", data_set_id=" + data_set_id +
                ", code='" + code + '\'' +
                '}';
    }
}

















































