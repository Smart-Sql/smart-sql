package cn.plus.model.ddl;

import java.io.Serializable;

public class MyViewsPk implements Serializable {
    private static final long serialVersionUID = -7849786428496484825L;
    /**
     * 限定权限的 group_id
     * */
    private Long group_id;
    private String table_name;
    private Long data_set_id;

    public MyViewsPk(final Long group_id, final String table_name, final Long data_set_id)
    {
        this.group_id = group_id;
        this.table_name = table_name;
        this.data_set_id = data_set_id;
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

    public Long getData_set_id() {
        return data_set_id;
    }

    public void setData_set_id(Long data_set_id) {
        this.data_set_id = data_set_id;
    }

    @Override
    public String toString() {
        return "MyViewsPk{" +
                "group_id=" + group_id +
                ", table_name='" + table_name + '\'' +
                ", data_set_id=" + data_set_id +
                '}';
    }
}
