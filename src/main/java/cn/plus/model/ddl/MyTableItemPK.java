package cn.plus.model.ddl;

import java.io.Serializable;

public class MyTableItemPK implements Serializable {
    private static final long serialVersionUID = 5877345189829065525L;

    private Long id;
    private Long table_id;

    public MyTableItemPK(final Long id, final Long table_id)
    {
        this.id = id;
        this.table_id = table_id;
    }

    public MyTableItemPK()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTable_id() {
        return table_id;
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    @Override
    public String toString() {
        return "MyTableItemPK{" +
                "id=" + id +
                ", table_id=" + table_id +
                '}';
    }
}
