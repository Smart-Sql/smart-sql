package cn.plus.model.ddl;

import java.io.Serializable;

/**
 * 对应表 table_index
 * */
public class MyTableIndex implements Serializable {
    private static final long serialVersionUID = -5049938522206922618L;

    private Long id;
    private String index_name;
    private Boolean spatial = false;
    private Long table_id;

    public MyTableIndex(final Long id, final String index_name, final Boolean spatial, final Long table_id)
    {
        this.id = id;
        this.index_name = index_name;
        this.table_id = table_id;
        this.spatial = spatial;
    }

    public MyTableIndex()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIndex_name() {
        return index_name;
    }

    public void setIndex_name(String index_name) {
        this.index_name = index_name;
    }

    public Boolean getSpatial() {
        return spatial;
    }

    public void setSpatial(Boolean spatial) {
        this.spatial = spatial;
    }

    public Long getTable_id() {
        return table_id;
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    @Override
    public String toString() {
        return "MyTableIndex{" +
                "id=" + id +
                ", index_name='" + index_name + '\'' +
                ", spatial=" + spatial +
                ", table_id=" + table_id +
                '}';
    }
}















































