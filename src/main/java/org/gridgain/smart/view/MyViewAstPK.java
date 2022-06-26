package org.gridgain.smart.view;

import java.io.Serializable;

/**
 * view 的 pk 定义
 * */
public class MyViewAstPK implements Serializable {
    private static final long serialVersionUID = -6799268990623959231L;

    private String schema_name;
    private String table_name;
    private Long my_group_id;

    public MyViewAstPK(final String schema_name, final String table_name, final Long my_group_id)
    {
        this.schema_name = schema_name;
        this.table_name = table_name;
        this.my_group_id = my_group_id;
    }

    public MyViewAstPK()
    {}

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

    public Long getMy_group_id() {
        return my_group_id;
    }

    public void setMy_group_id(Long my_group_id) {
        this.my_group_id = my_group_id;
    }
}



