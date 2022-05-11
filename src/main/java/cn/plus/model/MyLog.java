package cn.plus.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 记录操作的表
 * */
public class MyLog implements Serializable {
    private static final long serialVersionUID = 3195276982783527627L;

    private String id;
    private String table_name;
    private byte[] mycacheex;
    private Timestamp create_date;

    public MyLog(final String id, final String table_name, final byte[] mycacheex)
    {
        this.id = id;
        this.table_name = table_name;
        this.mycacheex = mycacheex;
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public MyLog()
    {
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public byte[] getMycacheex() {
        return mycacheex;
    }

    public void setMycacheex(byte[] mycacheex) {
        this.mycacheex = mycacheex;
    }

    public Timestamp getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Timestamp create_date) {
        this.create_date = create_date;
    }
}
