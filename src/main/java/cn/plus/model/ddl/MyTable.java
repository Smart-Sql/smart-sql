package cn.plus.model.ddl;

import java.io.Serializable;

public class MyTable implements Serializable {
    private static final long serialVersionUID = -2362973613235533917L;

    private Long id;
    private String table_name;
    private String descrip;
    private String code;
    private Long data_set_id;

    public MyTable(final Long id, final String table_name, final String descrip, final String code, final Long data_set_id)
    {
        this.id = id;
        this.table_name = table_name;
        this.descrip = descrip;
        this.code = code;
        this.data_set_id = data_set_id;
    }

    public MyTable(final String table_name, final String descrip, final String code, final Long data_set_id)
    {
        this.table_name = table_name;
        this.descrip = descrip;
        this.code = code;
        this.data_set_id = data_set_id;
    }

    public MyTable()
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

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Long getData_set_id() {
        return data_set_id;
    }

    public void setData_set_id(Long data_set_id) {
        this.data_set_id = data_set_id;
    }

    @Override
    public String toString() {
        return "MyTable{" +
                "id=" + id +
                ", table_name='" + table_name + '\'' +
                ", descrip='" + descrip + '\'' +
                ", code='" + code + '\'' +
                ", data_set_id=" + data_set_id +
                '}';
    }
}
