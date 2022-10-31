package cn.plus.model.ddl;

import java.io.Serializable;

public class MyDataSet implements Serializable {
    private static final long serialVersionUID = 7116154131226236070L;

    private Long id;
    private String schema_name;

    public MyDataSet()
    {}

    public MyDataSet(final Long id, final String schema_name)
    {
        this.id = id;
        this.schema_name = schema_name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSchema_name() {
        return schema_name;
    }

    public void setSchema_name(String schema_name) {
        this.schema_name = schema_name;
    }

    @Override
    public String toString() {
        return "MyDataSet{" +
                "id=" + id +
                ", schema_name='" + schema_name + '\'' +
                '}';
    }
}
