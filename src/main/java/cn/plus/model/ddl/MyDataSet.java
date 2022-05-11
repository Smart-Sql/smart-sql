package cn.plus.model.ddl;

import java.io.Serializable;

public class MyDataSet implements Serializable {
    private static final long serialVersionUID = 7116154131226236070L;

    private Long id;
    private String dataset_name;

    public MyDataSet()
    {}

    public MyDataSet(final Long id, final String dataset_name)
    {
        this.id = id;
        this.dataset_name = dataset_name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        this.dataset_name = dataset_name;
    }

    @Override
    public String toString() {
        return "MyDataSet{" +
                "id=" + id +
                ", dataset_name='" + dataset_name + '\'' +
                '}';
    }
}
