package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyMlCaches implements Serializable {
    private static final long serialVersionUID = 670783655671444992L;

    private String dataset_name;
    private String table_name;
    private String describe;

    public MyMlCaches(final String dataset_name, final String table_name, final String describe)
    {
        this.dataset_name = dataset_name.toLowerCase();
        this.table_name = table_name.toLowerCase();
        this.describe = describe;
    }

    public MyMlCaches()
    {}

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        if (!Strings.isNullOrEmpty(dataset_name))
        {
            this.dataset_name = dataset_name.toLowerCase();
        }
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        if (!Strings.isNullOrEmpty(table_name))
        {
            this.table_name = table_name.toLowerCase();
        }
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    @Override
    public String toString() {
        return "MyMlCaches{" +
                "dataset_name='" + dataset_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", describe='" + describe + '\'' +
                '}';
    }
}









































