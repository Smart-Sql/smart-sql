package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyMlShowData implements Serializable {
    private static final long serialVersionUID = 8327167221293861418L;

    private String dataset_name;
    private String table_name;
    private Integer item_size;

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        if (!Strings.isNullOrEmpty(dataset_name)) {
            this.dataset_name = dataset_name.toLowerCase();
        }
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        if (!Strings.isNullOrEmpty(table_name)) {
            this.table_name = table_name.toLowerCase();
        }
    }

    public Integer getItem_size() {
        return item_size;
    }

    public void setItem_size(Integer item_size) {
        this.item_size = item_size;
    }

    @Override
    public String toString() {
        return "MyMlShowData{" +
                "dataset_name='" + dataset_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", item_size=" + item_size +
                '}';
    }
}
