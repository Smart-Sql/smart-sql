package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

/**
 * 训练数据
 * */
public class MyTransData implements Serializable {
    private static final long serialVersionUID = 3269229930391174057L;

    private String dataset_name;
    private String table_name;
    private Object value;
    private Double label;
    private Boolean is_clustering;

    public MyTransData(String dataset_name, String table_name, Object value, Double label, Boolean is_clustering)
    {
        this.dataset_name = dataset_name;
        this.table_name = table_name;
        this.value = value;
        this.label = label;
        this.is_clustering = is_clustering;
    }

    public MyTransData()
    {}

    public Boolean getIs_clustering() {
        return is_clustering;
    }

    public void setIs_clustering(Boolean is_clustering) {
        this.is_clustering = is_clustering;
    }

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

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Double getLabel() {
        return label;
    }

    public void setLabel(Double label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "MyTransData{" +
                "dataset_name='" + dataset_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", value=" + value +
                ", label=" + label +
                ", is_clustering=" + is_clustering +
                '}';
    }
}










































