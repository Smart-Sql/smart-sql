package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyCachePK implements Serializable {
    private static final long serialVersionUID = -1677705010422916099L;

    private String dataset_name;
    private String table_name;

    public MyCachePK(final String dataset_name, final String table_name) {
        if (!Strings.isNullOrEmpty(dataset_name)) {
            this.dataset_name = dataset_name.toLowerCase();
        }

        this.table_name = table_name != null? table_name.toLowerCase(): table_name;
    }

    public MyCachePK()
    {}

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        this.dataset_name = dataset_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    @Override
    public String toString() {
        return "MyCachePK{" +
                "dataset_name='" + dataset_name + '\'' +
                ", table_name='" + table_name + '\'' +
                '}';
    }
}
