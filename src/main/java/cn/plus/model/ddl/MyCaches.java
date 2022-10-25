package cn.plus.model.ddl;

import java.io.Serializable;

public class MyCaches implements Serializable {

    private static final long serialVersionUID = -3653981400905906802L;
    private String dataset_name;
    private String table_name;
    private Boolean is_cache;
    private String mode;
    private Integer maxSize;

    public MyCaches(final String dataset_name, final String table_name, final Boolean is_cache, final String mode, final Integer maxSize)
    {
        this.dataset_name = dataset_name != null? dataset_name.toLowerCase(): dataset_name;
        this.table_name = table_name != null? table_name.toLowerCase(): table_name;
        this.is_cache = is_cache;
        this.mode = mode != null? mode.toLowerCase(): mode;
        this.maxSize = maxSize;
    }

    public MyCaches()
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

    public Boolean getIs_cache() {
        return is_cache;
    }

    public void setIs_cache(Boolean is_cache) {
        this.is_cache = is_cache;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Integer getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(Integer maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public String toString() {
        return "MyCaches{" +
                "dataset_name='" + dataset_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", is_cache=" + is_cache +
                ", mode='" + mode + '\'' +
                ", maxSize=" + maxSize +
                '}';
    }
}
