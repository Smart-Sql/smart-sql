package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyCachePK implements Serializable {
    private static final long serialVersionUID = -1677705010422916099L;

    private String schema_name;
    private String table_name;

    public MyCachePK(final String schema_name, final String table_name) {
        if (!Strings.isNullOrEmpty(schema_name)) {
            this.schema_name = schema_name.toLowerCase();
        }

        this.table_name = table_name != null? table_name.toLowerCase(): table_name;
    }

    public MyCachePK()
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

    @Override
    public String toString() {
        return "MyCachePK{" +
                "schema_name='" + schema_name + '\'' +
                ", table_name='" + table_name + '\'' +
                '}';
    }
}
