package cn.plus.model.ddl;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MySchemaTable implements Serializable {
    private static final long serialVersionUID = 7417428131726308553L;

    public String schema_name;
    public String table_name;

    public MySchemaTable(final String schema_name, final String table_name)
    {
        if (!Strings.isNullOrEmpty(schema_name)) {
            this.schema_name = schema_name.toLowerCase();
        }

        if (!Strings.isNullOrEmpty(table_name)) {
            this.table_name = table_name.toLowerCase();
        }
    }

    public MySchemaTable()
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
        return "MySchemaTable{" +
                "schema_name='" + schema_name + '\'' +
                ", table_name='" + table_name + '\'' +
                '}';
    }
}
