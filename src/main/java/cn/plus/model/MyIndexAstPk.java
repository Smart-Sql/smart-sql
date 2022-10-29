package cn.plus.model;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyIndexAstPk implements Serializable {
    private static final long serialVersionUID = -2904015194213831299L;

    private String schemaName;

    private String indexName;

    public MyIndexAstPk(String schemaName, String indexName)
    {
        if (!Strings.isNullOrEmpty(schemaName)) {
            this.schemaName = schemaName.toLowerCase();
        }

        if (!Strings.isNullOrEmpty(indexName)) {
            this.indexName = indexName.toLowerCase();
        }
    }

    public MyIndexAstPk(String indexName)
    {
        if (!Strings.isNullOrEmpty(indexName)) {
            this.indexName = indexName.toLowerCase();
        }
    }

    public MyIndexAstPk()
    {
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public String toString() {
        return "MyIndexAstPk{" +
                "schemaName='" + schemaName + '\'' +
                ", indexName='" + indexName + '\'' +
                '}';
    }
}
