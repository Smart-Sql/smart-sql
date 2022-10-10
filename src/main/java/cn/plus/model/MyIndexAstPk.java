package cn.plus.model;

import com.google.common.base.Strings;

import java.io.Serializable;

public class MyIndexAstPk implements Serializable {
    private static final long serialVersionUID = -2904015194213831299L;

    private String indexName;

    public MyIndexAstPk(String indexName)
    {
        if (!Strings.isNullOrEmpty(indexName)) {
            this.indexName = indexName.toLowerCase();
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
}
