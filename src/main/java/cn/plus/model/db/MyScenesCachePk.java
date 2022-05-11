package cn.plus.model.db;

import java.io.Serializable;

/**
 * 用 schema 和 scenes_name 来唯一确定一个方法
 * */
public class MyScenesCachePk implements Serializable {
    private static final long serialVersionUID = -8120427570272400470L;

    //private Long group_id;
    private String schema_name;
    private String scenes_name;

    public MyScenesCachePk(final String schema_name, final String scenes_name)
    {
        this.schema_name = schema_name;
        this.scenes_name = scenes_name;
    }

    public MyScenesCachePk()
    {}

    public String getSchema_name() {
        return schema_name;
    }

    public void setSchema_name(String schema_name) {
        this.schema_name = schema_name;
    }

    public String getScenes_name() {
        return scenes_name;
    }

    public void setScenes_name(String scenes_name) {
        this.scenes_name = scenes_name;
    }

    @Override
    public String toString() {
        return "MyScenesCachePk{" +
                "schema_name='" + schema_name + '\'' +
                ", scenes_name='" + scenes_name + '\'' +
                '}';
    }
}

























































