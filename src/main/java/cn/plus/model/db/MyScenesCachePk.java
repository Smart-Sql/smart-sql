package cn.plus.model.db;

import java.io.Serializable;

public class MyScenesCachePk implements Serializable {
    private static final long serialVersionUID = -8120427570272400470L;

    private Long group_id;
    private String scenes_name;

    public MyScenesCachePk(final Long group_id, final String scenes_name)
    {
        this.group_id = group_id;
        this.scenes_name = scenes_name;
    }

    public MyScenesCachePk()
    {}

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
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
                "group_id=" + group_id +
                ", scenes_name='" + scenes_name + '\'' +
                '}';
    }
}

























































