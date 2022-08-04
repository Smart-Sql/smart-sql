package cn.plus.model.db;

import java.io.Serializable;

public class MyCallScenesPk implements Serializable {
    private static final long serialVersionUID = 5003428301871545767L;

    private Long to_group_id;
    private String scenes_name;

    public MyCallScenesPk(final Long to_group_id, final String scenes_name)
    {
        this.to_group_id = to_group_id;
        this.scenes_name = scenes_name.toLowerCase();
    }

    public MyCallScenesPk()
    {}

    public Long getTo_group_id() {
        return to_group_id;
    }

    public void setTo_group_id(Long to_group_id) {
        this.to_group_id = to_group_id;
    }

    public String getScenes_name() {
        return scenes_name;
    }

    public void setScenes_name(String scenes_name) {
        this.scenes_name = scenes_name;
    }

    @Override
    public String toString() {
        return "MyCallScenesPk{" +
                "to_group_id=" + to_group_id +
                ", scenes_name='" + scenes_name + '\'' +
                '}';
    }
}
