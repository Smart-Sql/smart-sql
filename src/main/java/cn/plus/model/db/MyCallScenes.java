package cn.plus.model.db;

import java.io.Serializable;

/**
 * 对应 call_scenes 表，
 * 记录一个 group_id 可以调用那些场景函数
 * */
public class MyCallScenes implements Serializable {
    private static final long serialVersionUID = 7695989946243125451L;

    private Long group_id;
    private Long to_group_id;
    private String scenes_name;

    private Object group_id_obj;

    public MyCallScenes(final Long group_id, final Long to_group_id, final String scenes_name, final Object group_id_obj)
    {
        this.group_id = group_id;
        this.to_group_id = to_group_id;
        this.scenes_name = scenes_name;
        this.group_id_obj = group_id_obj;
    }

    public MyCallScenes(final Long group_id, final Long to_group_id, final String scenes_name)
    {
        this.group_id = group_id;
        this.to_group_id = to_group_id;
        this.scenes_name = scenes_name;
    }

    public MyCallScenes()
    {}

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }

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

    public Object getGroup_id_obj() {
        return group_id_obj;
    }

    public void setGroup_id_obj(Object group_id_obj) {
        this.group_id_obj = group_id_obj;
    }

    @Override
    public String toString() {
        return "MyCallScenes{" +
                "group_id=" + group_id +
                ", to_group_id=" + to_group_id +
                ", scenes_name='" + scenes_name + '\'' +
                '}';
    }
}











































