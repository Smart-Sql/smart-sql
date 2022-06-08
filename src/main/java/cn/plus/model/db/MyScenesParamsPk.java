package cn.plus.model.db;

import java.io.Serializable;

public class MyScenesParamsPk implements Serializable {
    private static final long serialVersionUID = -4525949020790586010L;

    private String scenes_name;
    private Integer ps_index;

    public MyScenesParamsPk(final String scenes_name, final Integer ps_index)
    {
        this.scenes_name = scenes_name;
        this.ps_index = ps_index;
    }

    public MyScenesParamsPk()
    {}

    public String getScenes_name() {
        return scenes_name;
    }

    public void setScenes_name(String scenes_name) {
        this.scenes_name = scenes_name;
    }

    public Integer getPs_index() {
        return ps_index;
    }

    public void setPs_index(Integer ps_index) {
        this.ps_index = ps_index;
    }

    @Override
    public String toString() {
        return "MyScenesParamsPk{" +
                "scenes_name='" + scenes_name + '\'' +
                ", ps_index=" + ps_index +
                '}';
    }
}
