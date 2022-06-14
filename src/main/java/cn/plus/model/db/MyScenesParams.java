package cn.plus.model.db;

import java.io.Serializable;

/**
 * 对应表 my_scenes_params
 * */
public class MyScenesParams implements Serializable {
    private static final long serialVersionUID = 3160214266305490064L;

    private String ps_name;
    private String ps_type;
    private Integer ps_index;

    public MyScenesParams (final String ps_name, final String ps_type, final Integer ps_index)
    {
        this.ps_name = ps_name;
        this.ps_type = ps_type;
        this.ps_index = ps_index;
    }

    public MyScenesParams (final String ps_type, final Integer ps_index)
    {
        this.ps_type = ps_type;
        this.ps_index = ps_index;
    }

    public MyScenesParams()
    {}

    public String getPs_name() {
        return ps_name;
    }

    public void setPs_name(String ps_name) {
        this.ps_name = ps_name;
    }

    public String getPs_type() {
        return ps_type;
    }

    public void setPs_type(String ps_type) {
        this.ps_type = ps_type;
    }

    public Integer getPs_index() {
        return ps_index;
    }

    public void setPs_index(Integer ps_index) {
        this.ps_index = ps_index;
    }

    @Override
    public String toString() {
        return "MyScenesParams{" +
                "ps_name='" + ps_name + '\'' +
                ", ps_type='" + ps_type + '\'' +
                ", ps_index=" + ps_index +
                '}';
    }
}













































