package cn.plus.model.ddl;

import java.io.Serializable;

public class MyFuncPs implements Serializable {
    private static final long serialVersionUID = -2727827280051252693L;

    private Integer ps_index;
    private String ps_type;

    public MyFuncPs(final Integer ps_index, final String ps_type)
    {
        this.ps_index = ps_index;
        this.ps_type = ps_type;
    }

    public MyFuncPs()
    {}

    public Integer getPs_index() {
        return ps_index;
    }

    public void setPs_index(Integer ps_index) {
        this.ps_index = ps_index;
    }

    public String getPs_type() {
        return ps_type;
    }

    public void setPs_type(String ps_type) {
        this.ps_type = ps_type;
    }

    @Override
    public String toString() {
        return "MyFuncPs{" +
                ", ps_index=" + ps_index +
                ", ps_type='" + ps_type + '\'' +
                '}';
    }
}
