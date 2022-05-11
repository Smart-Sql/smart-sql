package cn.plus.model;

import java.io.Serializable;

/**
 * key - value 键值对
 * */
public class MyKeyValue implements Serializable {
    private static final long serialVersionUID = 7137138551268922931L;

    private String name;
    private Object value;

    public MyKeyValue(final String name, final Object value)
    {
        this.name = name;
        this.value = value;
    }

    public MyKeyValue()
    {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyKeyValue{" +
                "name='" + name + '\'' +
                ", value=" + value +
                '}';
    }
}
