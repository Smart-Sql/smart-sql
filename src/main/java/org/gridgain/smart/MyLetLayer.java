package org.gridgain.smart;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 记录 let 的层次
 * */
public class MyLetLayer implements Serializable {
    private static final long serialVersionUID = 9205242288796004155L;

    private ArrayList<String> lst;
    private MyLetLayer upLayer;

    public MyLetLayer(final ArrayList<String> lst, final MyLetLayer upLayer)
    {
        this.lst = lst;
        this.upLayer = upLayer;
    }

    public MyLetLayer()
    {
        this.lst = new ArrayList<String>();
    }
    
    public MyLetLayer addLet(final String let)
    {
        this.getLst().add(let);
        return this;
    }

    public ArrayList<String> getLst() {
        return lst;
    }

    public void setLst(ArrayList<String> lst) {
        this.lst = lst;
    }

    public MyLetLayer getUpLayer() {
        return upLayer;
    }

    public void setUpLayer(MyLetLayer upLayer) {
        this.upLayer = upLayer;
    }

    @Override
    public String toString() {
        return "MyLetLayer{" +
                "lst=" + lst +
                ", upLayer=" + upLayer +
                '}';
    }
}

















































