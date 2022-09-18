package org.gridgain.smart.ml.model;

import java.io.Serializable;
import java.util.List;

public class MyMModelKey implements Serializable {

    private static final long serialVersionUID = 8126259412240588312L;
    private String cacheName;
    private MyMLMethodName methodName;
    private List<MyMPs> lstPs;

    public MyMModelKey()
    {}

    public MyMModelKey(final String cacheName, final MyMLMethodName methodName, final List<MyMPs> lstPs)
    {
        this.cacheName = cacheName;
        this.methodName = methodName;
        this.lstPs = lstPs;
    }

    public MyMModelKey(final String cacheName, final MyMLMethodName methodName)
    {
        this.cacheName = cacheName;
        this.methodName = methodName;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public MyMLMethodName getMethodName() {
        return methodName;
    }

    public void setMethodName(MyMLMethodName methodName) {
        this.methodName = methodName;
    }

    public List<MyMPs> getLstPs() {
        return lstPs;
    }

    public void setLstPs(List<MyMPs> lstPs) {
        this.lstPs = lstPs;
    }

    @Override
    public String toString() {
        return "MyMModelKey{" +
                "cacheName='" + cacheName + '\'' +
                ", methodName=" + methodName +
                ", lstPs=" + lstPs +
                '}';
    }
}
