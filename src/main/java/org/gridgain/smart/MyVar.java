package org.gridgain.smart;

import clojure.lang.PersistentVector;
import org.gridgain.myservice.MyJavaUtilService;

import java.io.Serializable;
import java.util.List;

/**
 * spirit sql 里面的对变量的赋值
 * */
public class MyVar implements Serializable {
    private static final long serialVersionUID = 5645425225182146226L;
    private MyJavaUtilService myJavaUtilService = MyJavaUtilService.getInstance();
    private Object var;
    private String varType;

    public MyVar()
    {}

    public MyVar(final Object var)
    {
        if (var instanceof List)
        {
            this.varType = "List";
        }

        this.var = myJavaUtilService.getJavaUtil().toArrayOrHashtable(var);
    }

    public MyVar(final Object var, final String varType)
    {
        this.var = var;
        this.varType = varType;
    }

    public Object getVar() {
        return var;
    }

    public void setVar(Object var) {
        this.var = var;
    }

    public String getVarType() {
        return varType;
    }

    public void setVarType(String varType) {
        this.varType = varType;
    }

    @Override
    public String toString() {
        return "MyVar{" +
                "var=" + var +
                ", varType='" + varType + '\'' +
                '}';
    }
}
