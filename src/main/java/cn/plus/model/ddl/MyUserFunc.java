package cn.plus.model.ddl;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class MyUserFunc implements Serializable {
    private static final long serialVersionUID = 8331935187125596376L;

    private String method_name;
    private String java_method_name;
    private String cls_name;
    private String return_type;
    private String descrip;
    private List<MyFuncPs> lst;

    public MyUserFunc(final String method_name, final String java_method_name, final String cls_name, final String return_type, List<MyFuncPs> lst, final String descrip)
    {
        this.method_name = method_name != null? method_name.toLowerCase(): "";
        this.java_method_name = java_method_name;
        this.cls_name = cls_name;
        this.return_type = return_type;
        this.descrip = descrip;
        //lst = new ArrayList<>();
        if (lst != null) {
            this.lst = lst;
        }
    }

    public MyUserFunc()
    {}

    public String getMethod_name() {
        return method_name;
    }

    public void setMethod_name(String method_name) {
        this.method_name = method_name != null? method_name.toLowerCase(): "";
    }

    public String getJava_method_name() {
        return java_method_name;
    }

    public void setJava_method_name(String java_method_name) {
        this.java_method_name = java_method_name;
    }

    public String getCls_name() {
        return cls_name;
    }

    public void setCls_name(String cls_name) {
        this.cls_name = cls_name;
    }

    public String getReturn_type() {
        return return_type;
    }

    public void setReturn_type(String return_type) {
        this.return_type = return_type;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    public List<MyFuncPs> getLst() {
        return lst;
    }

    public void setLst(List<MyFuncPs> lst) {
        this.lst = lst;
    }

    @Override
    public String toString() {
        return "MyUserFunc{" +
                "method_name='" + method_name + '\'' +
                ", java_method_name='" + java_method_name + '\'' +
                ", cls_name='" + cls_name + '\'' +
                ", return_type='" + return_type + '\'' +
                ", descrip='" + descrip + '\'' +
                ", lst=" + lst +
                '}';
    }
}
