package cn.plus.model;

import java.io.Serializable;

/**
 * Rpc 信息
 * */
public class RpcInfo implements Serializable {

    private static final long serialVersionUID = -6338610523542559889L;

    private String node_name;
    private String ip;
    //private int editPort;
    private int clsPort;
    private int servicePort;

//    public int getEditPort() {
//        return editPort;
//    }
//
//    public void setEditPort(int editPort) {
//        this.editPort = editPort;
//    }

    public int getClsPort() {
        return clsPort;
    }

    public void setClsPort(int clsPort) {
        this.clsPort = clsPort;
    }

    public int getServicePort() {
        return servicePort;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public String getNode_name() {
        return node_name;
    }

    public void setNode_name(String node_name) {
        this.node_name = node_name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

}
