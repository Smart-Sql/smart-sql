package cn.plus.model;

import clojure.lang.PersistentArrayMap;

import java.io.Serializable;

/**
 * 定时任务
 * */
public class MCron implements Serializable {

    private static final long serialVersionUID = 5363891392457651930L;
    private String cron_name;
    private String cron;
    private byte[] ps;
    private String descrip;
    private clojure.lang.PersistentArrayMap ast;

    public MCron(final String cron_name, final String cron, final String descrip, final clojure.lang.PersistentArrayMap ast)
    {
        this.cron_name = cron_name;
        this.cron = cron;
        this.descrip = descrip;
        this.ast = ast;
    }

    public MCron(final String cron_name, final String cron, final byte[] ps)
    {
        this.cron_name = cron_name;
        this.cron = cron;
        this.ps = ps;
    }

    public MCron()
    {}

    public byte[] getPs() {
        return ps;
    }

    public void setPs(byte[] ps) {
        this.ps = ps;
    }

    public PersistentArrayMap getAst() {
        return ast;
    }

    public void setAst(PersistentArrayMap ast) {
        this.ast = ast;
    }

    public String getCron_name() {
        return cron_name;
    }

    public void setCron_name(String cron_name) {
        this.cron_name = cron_name;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    @Override
    public String toString() {
        return "MCron{" +
                "cron_name='" + cron_name + '\'' +
                ", cron='" + cron + '\'' +
                ", descrip='" + descrip + '\'' +
                ", ast=" + ast +
                '}';
    }
}
