package cn.plus.model;

import clojure.lang.PersistentArrayMap;

import java.io.Serializable;

/**
 * 定时任务
 * */
public class MCron implements Serializable {

    private static final long serialVersionUID = 5363891392457651930L;
    private String job_name;
    private String cron;
    private String ps;
    private Long group_id;
//    private String descrip;
//
//    public MCron(final String job_name, final String cron, final String ps, final String descrip)
//    {
//        this.job_name = job_name;
//        this.cron = cron;
//        this.ps = ps;
//        this.descrip = descrip;
//    }

    public MCron(final String job_name, final Long group_id, final String cron, final String ps)
    {
        this.job_name = job_name;
        this.group_id = group_id;
        this.cron = cron;
        this.ps = ps;
    }

    public MCron()
    {}

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }

    public String getJob_name() {
        return job_name;
    }

    public void setJob_name(String job_name) {
        this.job_name = job_name;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getPs() {
        return ps;
    }

    public void setPs(String ps) {
        this.ps = ps;
    }

    @Override
    public String toString() {
        return "MCron{" +
                "job_name='" + job_name + '\'' +
                ", cron='" + cron + '\'' +
                ", ps='" + ps + '\'' +
                '}';
    }
}
