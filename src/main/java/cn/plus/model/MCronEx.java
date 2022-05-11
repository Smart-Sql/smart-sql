package cn.plus.model;

import java.io.Serializable;

public class MCronEx implements Serializable {
    private static final long serialVersionUID = -8937649811554650575L;

    private String cron_name;
    private String cron;
    private String descrip;
    private String cljCode;

    public MCronEx(final String cron_name, final String cron, final String descrip, final String cljCode)
    {
        this.cron_name = cron_name;
        this.cron = cron;
        this.descrip = descrip;
        this.cljCode = cljCode;
    }

    public MCronEx()
    {}

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

    public String getCljCode() {
        return cljCode;
    }

    public void setCljCode(String cljCode) {
        this.cljCode = cljCode;
    }
}
