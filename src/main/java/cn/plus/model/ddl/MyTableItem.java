package cn.plus.model.ddl;

import java.io.Serializable;

public class MyTableItem implements Serializable {
    private static final long serialVersionUID = -5470859524094296824L;

    private Long id;
    private String column_name;
    private Integer column_len;
    private Integer scale;
    private String column_type = "";
    private Boolean not_null = true;
    private Boolean pkid = false;
    private String comment = "";
    //private Integer comment_len;
    private Boolean auto_increment = false;
    private Long table_id;
    private String default_value = "";
    //private Long ex_table_id_;

    public MyTableItem(final Long id, final Long table_id, final String column_name, final Integer column_len, final Integer scale, final String column_type, final Boolean not_null, final Boolean pkid,
                       final String comment, /*final Integer comment_len, */final Boolean auto_increment, final String default_value)
    {
        this.id = id;
        this.table_id = table_id;
        if (column_name != null) {
            this.column_name = column_name.toLowerCase();
        }
        this.column_len = column_len;
        this.scale = scale;
        this.column_type = column_type;
        this.not_null = not_null;
        this.pkid = pkid;
        this.comment = comment;
        //this.comment_len = comment_len;
        this.auto_increment = auto_increment;
        this.default_value = default_value;
        //this.ex_table_id_ = table_id;
    }

    public MyTableItem(final String column_name, final Integer column_len, final Integer scale, final String column_type, final Boolean not_null, final Boolean pkid,
                       final String comment, /*final Integer comment_len, */final Boolean auto_increment, final String default_value)
    {
        if (column_name != null) {
            this.column_name = column_name.toLowerCase();
        }
        this.column_len = column_len;
        this.scale = scale;
        this.column_type = column_type;
        this.not_null = not_null;
        this.pkid = pkid;
        this.comment = comment;
        //this.comment_len = comment_len;
        this.auto_increment = auto_increment;
        this.default_value = default_value;
    }

    public MyTableItem()
    {}

    public Boolean getNot_null() {
        return not_null;
    }

    public void setNot_null(Boolean not_null) {
        this.not_null = not_null;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        if (column_name != null) {
            this.column_name = column_name.toLowerCase();
        }
    }

    public Integer getColumn_len() {
        return column_len;
    }

    public void setColumn_len(Integer column_len) {
        this.column_len = column_len;
    }

    public String getColumn_type() {
        return column_type;
    }

    public void setColumn_type(String column_type) {
        this.column_type = column_type;
    }

    public Boolean getPkid() {
        return pkid;
    }

    public void setPkid(Boolean pkid) {
        this.pkid = pkid;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /*
    public Integer getComment_len() {
        return comment_len;
    }

    public void setComment_len(Integer comment_len) {
        this.comment_len = comment_len;
    }
     */

    public Boolean getAuto_increment() {
        return auto_increment;
    }

    public void setAuto_increment(Boolean auto_increment) {
        this.auto_increment = auto_increment;
    }

    public Long getTable_id() {
        return table_id;
    }

    public void setTable_id(Long table_id) {
        this.table_id = table_id;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public String getDefault_value() {
        return default_value;
    }

    public void setDefault_value(String default_value) {
        this.default_value = default_value;
    }

    @Override
    public String toString() {
        return "MyTableItem{" +
                "id=" + id +
                ", column_name='" + column_name + '\'' +
                ", column_len=" + column_len +
                ", scale=" + scale +
                ", column_type='" + column_type + '\'' +
                ", not_null=" + not_null +
                ", pkid=" + pkid +
                ", comment='" + comment + '\'' +
                ", auto_increment=" + auto_increment +
                ", table_id=" + table_id +
                ", default_value='" + default_value + '\'' +
                '}';
    }
}
