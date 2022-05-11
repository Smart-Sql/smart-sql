package cn.plus.model;

import java.io.Serializable;

/**
 * 超级管理员设置
 * 用户组
 * */
public class MyUsersGroup implements Serializable {
    private static final long serialVersionUID = -2653254803806803757L;

    private Long id;
    /**
     * 用户组名称
     * */
    private String group_name;
    /**
     * 数据集的 ID
     * */
    private Long data_set_id;

    /**
     * 用户 token 这个用在连接字符串
     * */
    private String user_token;

    /**
     * 用户组对于 sql 的类型
     * DDL, DML, 或者都有
     * */
    private String group_type;
    private MyGroupSqlType myGroupSqlType;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public Long getData_set_id() {
        return data_set_id;
    }

    public void setData_set_id(Long data_set_id) {
        this.data_set_id = data_set_id;
    }

    public String getGroup_type() {
        return group_type;
    }

    public void setGroup_type(String group_type) throws Exception {
        switch (group_type.toUpperCase())
        {
            case "DDL":
                this.group_type = "DDL";
                this.myGroupSqlType = MyGroupSqlType.DDL;
                break;
            case "DML":
                this.group_type = "DML";
                this.myGroupSqlType = MyGroupSqlType.DML;
                break;
            case "ALL":
                this.group_type = "ALL";
                this.myGroupSqlType = MyGroupSqlType.ALL;
                break;
            default:
                throw new Exception("group_type 的值，只能是 DDL, DML, ALL 这三种！");
        }
    }

    public MyGroupSqlType getMyGroupSqlType() {
        return myGroupSqlType;
    }

    public void setMyGroupSqlType(MyGroupSqlType myGroupSqlType) throws Exception {
        this.myGroupSqlType = myGroupSqlType;
        this.setGroup_type(this.myGroupSqlType.toString());
    }

    public String getUser_token() {
        return user_token;
    }

    public void setUser_token(String user_token) {
        this.user_token = user_token;
    }
}
