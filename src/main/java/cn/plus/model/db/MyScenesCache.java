package cn.plus.model.db;

import java.io.Serializable;

/**
 * 对应于 my_scenes 表
 * */
public class MyScenesCache implements Serializable {
    private static final long serialVersionUID = -7489535188547987509L;

    // schema_name
    private String schema_name;
    // 场景名字 （主键id）
    private String scenes_name;
    // 场景 code
    private String sql_code;
    // 场景描述
    private String descrip;
    // 参数
    //private List<MyScenesParams> params;
    private String params;

    public MyScenesCache(final String schema_name, final String scenes_name, final String sql_code, final String descrip, final String params)
    {
        this.schema_name = schema_name;
        this.scenes_name = scenes_name;
        this.sql_code = sql_code;
        this.descrip = descrip;
        this.params = params;
    }

    public MyScenesCache()
    {}

    public String getScenes_name() {
        return scenes_name;
    }

    public void setScenes_name(String scenes_name) {
        this.scenes_name = scenes_name;
    }

    public String getSql_code() {
        return sql_code;
    }

    public void setSql_code(String sql_code) {
        this.sql_code = sql_code;
    }

    public String getDescrip() {
        return descrip;
    }

    public void setDescrip(String descrip) {
        this.descrip = descrip;
    }

    @Override
    public String toString() {
        return "MyScenesCache{" +
                "schema_name='" + schema_name + '\'' +
                ", scenes_name='" + scenes_name + '\'' +
                ", sql_code='" + sql_code + '\'' +
                ", descrip='" + descrip + '\'' +
                ", params='" + params + '\'' +
                '}';
    }
}







































