package cn.plus.model.ddl;

import java.io.Serializable;

public class MyTableIndexPk implements Serializable {
    private static final long serialVersionUID = 4141566812550806474L;

    private Long id;
    private Long index_no;

    public MyTableIndexPk(final Long id, final Long index_no)
    {
        this.id = id;
        this.index_no = index_no;
    }

    public MyTableIndexPk()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getIndex_no() {
        return index_no;
    }

    public void setIndex_no(Long index_no) {
        this.index_no = index_no;
    }
}
