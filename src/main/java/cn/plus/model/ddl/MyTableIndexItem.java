package cn.plus.model.ddl;

import java.io.Serializable;

/**
 * 对应表 table_index_item
 * */
public class MyTableIndexItem implements Serializable {
    private static final long serialVersionUID = 6610781692448074456L;

    private Long id;
    private String index_item;
    private String sort_order;
    private Long index_no;

    public MyTableIndexItem(final Long id, final String index_item, final String sort_order, final Long index_no)
    {
        this.id = id;
        this.index_item = index_item;
        this.sort_order = sort_order;
        this.index_no = index_no;
    }

    public MyTableIndexItem()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIndex_item() {
        return index_item;
    }

    public void setIndex_item(String index_item) {
        this.index_item = index_item;
    }

    public String getSort_order() {
        return sort_order;
    }

    public void setSort_order(String sort_order) {
        this.sort_order = sort_order;
    }

    public Long getIndex_no() {
        return index_no;
    }

    public void setIndex_no(Long index_no) {
        this.index_no = index_no;
    }

    @Override
    public String toString() {
        return "MyTableIndexItem{" +
                "id=" + id +
                ", index_item='" + index_item + '\'' +
                ", sort_order='" + sort_order + '\'' +
                ", index_no=" + index_no +
                '}';
    }
}
