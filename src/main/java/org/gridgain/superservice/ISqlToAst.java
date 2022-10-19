package org.gridgain.superservice;

import java.util.List;

public interface ISqlToAst {
    public Object sqlToAst(List lst);

    public Object mySqlToAst(String sql);
}
