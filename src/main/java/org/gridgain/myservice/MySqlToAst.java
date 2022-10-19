package org.gridgain.myservice;

import org.gridgain.superservice.ISqlToAst;

public class MySqlToAst {
    private ISqlToAst sqlToAst;

    public ISqlToAst getSqlToAst() {
        return sqlToAst;
    }

    private static class InstanceHolder {

        public static MySqlToAst instance;

        static {
            try {
                instance = new MySqlToAst();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取单例模式
     * */
    public static MySqlToAst getInstance() {
        return MySqlToAst.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MySqlToAst() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.dml.MySelectPlus");
        sqlToAst = (ISqlToAst) cls.newInstance();
    }
}
