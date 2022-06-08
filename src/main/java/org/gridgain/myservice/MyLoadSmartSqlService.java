package org.gridgain.myservice;

import org.gridgain.superservice.ILoadSmartSql;

public class MyLoadSmartSqlService {
    private ILoadSmartSql loadSmartSql;

    public ILoadSmartSql getLoadSmartSql() {
        return loadSmartSql;
    }

    private static class InstanceHolder {

        public static MyLoadSmartSqlService instance;

        static {
            try {
                instance = new MyLoadSmartSqlService();
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
    public static MyLoadSmartSqlService getInstance() {
        return MyLoadSmartSqlService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyLoadSmartSqlService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.dml.MyLoadSmartSql");
        loadSmartSql = (ILoadSmartSql) cls.newInstance();
    }
}
