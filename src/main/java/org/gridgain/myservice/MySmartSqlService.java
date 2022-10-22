package org.gridgain.myservice;

import org.gridgain.superservice.IMySmartSql;

public class MySmartSqlService {
    private IMySmartSql mySmartSql;

    private static class InstanceHolder {

        public static MySmartSqlService instance;

        static {
            try {
                instance = new MySmartSqlService();
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
    public static MySmartSqlService getInstance() {
        return MySmartSqlService.InstanceHolder.instance;
    }

    public IMySmartSql getMySmartSql() {
        return mySmartSql;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MySmartSqlService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.sql.MySuperSql");
        mySmartSql = (IMySmartSql) cls.newInstance();
    }
}
