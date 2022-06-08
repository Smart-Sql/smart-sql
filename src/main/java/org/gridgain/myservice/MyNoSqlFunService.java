package org.gridgain.myservice;

import org.gridgain.superservice.INoSqlFun;

public class MyNoSqlFunService {
    private INoSqlFun noSqlFun;

    public INoSqlFun getNoSqlFun() {
        return noSqlFun;
    }

    private static class InstanceHolder {

        public static MyNoSqlFunService instance;

        static {
            try {
                instance = new MyNoSqlFunService();
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
    public static MyNoSqlFunService getInstance() {
        return MyNoSqlFunService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyNoSqlFunService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.dml.MySmartDb");
        noSqlFun = (INoSqlFun) cls.newInstance();
    }
}
