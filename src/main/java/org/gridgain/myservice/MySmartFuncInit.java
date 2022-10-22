package org.gridgain.myservice;

import org.gridgain.superservice.ISmartFuncInit;

public class MySmartFuncInit {
    private ISmartFuncInit smartFuncInit;

    private static class InstanceHolder {

        public static MySmartFuncInit instance;

        static {
            try {
                instance = new MySmartFuncInit();
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
    public static MySmartFuncInit getInstance() {
        return MySmartFuncInit.InstanceHolder.instance;
    }

    public ISmartFuncInit getSmartFuncInit() {
        return smartFuncInit;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MySmartFuncInit() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.tools.MySmartInitFunc");
        smartFuncInit = (ISmartFuncInit) cls.newInstance();
    }
}
