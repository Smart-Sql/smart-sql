package org.gridgain.myservice;

import org.gridgain.superservice.IJavaUtil;

public class MyJavaUtilService {
    private IJavaUtil javaUtil;

    public IJavaUtil getJavaUtil() {
        return javaUtil;
    }

    private static class InstanceHolder {

        public static MyJavaUtilService instance;

        static {
            try {
                instance = new MyJavaUtilService();
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
    public static MyJavaUtilService getInstance() {
        return MyJavaUtilService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyJavaUtilService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.tools.MyJavaUtil");
        javaUtil = (IJavaUtil) cls.newInstance();
    }
}
