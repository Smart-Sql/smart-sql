package org.jvm;

public class MyInfoService {
    private IMyInfo myInfo;

    public IMyInfo getMyInfo() {
        return myInfo;
    }

    private static class InstanceHolder {

        public static MyInfoService instance;

        static {
            try {
                instance = new MyInfoService();
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
    public static MyInfoService getInstance() {
        return MyInfoService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyInfoService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("cn.jvm.MySuperFuncs");
        myInfo = (IMyInfo) cls.newInstance();
    }
}
