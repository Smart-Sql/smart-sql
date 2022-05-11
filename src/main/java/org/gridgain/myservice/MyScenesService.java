package org.gridgain.myservice;

import org.gridgain.superservice.IMyScenes;

public class MyScenesService {
    private IMyScenes myScenes;

    public IMyScenes getMyScenes() {
        return myScenes;
    }

    private static class InstanceHolder {

        public static MyScenesService instance;

        static {
            try {
                instance = new MyScenesService();
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
    public static MyScenesService getInstance() {
        return MyScenesService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyScenesService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.dml.MyScenes");
        myScenes = (IMyScenes) cls.newInstance();
    }
}
