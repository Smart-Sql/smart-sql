package org.gridgain.myservice;

import org.gridgain.superservice.IMyRpc;

public class MyRpcFuncService {
    private IMyRpc myRpc;

    public IMyRpc getMyRpc() {
        return myRpc;
    }

    private static class InstanceHolder {

        public static MyRpcFuncService instance;

        static {
            try {
                instance = new MyRpcFuncService();
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
    public static MyRpcFuncService getInstance() {
        return MyRpcFuncService.InstanceHolder.instance;
    }

    /**
     * 构造函数设置为私有，只能通过 getInstance() 方法获取
     * */
    private MyRpcFuncService() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> cls = Class.forName("org.gridgain.plus.rpc.MyRpc");
        myRpc = (IMyRpc) cls.newInstance();
    }
}
