package org.dawn.rpc;

import org.apache.thrift.TException;
import org.gridgain.myservice.MyRpcFuncService;

public class MyRpcServiceImpl implements MyRpcService.Iface {
    @Override
    public String executeSqlQuery(String userToken, String sql, String ps) throws TException {
        return MyRpcFuncService.getInstance().getMyRpc().executeSqlQuery(userToken, sql, ps);
    }
//    @Override
//    public String executeSqlQuery(String sql) throws TException {
//        return MyRpcFuncService.getInstance().getMyRpc().executeSqlQuery(sql);
//    }
}
