package org.gridgain.superservice;

public interface IMyRpc {
    public String executeSqlQuery(String userToken, String line, String ps);
}
