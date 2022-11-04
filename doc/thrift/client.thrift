namespace java org.gridgain.dawn.rpc
namespace py rpc

/**
 * rpc for client
 * thrift -r --gen java /Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/thrift/client.thrift
 * thrift -r --gen py /Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/thrift/client.thrift
*/
service MyRpcService
{
    /**
     *  执行 sql
     *  参数 sql 语句
     */
    string executeSqlQuery(1: string sql)
}