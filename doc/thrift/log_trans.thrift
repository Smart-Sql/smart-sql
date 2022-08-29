namespace java org.gridgain.smart.log

/**
 * log trans
 * thrift -r --gen java /Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/thrift/log_trans.thrift
*/
service MyLogTransService
{
    /**
     *  开始事务的会话
     *  参数传入会话的 ID
     */
    void createSession(1: string tranSession),

    /**
     *  保存数据
     *  参数传入会话的 ID
     *  参数传入要保存的二进制数据
     */
    void saveTo(1: string tranSession, 2: binary data),

    /**
     *  提交事务
     *  参数传入会话的 ID
     */
    void commit(1: string tranSession),

    /**
     *  回滚事务
     *  参数传入会话的 ID
     */
    void rollback(1: string tranSession)
}