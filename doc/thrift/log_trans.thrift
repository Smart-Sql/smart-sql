namespace java org.gridgain.smart.log

/**
 * log trans
 * thrift -r --gen java /Users/chenfei/Documents/Java/MyGridGain/smart-sql/doc/thrift/log_trans.thrift
*/
service MyLogTrans
{
    void createSession(1: string tranSession),

    void saveTo(1: string tranSession, 2: binary data),

    void commit(1: string tranSession),

    void rollback(1: string tranSession)
}