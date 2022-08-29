## Smart Sql 的运维

### Smart Sql 集群异地强一致性实时多活解决方案
  在金融行业中，我们要求数据是 100% 一致性，且稳定性要达到 5 个 9 既 99.999%。例如：我们有深圳机房、上海机房、贵州机房。我们要把深圳机房集群中的数据，实时事务性的复制到上海机房和贵州机房的集群。即使是深圳上海受到不可能抗击的毁灭，都不会影响数据的完整性、安全性、可用性！
  要达到这种要求，我们依赖两个机制，一个是集群本身，备份数据机制，既通过设置配置参数，可以达到一个集群中的，部分机器完全坏掉后，集群照样不受影响。二是依赖实时同步的事务机制，既深圳机房中集群的数据，是事务性的复杂到，其它集群。
  集群异地实时事务复制机制的原理：

1、在 Smart Sql 中将 insert、update、delete 的操作都转换成了 key-value 的操作，一条数据对应对应一个对象，把这个对象，传到高速存储器中。高速存储器在将对象写入异地的集群。

2、在事务中，本地集群和异地都提交成功了，才算提交成功，否则都会回滚！

**下面我们会做一个简单的例子来说明，异地强一致性实时多活解决方案。这个例子是有代表性的，但是在实际的过程中，用户可以参考这个例子做进一步的优化。例如：进一步优化传输效率，用磁盘阵列、固态硬盘的机器来作为，接收数据的服务器，等等**

**具体实现的思路：**
1. Smart Sql 集群中通过反射来调用 IMyLogTrans 接口的实现。在事务开始的时候，传递一个指令给服务器，让它也创建一个事务。Smart Sql 执行事务的时候，也将相应的 key-value 传递给服务器，当 Smart Sql 提交事务的时候，也发送一个指令给服务器，让服务器上相应的数据也提交。如果提交失败。(无论是 Smart Sql 集群，还是异地的服务器)都同时回滚！
2. 为了简单，我们用 Apache Thrift 作为通讯框架来演示，一下整个流程。但实际的业务要求可能比例子更复杂，如果读者有需要的话，我们可以单独交流。

2.1、Thrift 的定义
```java
namespace java org.gridgain.smart.log

/**
 * log trans
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
```

实现这个 thrift 的定义
```java
public class MyLogTransactionImpl implements MyLogTransService.Iface {

    private String path;
    private MyLogStore myStore;

    private static class InstanceHolder {
        public static MyLogTransactionImpl instance = new MyLogTransactionImpl();
    }

    public static MyLogTransactionImpl getInstance() {
        return MyLogTransactionImpl.InstanceHolder.instance;
    }

    public void setMyStore(String path) {
        this.path = path;
        this.myStore = MyLogStore.getInstance();
        this.myStore.setMvStore(path);
    }

    private MyLogTransactionImpl()
    {}

    @Override
    public void createSession(String tranSession) throws TException {
        this.myStore.createSession(tranSession);
    }

    @Override
    public void saveTo(String tranSession, ByteBuffer data) throws TException {
        this.myStore.saveTo(tranSession, MyConvertUtil.bufferToByte(data));
    }

    @Override
    public void commit(String tranSession) throws TException {
        this.myStore.commit(tranSession);
    }

    @Override
    public void rollback(String tranSession) throws TException {
        this.myStore.rollback(tranSession);
    }
}
```

Thrift 的服务
```java
public class MySmartLogService {
    private MyLogTransactionImpl myLogTransaction;

    public MySmartLogService()
    {
        myLogTransaction = MyLogTransactionImpl.getInstance();
 
 // 备份的二进制文件
 myLogTransaction.setMyStore("/Users/chenfei/Documents/Java/MyGridGainServer/MyGGServer/target/my.data");
    }

    public void start()
    {
        ExecutorService singlePool = Executors.newSingleThreadExecutor();
        singlePool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // 设置传输通道，普通通道
                    TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(8090);
                    // 使用高密度二进制协议
                    TProtocolFactory proFactory = new TCompactProtocol.Factory();
                    // 设置处理器 MyMetaServiceImpl

                    TProcessor processor = new MyLogTransService.Processor(myLogTransaction);

                    TNonblockingServer.Args tnbargs = new TNonblockingServer.Args(serverTransport);
                    tnbargs.processor(processor);
                    tnbargs.transportFactory(new TFramedTransport.Factory());
                    tnbargs.protocolFactory(proFactory);

                    // 使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
                    TServer server = new TNonblockingServer(tnbargs);
                    X.println("Log Server on port "+ String.valueOf(8090) +" ...");
                    server.serve();


                } catch (TTransportException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
```

客户端 MyLogTransactionClient 必须实现 IMyLogTrans 接口，同时将客户端 MyLogTransactionClient 类单独打成 jar 包放到，安装文件 lib 目录下的 cls 中。并且在配置文件default-config.xml 中，添加如下配置项：
```xml
<!-- 设置实现 log 事务接口的类 -->
<property name="myLogCls" value="org.gridgain.smart.logClient.MyLogTransactionClient"/>
```
这样当，数据库启动的时候，就会实例化这个类，并且让本地事务和远程事务保持一致。

MyLogTransactionClient 的实现：
```java

import cn.smart.service.IMyLogTrans;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.gridgain.smart.log.MyLogTransService;
import org.tools.MyConvertUtil;

public class MyLogTransactionClient implements IMyLogTrans {
    private TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 8090));
    private TProtocol protocol = new TCompactProtocol(transport);
    private MyLogTransService.Client client = new MyLogTransService.Client(protocol);

    public MyLogTransactionClient() throws TTransportException {
        transport.open();
    }

    @Override
    public void createSession(String tranSession) {
        try {
            client.createSession(tranSession);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveTo(String tranSession, byte[] bytes) {
        try {
            client.saveTo(tranSession, MyConvertUtil.byteToBuffer(bytes));
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void commit(String tranSession) {
        try {
            client.commit(tranSession);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void rollback(String tranSession) {
        try {
            client.rollback(tranSession);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

}
```

**例子中其它代码，在  my-back-up 模块中**

### Smart Sql 集群的安全设置
Smart Sql 中只允许，设置相同 token 的节点，并入集群。这样做是为了整个集群的数据安全。当然还可以根据业务的要求，进一步提升数据的安全性。这一块的扩展也非常的容易。
```xml
        <property name="pluginProviders">
            <list>
                <bean class="org.apache.ignite.smart.plugins.MyPluginNodeValidationProvider">
                    <!-- 相同 token 的节点才能并入集群 -->
                    <property name="token" value="MySmartSqlToken"/>
                </bean>
            </list>
        </property>
```