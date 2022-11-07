package org.gridgain.dml.util;

import clojure.lang.*;
import cn.plus.model.*;
import cn.smart.service.IMyLogTrans;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.smart.service.MyLogService;
import org.apache.ignite.transactions.Transaction;
import org.tools.KvSql;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.tools.MyLineToBinary;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * sql 和 nosql 的增删改
 * */
public class MyCacheExUtil implements Serializable {
    private static final long serialVersionUID = 7714300623488330841L;

    private static IMyLogTrans myLog = MyLogService.getInstance().getMyLog();

//    public static void transLogCache(final Ignite ignite, final List<MyLogCache> lstLogCache)
//    {
//        List<MyCacheEx> lstCache = lstLogCache.stream().map(m -> convertToCacheEx(ignite, m)).collect(Collectors.toList());
//        long log_id = ignite.atomicSequence("my_log", 0, true).incrementAndGet();
//        List<MyCacheEx> lst = lstLogCache.stream().map(m -> new MyCacheEx(ignite.cache("my_log"), log_id, MyCacheExUtil.objToBytes(m), SqlType.INSERT)).collect(Collectors.toList());
//        lstCache.addAll(lst);
//        transMyCache(ignite, lstCache);
//    }

    public static void transLogCache(final Ignite ignite, final List lstLogCache)
    {
        List<MyCacheEx> lstCache = (List<MyCacheEx>) lstLogCache.stream().map(m -> convertToCacheEx(ignite, m)).collect(Collectors.toList());
//        if (ignite.configuration().isMyLogEnabled() && myLog != null) {
//            long log_id = ignite.atomicSequence("my_log", 0, true).incrementAndGet();
//            List<MyCacheEx> lst = (List<MyCacheEx>) lstLogCache.stream().map(m -> new MyCacheEx(ignite.cache("my_log"), log_id, MyCacheExUtil.objToBytes(m), SqlType.INSERT)).collect(Collectors.toList());
//            lstCache.addAll(lst);
//        }
        if (lstCache != null && lstCache.size() > 0) {
            transMyCache(ignite, lstCache);
        }
    }

    public static void transCache(final Ignite ignite, final List<MyLogCache> lstLogCache)
    {
        List<MyCacheEx> lstCache = lstLogCache.stream().map(m -> convertToCacheEx(ignite, m)).collect(Collectors.toList());

        transMyCache(ignite, lstCache);
    }

    public static void transCache(final Ignite ignite, final PersistentVector lstLogCache)
    {
        transMyCache(ignite, (List<MyCacheEx>) lstLogCache.stream().map(m -> convertToCacheEx(ignite, (MyLogCache)m)).collect(Collectors.toList()));
    }

    public static void transCache(final Ignite ignite, final PersistentList lstLogCache)
    {
        transMyCache(ignite, (List<MyCacheEx>) lstLogCache.stream().map(m -> convertToCacheEx(ignite, (MyLogCache)m)).collect(Collectors.toList()));
    }

    public static void transCache(final Ignite ignite, final LazySeq lstLogCache)
    {
        transMyCache(ignite, (List<MyCacheEx>) lstLogCache.stream().map(m -> convertToCacheEx(ignite, (MyLogCache)m)).collect(Collectors.toList()));
    }

    private static void transMyCache(final Ignite ignite, final List<MyCacheEx> lstCache) {
        //List<MyCacheEx> lstCache = lstLogCache.stream().map(m -> convertToCacheEx(ignite, m)).collect(Collectors.toList());
        if (myLog != null)
        {
            IgniteTransactions transactions = ignite.transactions();
            Transaction tx = null;
            String transSession = UUID.randomUUID().toString();
            try {
                tx = transactions.txStart();
                myLog.createSession(transSession);

                for (MyCacheEx m : lstCache)
                {
                    switch (m.getSqlType()) {
                        case UPDATE:
                            m.getCache().replace(m.getKey(), m.getValue());
                            myLog.saveTo(transSession, MyCacheExUtil.objToBytes(m.getData()));
                            break;
                        case INSERT:
                            m.getCache().put(m.getKey(), m.getValue());
                            myLog.saveTo(transSession, MyCacheExUtil.objToBytes(m.getData()));
                            break;
                        case DELETE:
                            m.getCache().remove(m.getKey());
                            myLog.saveTo(transSession, MyCacheExUtil.objToBytes(m.getData()));
                            break;
                    }
                }

                myLog.commit(transSession);
                tx.commit();
            } catch (Exception ex) {
                if (tx != null) {
                    myLog.rollback(transSession);
                    tx.rollback();
                }
                throw ex;
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
        else
        {
            IgniteTransactions transactions = ignite.transactions();
            Transaction tx = null;
            try {
                tx = transactions.txStart();

                for (MyCacheEx m : lstCache)
                {
                    switch (m.getSqlType()) {
                        case UPDATE:
                            m.getCache().replace(m.getKey(), m.getValue());
                            break;
                        case INSERT:
                            m.getCache().put(m.getKey(), m.getValue());
                            break;
                        case DELETE:
                            m.getCache().remove(m.getKey());
                            break;
                    }
                }

                tx.commit();
            } catch (Exception ex) {
                if (tx != null) {
                    tx.rollback();
                }
                throw ex;
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
    }

    /**
     * MyLogCache 转换到 MyCacheEx
     * */
    public static MyCacheEx convertToCacheEx(final Ignite ignite, final Object logCache)
    {
        if (logCache instanceof MyLogCache) {
            try {
                return convertToCacheEx_logCache(ignite, (MyLogCache) logCache);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            return convertToCacheEx_noSql(ignite, (MyNoSqlCache) logCache);
        }
        return null;
    }

    private static Boolean isLstKv(Object o)
    {
        if (o instanceof List)
        {
            List lst = (List) o;
            if (lst.size() > 0 && lst.get(0) instanceof MyKeyValue)
            {
                return true;
            }
        }
        return false;
    }

    public static MyCacheEx convertToCacheEx_logCache(final Ignite ignite, final MyLogCache logCache) throws Exception {
        IgniteCache myIgniteCache = ignite.cache(logCache.getCache_name());
        if (myIgniteCache != null) {
            switch (logCache.getSqlType()) {
                case INSERT:
                    return new MyCacheEx(myIgniteCache, convertToKey(ignite, logCache), convertToValue(ignite, logCache), logCache.getSqlType(), logCache);
                case DELETE:
                    return new MyCacheEx(myIgniteCache, convertToKey(ignite, logCache), null, logCache.getSqlType(), logCache);
                case UPDATE:
                    Object key = convertToKey(ignite, logCache);
                    IgniteCache igniteCache = myIgniteCache.withKeepBinary();
                    BinaryObject binaryObject = (BinaryObject) igniteCache.get(key);
                    if (isLstKv(logCache.getValue())) {
                        BinaryObjectBuilder binaryObjectBuilder = binaryObject.toBuilder();
                        for (MyKeyValue m : (List<MyKeyValue>) logCache.getValue()) {
                            binaryObjectBuilder.setField(m.getName(), m.getValue());
                        }
                        return new MyCacheEx(igniteCache, key, binaryObjectBuilder.build(), logCache.getSqlType(), logCache);
                    } else {
                        return new MyCacheEx(igniteCache, key, logCache.getValue(), logCache.getSqlType(), logCache);
                    }
            }
        }
        else
        {
            throw new Exception("缓存不存在！");
        }
        return null;
    }

    public static MyCacheEx convertToCacheEx_noSql(final Ignite ignite, final MyNoSqlCache sqlCache)
    {
        switch (sqlCache.getSqlType())
        {
            case INSERT:
                return new MyCacheEx(ignite.cache(sqlCache.getCache_name()), sqlCache.getKey(), sqlCache.getValue(), sqlCache.getSqlType(), sqlCache);
            case DELETE:
                return new MyCacheEx(ignite.cache(sqlCache.getCache_name()), sqlCache.getKey(), null, sqlCache.getSqlType(), sqlCache);
            case UPDATE:
                return new MyCacheEx(ignite.cache(sqlCache.getCache_name()), sqlCache.getKey(), sqlCache.getValue(), sqlCache.getSqlType(), sqlCache);
        }
        return null;
    }

    public static MyCacheEx convertToCacheEx(final Ignite ignite, final byte[] bytes)
    {
        return convertToCacheEx(ignite, (MyLogCache)MyCacheExUtil.restore(bytes));
    }

    public static byte[] mycacheToBytes(final MyLogCache myLogCache)
    {
        return objToBytes(myLogCache);
    }

    public static MyLogCache restoreMyCache(final byte[] bytes)
    {
        return (MyLogCache)restore(bytes);
    }

    public static BinaryObject getValues(final Ignite ignite, final String table_name, final List<MyKeyValue> lst)
    {
        BinaryObjectBuilder valueBuilder = ignite.binary().builder(KvSql.getValueType(ignite, table_name));
        for (MyKeyValue m : lst)
        {
            valueBuilder.setField(m.getName(), m.getValue());
        }
        return valueBuilder.build();
    }

    public static BinaryObject getKeys(final Ignite ignite, final String table_name, final List<MyKeyValue> lst)
    {
        BinaryObjectBuilder keyBuilder = ignite.binary().builder(KvSql.getKeyType(ignite, table_name));
        for (MyKeyValue m : lst)
        {
            if (m.getValue() != null) {
                keyBuilder.setField(m.getName(), m.getValue());
            }
            else
            {
                keyBuilder.setField(m.getName(), null);
            }
        }
        return keyBuilder.build();
    }

    public static Object convertToKey(final Ignite ignite, final MyLogCache myLogCache)
    {
        Object key = null;
        if (isLstKv(myLogCache.getKey())) {
            key = MyCacheExUtil.getKeys(ignite, myLogCache.getCache_name(), (List<MyKeyValue>)myLogCache.getKey());
        } else {
            key = myLogCache.getKey();
        }
        return key;
    }

    public static Object convertToValue(final Ignite ignite, final MyLogCache myLogCache)
    {
        Object value = null;
        if (isLstKv(myLogCache.getValue())) {
            value = MyCacheExUtil.getValues(ignite, myLogCache.getCache_name(), (List<MyKeyValue>)myLogCache.getValue());
        } else {
            value = myLogCache.getValue();
        }
        return value;
    }

    public static byte[] objToBytes(final Object obj) {
        return MyLineToBinary.objToBytes(obj);
    }

    /**
     * 二进制数组转回对象
     * */
    public static Object restore(final byte[] bytes)
    {
        return MyLineToBinary.restore(bytes);
    }

    public static String restoreToLine(final byte[] bytes)
    {
        return (String)restore(bytes);
    }

    public static void restoreNoSqlCache(final Ignite ignite, final MyNoSqlCache myNoSqlCache)
    {
        MyCacheEx myCacheEx = convertToCacheEx_noSql(ignite, myNoSqlCache);
        switch (myCacheEx.getSqlType())
        {
            case INSERT:
                myCacheEx.getCache().put(myCacheEx.getKey(), myCacheEx.getValue());
                break;
            case DELETE:
                myCacheEx.getCache().remove(myCacheEx.getKey());
                break;
            case UPDATE:
                myCacheEx.getCache().replace(myCacheEx.getKey(), myCacheEx.getValue());
                break;
        }
    }

    public static void restoreLogCache(final Ignite ignite, final MyLogCache myLogCache) throws Exception {
        MyCacheEx myCacheEx = convertToCacheEx_logCache(ignite, myLogCache);
        switch (myCacheEx.getSqlType())
        {
            case INSERT:
                myCacheEx.getCache().put(myCacheEx.getKey(), myCacheEx.getValue());
                break;
            case DELETE:
                myCacheEx.getCache().remove(myCacheEx.getKey());
                break;
            case UPDATE:
                myCacheEx.getCache().replace(myCacheEx.getKey(), myCacheEx.getValue());
                break;
        }
    }

    public static void restoreListData(final Ignite ignite, final List lst)
    {
        IgniteTransactions transactions = ignite.transactions();
        Transaction tx = null;
        try {
            tx = transactions.txStart();

            for (Object m : lst)
            {
                MyCacheEx myCacheEx = convertToCacheEx(ignite, m);
                switch (myCacheEx.getSqlType())
                {
                    case INSERT:
                        myCacheEx.getCache().put(myCacheEx.getKey(), myCacheEx.getValue());
                        break;
                    case DELETE:
                        myCacheEx.getCache().remove(myCacheEx.getKey());
                        break;
                    case UPDATE:
                        myCacheEx.getCache().replace(myCacheEx.getKey(), myCacheEx.getValue());
                        break;
                }
            }

            tx.commit();
        } catch (Exception ex) {
            if (tx != null) {
                tx.rollback();
            }
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
    }

//    /**
//     * 对象转变成二进制
//     * */
//    public static byte[] objToBytes(final Object obj) {
//        ByteArrayOutputStream byteArrayOutputStream = null;
//        ObjectOutputStream objectOutputStream = null;
//
//        try {
//            byteArrayOutputStream = new ByteArrayOutputStream();
//            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//            objectOutputStream.writeObject(obj);
//            return byteArrayOutputStream.toByteArray();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (objectOutputStream != null) {
//                try {
//                    objectOutputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            if (byteArrayOutputStream != null) {
//                try {
//                    byteArrayOutputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return null;
//    }

//    /**
//     * 二进制数组转回对象
//     * */
//    public static Object restore(final byte[] bytes)
//    {
//        ByteArrayInputStream byteArrayInputStream = null;
//        ObjectInputStream objectInputStream = null;
//
//        try {
//            byteArrayInputStream = new ByteArrayInputStream(bytes);
//            objectInputStream = new ObjectInputStream(byteArrayInputStream);
//            return objectInputStream.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        } finally {
//            if (objectInputStream != null) {
//                try {
//                    objectInputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            if (byteArrayInputStream != null) {
//                try {
//                    byteArrayInputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return null;
//    }
}











































