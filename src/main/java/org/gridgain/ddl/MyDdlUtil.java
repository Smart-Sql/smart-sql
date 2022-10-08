package org.gridgain.ddl;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import cn.plus.model.MyCacheEx;
import cn.plus.model.MyNoSqlCache;
import cn.plus.model.MySmartCache;
import cn.plus.model.MySmartDll;
import cn.plus.model.ddl.MySchemaTable;
import cn.smart.service.IMyLogTrans;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.smart.service.MyLogService;
import org.apache.ignite.transactions.Transaction;
import org.gridgain.dml.util.MyCacheExUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

/**
 * alter table
 * create index
 * alert index
 * drop table
 * */
public class MyDdlUtil implements Serializable {
    private static final long serialVersionUID = 3140120621889292506L;

    private static IMyLogTrans myLog = MyLogService.getInstance().getMyLog();

    private static void saveCache_0(final Ignite ignite, final PersistentArrayMap map)
    {
        IgniteTransactions transactions = ignite.transactions();
        Transaction tx = null;
        //boolean flag = false;
        try {
            tx = transactions.txStart();
            if (map.containsKey(Keyword.intern("lst_cachex"))) {
                ArrayList lst_caches = (ArrayList) map.get(Keyword.intern("lst_cachex"));
                for (int i = 0; i < lst_caches.size(); i++)
                {
                    MyCacheEx myCacheEx = (MyCacheEx) lst_caches.get(i);
                    switch (myCacheEx.getSqlType())
                    {
                        case UPDATE:
                            myCacheEx.getCache().replace(myCacheEx.getKey(), myCacheEx.getValue());
                            break;
                        case INSERT:
                            myCacheEx.getCache().put(myCacheEx.getKey(), myCacheEx.getValue());
                            break;
                        case DELETE:
                            myCacheEx.getCache().remove(myCacheEx.getKey());
                            break;
                    }
                }
            }
            tx.commit();

        } catch (Exception ex)
        {
            if (tx != null)
            {
                tx.rollback();

                if (map.containsKey(Keyword.intern("un_sql")))
                {
                    ((ArrayList<String>) map.get(Keyword.intern("un_sql"))).stream().forEach(sql -> ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll());
                }
                //flag = true;
            }
        }
        finally {
            if (tx != null) {
                tx.close();
            }
        }
    }

    public static void saveCache(final Ignite ignite, final PersistentArrayMap map)
    {
        if (map.containsKey(Keyword.intern("lst_cachex"))) {
            ArrayList lst_caches = (ArrayList) map.get(Keyword.intern("lst_cachex"));
            if (lst_caches == null)
            {
                lst_caches = new ArrayList();
            }
            MyNoSqlCache noSqlCache = (MyNoSqlCache) map.get(Keyword.intern("nosql"));

            if (noSqlCache != null)
            {
                MyCacheEx kvCache = new MyCacheEx(ignite.cache(noSqlCache.getCache_name()), noSqlCache.getKey(), noSqlCache.getValue(), noSqlCache.getSqlType(), noSqlCache);
                lst_caches.add(kvCache);
            }

            if (lst_caches != null) {

                IgniteTransactions transactions = ignite.transactions();
                Transaction tx = null;

                try {
                    tx = transactions.txStart();

                    for (int i = 0; i < lst_caches.size(); i++) {
                        MyCacheEx myCacheEx = (MyCacheEx) lst_caches.get(i);
                        switch (myCacheEx.getSqlType()) {
                            case UPDATE:
                                myCacheEx.getCache().replace(myCacheEx.getKey(), myCacheEx.getValue());
                                break;
                            case INSERT:
                                myCacheEx.getCache().put(myCacheEx.getKey(), myCacheEx.getValue());
                                break;
                            case DELETE:
                                myCacheEx.getCache().remove(myCacheEx.getKey());
                                break;
                        }
                    }

                    tx.commit();

                } catch (Exception ex) {
                    if (tx != null) {

                        tx.rollback();

                        if (map.containsKey(Keyword.intern("un_sql"))) {
                            ((ArrayList<String>) map.get(Keyword.intern("un_sql"))).stream().forEach(sql -> ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll());
                        }
                    }
                    ex.printStackTrace();
                } finally {
                    if (tx != null) {
                        tx.close();
                    }
                }
            }
        }
    }

    public static void runDdlDs(final Ignite ignite, final String code)
    {
        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);
                MySmartDll mySmartDll = new MySmartDll(code);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
    }

    public static void runDdl(final Ignite ignite, final PersistentArrayMap map, final String code)
    {
        boolean flag = false;
        IgniteCache cache = ignite.cache("public_meta");
        if (map.containsKey(Keyword.intern("sql")))
        {
            String transSession = UUID.randomUUID().toString();
            try {
                if (myLog != null)
                {
                    myLog.createSession(transSession);
                    ArrayList<String> lst = (ArrayList<String>) map.get(Keyword.intern("sql"));
                    lst.stream().forEach(sql -> {
                        cache.query(new SqlFieldsQuery(sql)).getAll();
                    });

                    MySmartDll mySmartDll = new MySmartDll(code);
                    myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));

                    myLog.commit(transSession);
                    //cache.query(new SqlFieldsQuery(map.get(Keyword.intern("sql")).toString())).getAll();

                    flag = true;
                }
                else {
                    ArrayList<String> lst = (ArrayList<String>) map.get(Keyword.intern("sql"));
                    lst.stream().forEach(sql -> cache.query(new SqlFieldsQuery(sql)).getAll());
                    //cache.query(new SqlFieldsQuery(map.get(Keyword.intern("sql")).toString())).getAll();

                    flag = true;
                }
            }
            catch (Exception e)
            {
                if (myLog != null)
                {
                    myLog.rollback(transSession);
                }
                if (map.containsKey(Keyword.intern("un_sql"))) {
                    ((ArrayList<String>) map.get(Keyword.intern("un_sql"))).stream().forEach(sql -> cache.query(new SqlFieldsQuery(sql)).getAll());
                }
                throw e;
            }

            if (flag)
            {
                saveCache(ignite, map);
            }
        }
        else
        {
            saveCache(ignite, map);
        }
    }
}




































