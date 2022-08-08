package org.gridgain.ddl;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import cn.plus.model.MyCacheEx;
import cn.plus.model.MyNoSqlCache;
import cn.plus.model.MySmartCache;
import cn.plus.model.MySmartDll;
import cn.plus.model.ddl.MySchemaTable;
import cn.smart.service.IMyLogTransaction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.smart.service.MyLogService;
import org.apache.ignite.transactions.Transaction;
import org.gridgain.dml.util.MyCacheExUtil;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * alter table
 * create index
 * alert index
 * drop table
 * */
public class MyDdlUtil implements Serializable {
    private static final long serialVersionUID = 3140120621889292506L;

    private static IMyLogTransaction myLog = MyLogService.getInstance().getMyLog();

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

                if (myLog != null)
                {
                    IgniteTransactions transactions = ignite.transactions();
                    Transaction tx = null;

                    try {
                        tx = transactions.txStart();
                        myLog.begin();

                        ArrayList<String> lst = (ArrayList<String>) map.get(Keyword.intern("sql"));
                        lst.stream().forEach(sql -> {

                            MySmartDll mySmartDll = new MySmartDll(sql);
                            if(myLog.saveTo(MyCacheExUtil.objToBytes(mySmartDll)) == false)
                            {
                                try {
                                    throw new Exception("log 保存失败！");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });

                        for (int i = 0; i < lst_caches.size(); i++) {
                            MyCacheEx myCacheEx = (MyCacheEx) lst_caches.get(i);
                            switch (myCacheEx.getSqlType()) {
                                case UPDATE:
                                    myCacheEx.getCache().replace(myCacheEx.getKey(), myCacheEx.getValue());
                                    if(myLog.saveTo(MyCacheExUtil.objToBytes(myCacheEx.getData())) == false)
                                    {
                                        throw new Exception("log 保存失败！");
                                    }
                                    break;
                                case INSERT:
                                    myCacheEx.getCache().put(myCacheEx.getKey(), myCacheEx.getValue());
                                    if(myLog.saveTo(MyCacheExUtil.objToBytes(myCacheEx.getData())) == false)
                                    {
                                        throw new Exception("log 保存失败！");
                                    }
                                    break;
                                case DELETE:
                                    myCacheEx.getCache().remove(myCacheEx.getKey());
                                    if(myLog.saveTo(MyCacheExUtil.objToBytes(myCacheEx.getData())) == false)
                                    {
                                        throw new Exception("log 保存失败！");
                                    }
                                    break;
                            }
                        }

                        myLog.commit();
                        tx.commit();

                    } catch (Exception ex) {
                        if (tx != null) {

                            myLog.rollback();
                            tx.rollback();

                            if (map.containsKey(Keyword.intern("un_sql"))) {
                                ((ArrayList<String>) map.get(Keyword.intern("un_sql"))).stream().forEach(sql -> ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll());
                            }
                        }
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
                    } finally {
                        if (tx != null) {
                            tx.close();
                        }
                    }
                }
            }
        }
    }

    public static void runDdl(final Ignite ignite, final PersistentArrayMap map)
    {
        boolean flag = false;
        IgniteCache cache = ignite.cache("public_meta");
        if (map.containsKey(Keyword.intern("sql")))
        {
            try {
                ArrayList<String> lst = (ArrayList<String>) map.get(Keyword.intern("sql"));
                lst.stream().forEach(sql -> cache.query(new SqlFieldsQuery(sql)).getAll());
                //cache.query(new SqlFieldsQuery(map.get(Keyword.intern("sql")).toString())).getAll();
                flag = true;
            }
            catch (Exception e)
            {
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




































