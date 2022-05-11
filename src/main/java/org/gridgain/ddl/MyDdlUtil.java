package org.gridgain.ddl;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import cn.plus.model.MyCacheEx;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.transactions.Transaction;

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

    private static void saveCache(final Ignite ignite, final PersistentArrayMap map)
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




































