package org.gridgain.ddl;

import clojure.lang.*;
import cn.plus.model.MyCacheEx;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import java.io.Serializable;
import java.util.ArrayList;

public class MyCreateTableUtil implements Serializable {
    private static final long serialVersionUID = 1484107803014288212L;

    public static CacheConfiguration getCfg()
    {
        return new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
    }

    public static void run_ddl_dml(final Ignite ignite, final ArrayList lst_ddl, final ArrayList lst_dml_table)
    {
        boolean flag = false;
        IgniteCache cache = ignite.getOrCreateCache(getCfg());
        try {
            lst_ddl.stream().forEach(map ->
            {
                PersistentArrayMap pmap = ((PersistentArrayMap) map);
                String sql = pmap.get(Keyword.intern("sql")).toString();
                SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                cache.query(sqlFieldsQuery).getAll();
            });
            //cache.query(new SqlFieldsQuery(map.get(Keyword.intern("sql")).toString())).getAll();
            flag = true;
        }
        catch (Exception e)
        {
            lst_ddl.stream().forEach(map -> cache.query(new SqlFieldsQuery(((PersistentArrayMap) map).get(Keyword.intern("un_sql")).toString())).getAll());
            throw e;
        }

        if (flag)
        {
            saveCache(ignite, lst_ddl, lst_dml_table);
        }
    }

    private static void saveCache(final Ignite ignite, final ArrayList lst_ddl, final ArrayList lst_dml_table)
    {
        IgniteTransactions transactions = ignite.transactions();
        Transaction tx = null;
        try {
            tx = transactions.txStart();
            for (int i = 0; i < lst_dml_table.size(); i++)
            {
                MyCacheEx myCacheEx = (MyCacheEx) lst_dml_table.get(i);
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
            tx.commit();
        } catch (Exception ex)
        {
            if (tx != null)
            {
                tx.rollback();

                lst_ddl.stream().forEach(map -> ignite.getOrCreateCache(new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META")).query(new SqlFieldsQuery(((PersistentArrayMap) map).get(Keyword.intern("un_sql")).toString())).getAll());
            }
        }
        finally {
            if (tx != null) {
                tx.close();
            }
        }
    }

//    private static void runError(final IgniteCache cache, final List<PersistentArrayMap> lstError)
//    {
//        List<PersistentArrayMap> lst = new ArrayList<>();
//        for (PersistentArrayMap map : lstError)
//        {
//            try {
//                cache.query(new SqlFieldsQuery(map.get(Keyword.intern("un_sql")).toString())).getAll();
//            }
//            catch (Exception e)
//            {
//                lst.add(map);
//                break;
//            }
//        }
//
//        if (lst.size() > 0)
//        {
//            runError(cache, lst);
//        }
//    }
//
//    private static void saveTables(final Ignite ignite, final ArrayList lst_dml_table)
//    {
//        IgniteTransactions transactions = ignite.transactions();
//        Transaction tx = null;
//        boolean flag = false;
//        try {
//            tx = transactions.txStart();
//            for (Object m : lst_dml_table)
//            {
//                PersistentArrayMap map = (PersistentArrayMap) m;
//                ignite.cache(map.get(Keyword.intern("table")).toString()).put(map.get(Keyword.intern("key")), map.get(Keyword.intern("value")));
//            }
//
//            tx.commit();
//        } catch (Exception ex)
//        {
//            if (tx != null)
//            {
//                tx.rollback();
//                flag = true;
//            }
//            System.out.println(ex.getMessage());
//        }
//        finally {
//            if (tx != null) {
//                tx.close();
//            }
//        }
//
//        if (flag == true)
//        {
//            saveTables(ignite, lst_dml_table);
//        }
//
//    }
//
//    /**
//     * 执行 ddl
//     * */
//    public static void run_ddl_dml(final Ignite ignite, final ArrayList lst_ddl, final ArrayList lst_dml_table)
//    {
//        if (lst_ddl != null && lst_dml_table != null) {
//            ArrayList<PersistentArrayMap> lst = new ArrayList<PersistentArrayMap>();
//            IgniteCache cache = ignite.cache("public_meta");
//            for (Object m : lst_ddl)
//            {
//                PersistentArrayMap map = (PersistentArrayMap) m;
//                try {
//                    cache.query(new SqlFieldsQuery(map.get(Keyword.intern("sql")).toString())).getAll();
//                    lst.add((PersistentArrayMap) map.assoc(Keyword.intern("is_success"), true));
//                } catch (Exception e) {
//                    lst.add(map);
//                    System.out.println(e.getMessage());
//                    break;
//                }
//            }
//
//            List<PersistentArrayMap> lstError = lst.stream().filter(map -> map.get(Keyword.intern("is_success")).equals(false)).collect(Collectors.toList());
//            if (lstError.size() > 0) {
//                runError(cache, lstError);
//            } else {
//                saveTables(ignite, lst_dml_table);
//            }
//        }
//    }
}



























