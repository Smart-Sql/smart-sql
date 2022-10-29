package org.gridgain.ddl;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import cn.plus.model.MyIndexAstPk;
import cn.plus.model.MySmartDll;
import cn.plus.model.ddl.MySchemaTable;
import cn.smart.service.IMyLogTrans;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.smart.service.MyLogService;
import org.gridgain.dml.util.MyCacheExUtil;

import java.util.UUID;

public class MyDdlUtilEx {
    private static IMyLogTrans myLog = MyLogService.getInstance().getMyLog();

    public static void saveCache(final Ignite ignite, final PersistentArrayMap map)
    {
        String sql = (String) map.get(Keyword.intern("sql"));
        PersistentArrayMap pk_data = (PersistentArrayMap) map.get(Keyword.intern("pk-data"));

        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);

                ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
                ignite.cache("table_ast").put(new MySchemaTable((String) map.get(Keyword.intern("schema_name")), (String) map.get(Keyword.intern("table_name"))), pk_data);

                MySmartDll mySmartDll = new MySmartDll(sql);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
            ignite.cache("table_ast").put(new MySchemaTable((String) map.get(Keyword.intern("schema_name")), (String) map.get(Keyword.intern("table_name"))), pk_data);
        }
    }

    public static void updateCache(final Ignite ignite, final PersistentArrayMap map)
    {
        String sql = (String) map.get(Keyword.intern("sql"));
        PersistentArrayMap pk_data = (PersistentArrayMap) map.get(Keyword.intern("pk-data"));

        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);

                ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
                ignite.cache("table_ast").replace(new MySchemaTable((String) map.get(Keyword.intern("schema_name")), (String) map.get(Keyword.intern("table_name"))), pk_data);

                MySmartDll mySmartDll = new MySmartDll(sql);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
            ignite.cache("table_ast").replace(new MySchemaTable((String) map.get(Keyword.intern("schema_name")), (String) map.get(Keyword.intern("table_name"))), pk_data);
        }
    }

    public static void deleteCache(final Ignite ignite, final PersistentArrayMap map)
    {
        String sql = (String) map.get(Keyword.intern("sql"));

        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);

                ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
                ignite.cache("table_ast").remove(new MySchemaTable((String) map.get(Keyword.intern("schema_name")), (String) map.get(Keyword.intern("table_name"))));

                MySmartDll mySmartDll = new MySmartDll(sql);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
            ignite.cache("table_ast").remove(new MySchemaTable((String) map.get(Keyword.intern("schema_name")), (String) map.get(Keyword.intern("table_name"))));
        }
    }

    public static void saveIndexCache(final Ignite ignite, final PersistentArrayMap map)
    {
        String sql = (String) map.get(Keyword.intern("sql"));
        PersistentArrayMap index_data = (PersistentArrayMap) map.get(Keyword.intern("index"));

        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);

                ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
                ignite.cache("index_ast").put(new MyIndexAstPk(index_data.get(Keyword.intern("schema_index")).toString(), index_data.get(Keyword.intern("index_name")).toString()), index_data.get(Keyword.intern("index_ast")));

                MySmartDll mySmartDll = new MySmartDll(sql);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
            ignite.cache("index_ast").put(new MyIndexAstPk(index_data.get(Keyword.intern("schema_index")).toString(), index_data.get(Keyword.intern("index_name")).toString()), index_data.get(Keyword.intern("index_ast")));
        }
    }

    public static void updateIndexCache(final Ignite ignite, final PersistentArrayMap map)
    {
        String sql = (String) map.get(Keyword.intern("sql"));

        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);

                ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();

                MySmartDll mySmartDll = new MySmartDll(sql);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
        }
    }

    public static void deleteIndexCache(final Ignite ignite, final PersistentArrayMap map)
    {
        String sql = (String) map.get(Keyword.intern("sql"));
        PersistentArrayMap index_data = (PersistentArrayMap) map.get(Keyword.intern("index"));

        if (myLog != null)
        {
            String transSession = UUID.randomUUID().toString();
            try {
                myLog.createSession(transSession);

                ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
                ignite.cache("index_ast").remove(new MyIndexAstPk(index_data.get(Keyword.intern("schema_index")).toString(), index_data.get(Keyword.intern("index_name")).toString()));

                MySmartDll mySmartDll = new MySmartDll(sql);
                myLog.saveTo(transSession, MyCacheExUtil.objToBytes(mySmartDll));
                myLog.commit(transSession);
            }
            catch (Exception e)
            {
                myLog.rollback(transSession);
            }
        }
        else
        {
            ignite.cache("public_meta").query(new SqlFieldsQuery(sql)).getAll();
            ignite.cache("index_ast").remove(new MyIndexAstPk(index_data.get(Keyword.intern("schema_index")).toString(), index_data.get(Keyword.intern("index_name")).toString()));
        }
    }
}
