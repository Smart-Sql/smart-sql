package org.gridgain.smart.view;

import clojure.lang.PersistentArrayMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import java.io.Serializable;

/**
 * 操作 view ast
 * */
public class MyViewAstUtil implements Serializable {
    private static final long serialVersionUID = -4359040984561404214L;

    public static PersistentArrayMap getSelectViewAst(final Ignite ignite, final MyViewAstPK myViewAstPK)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_select_view_ast");
        return cache.get(myViewAstPK);
    }

    public static PersistentArrayMap getSelectViewAst(final Ignite ignite, final String schema_name, final String table_name, final Long my_group_id)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_select_view_ast");
        return cache.get(new MyViewAstPK(schema_name, table_name, my_group_id));
    }

    public static PersistentArrayMap getInsertViewAst(final Ignite ignite, final MyViewAstPK myViewAstPK)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_insert_view_ast");
        return cache.get(myViewAstPK);
    }

    public static PersistentArrayMap getInsertViewAst(final Ignite ignite, final String schema_name, final String table_name, final Long my_group_id)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_insert_view_ast");
        return cache.get(new MyViewAstPK(schema_name, table_name, my_group_id));
    }

    public static PersistentArrayMap getUpdateViewAst(final Ignite ignite, final MyViewAstPK myViewAstPK)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_update_view_ast");
        return cache.get(myViewAstPK);
    }

    public static PersistentArrayMap getUpdateViewAst(final Ignite ignite, final String schema_name, final String table_name, final Long my_group_id)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_update_view_ast");
        return cache.get(new MyViewAstPK(schema_name, table_name, my_group_id));
    }

    public static PersistentArrayMap getDeleteViewAst(final Ignite ignite, final MyViewAstPK myViewAstPK)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_delete_view_ast");
        return cache.get(myViewAstPK);
    }

    public static PersistentArrayMap getDeleteViewAst(final Ignite ignite, final String schema_name, final String table_name, final Long my_group_id)
    {
        IgniteCache<MyViewAstPK, PersistentArrayMap> cache = ignite.cache("my_delete_view_ast");
        return cache.get(new MyViewAstPK(schema_name, table_name, my_group_id));
    }

}





















































