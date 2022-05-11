package org.tools;

import clojure.lang.PersistentVector;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.List;

public class MyDbUtil {

    public static Object[] getArrayObject(final PersistentVector args)
    {
        return args.toArray();
    }

    public static List<List<?>> runMetaSql(final Ignite ignite, final String sql, final PersistentVector args)
    {
        final IgniteCache<?, ?> cache = ignite.cache("my_meta_table");
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
        FieldsQueryCursor<List<?>> cursor = cache.query(sqlFieldsQuery.setArgs(args.toArray()));
        return cursor.getAll();
    }

    public static List<List<?>> runSql(final Ignite ignite, final String sql, final PersistentVector args)
    {
        final IgniteCache<?, ?> cache = ignite.cache("public_meta");
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
        FieldsQueryCursor<List<?>> cursor = cache.query(sqlFieldsQuery.setArgs(args.toArray()));
        return cursor.getAll();
    }

    public static CacheConfiguration getPublicCfg()
    {
        return new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
    }

    public static CacheConfiguration getMetaCfg()
    {
        return new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
    }

    public static Long getGroupIdByCall(final Ignite ignite, final Long to_group_id, final String scenes_name)
    {
        List<List<?>> lst = ignite.getOrCreateCache(getMetaCfg()).query(new SqlFieldsQuery("select m.group_id from call_scenes m where m.to_group_id = ? and m.scenes_name = ?").setArgs(to_group_id, scenes_name)).getAll();
        if (lst != null && !lst.isEmpty())
        {
            return (Long) lst.get(0).get(0);
        }
        return null;
    }
}
