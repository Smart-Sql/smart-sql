package org.gridgain.superservice;

import org.apache.ignite.Ignite;
import java.util.Hashtable;

public interface ILoadSmartSql {

    public Object loadSmartSql(final Ignite ignite, final Object group_id, final String code);

    public Object loadCsv(final Ignite ignite, final Object group_id, final Hashtable hashtable);
}
