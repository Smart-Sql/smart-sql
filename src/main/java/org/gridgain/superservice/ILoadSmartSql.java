package org.gridgain.superservice;

import org.apache.ignite.Ignite;

public interface ILoadSmartSql {

    public Object loadSmartSql(final Ignite ignite, final Long group_id, final String code);
}
