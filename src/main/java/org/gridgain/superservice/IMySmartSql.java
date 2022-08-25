package org.gridgain.superservice;

import org.apache.ignite.Ignite;

public interface IMySmartSql {

    public Object recovery_ddl(Ignite ignite, String line);
}
