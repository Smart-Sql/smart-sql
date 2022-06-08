package org.gridgain.superservice;

import org.apache.ignite.Ignite;

public interface INoSqlFun {

    public Object myInsert(Ignite ignite, Long group_id, Object my_obj);

    public Object myUpdate(Ignite ignite, Long group_id, Object my_obj);

    public Object myDelete(Ignite ignite, Long group_id, Object my_obj);

    public Object myInsertTran(Ignite ignite, Long group_id, Object my_obj);

    public Object myUpdateTran(Ignite ignite, Long group_id, Object my_obj);

    public Object myDeleteTran(Ignite ignite, Long group_id, Object my_obj);

    public Object myDrop(Ignite ignite, Long group_id, Object my_obj);
}
