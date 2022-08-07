package org.gridgain.superservice;

import org.apache.ignite.Ignite;

public interface INoSqlFun {

    public Object myCreate(Ignite ignite, Object group_id, Object my_obj);

    public Object myGetValue(Ignite ignite, Object group_id, Object my_obj);

    public Object myInsert(Ignite ignite, Object group_id, Object my_obj);

    public Object myUpdate(Ignite ignite, Object group_id, Object my_obj);

    public Object myDelete(Ignite ignite, Object group_id, Object my_obj);

    public Object myInsertTran(Ignite ignite, Object group_id, Object my_obj);

    public Object myUpdateTran(Ignite ignite, Object group_id, Object my_obj);

    public Object myDeleteTran(Ignite ignite, Object group_id, Object my_obj);

    public Object myDrop(Ignite ignite, Object group_id, Object my_obj);
}
