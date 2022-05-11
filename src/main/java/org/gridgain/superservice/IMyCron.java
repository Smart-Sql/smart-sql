package org.gridgain.superservice;

import org.apache.ignite.Ignite;

/**
 * 定时任务
 * */
public interface IMyCron {

    public Object addJob(Ignite ignite, Long group_id, clojure.lang.PersistentArrayMap ast);

    public Object removeJob(Ignite ignite, Long group_id, String name);

}
