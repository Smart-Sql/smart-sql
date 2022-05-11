package org.tools;

import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

import java.io.Serializable;

public class MySqlFunc implements Serializable {
    private static final long serialVersionUID = 2294760867715026212L;

    @QuerySqlFunction
    public static Object myInvoke(final Ignite ignite, final String methodName, final Object... ps)
    {
        //Ignite ignite = Ignition.ignite();
        RT.init();
        Var myscenes_obj = (Var)ignite.cache("myscenes").get(methodName);
        return myscenes_obj.invoke(ps);
    }
}
