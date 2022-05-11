package org.gridgain.superservice;

import org.apache.ignite.Ignite;

import java.util.ArrayList;

public interface IMyScenes {
    public Object myInvoke(Ignite ignite, String methodName, ArrayList lst);
}
