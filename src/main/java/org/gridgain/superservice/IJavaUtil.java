package org.gridgain.superservice;

import java.util.ArrayList;
import java.util.Hashtable;

public interface IJavaUtil {

    public Object toArrayOrHashtable(final Object m);

    public Boolean isSeq(final Object m);

    public Boolean isDic(final Object m);

    public ArrayList myToArrayList(final Object m);

}
