package org.gridgain.plus;

import cn.plus.model.MyKeyValue;
import cn.plus.model.MyLogCache;
import cn.plus.model.SqlType;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.gridgain.dml.util.MyCacheExUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MyDeleteCase {

    @Test
    public void my_case() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        ArrayList<MyLogCache> lst = new ArrayList<MyLogCache>();
        MyLogCache myLogCache = new MyLogCache();
        myLogCache.setCache_name("my_scenes");

        List<MyKeyValue> lst_keys = new ArrayList<>();
        lst_keys.add(new MyKeyValue("group_id", 0L));
        //lst_keys.add(new MyKeyValue("SCENES_NAME".toLowerCase(), "helloworld"));
        lst_keys.add(new MyKeyValue("SCENES_NAME", "get_data_set_id"));
        myLogCache.setKey(lst_keys);
        myLogCache.setSqlType(SqlType.DELETE);

        lst.add(myLogCache);

        MyCacheExUtil.transCache(ignite, lst);
    }
}
