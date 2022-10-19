package org.tools;

import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;
import cn.plus.model.MyIndexAstPk;
import cn.plus.model.ddl.MySchemaTable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.gridgain.myservice.MySqlToAst;
import org.gridgain.smart.view.MyViewAstPK;

public class MyInitCache {

    /**
     * 初始化 Cache
     * */
    public void InitCache(final Ignite ignite)
    {
        // insert 语句中要找到表的定义 "table_ast"
        CacheConfiguration<MySchemaTable, PersistentArrayMap> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg.setDataRegionName("Near_Caches_Region_Eviction");
        cfg.setName("table_ast");
        cfg.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg);

        CacheConfiguration<MyIndexAstPk, PersistentArrayMap> cfg_kv = new CacheConfiguration<>();
        cfg_kv.setCacheMode(CacheMode.REPLICATED);
        cfg_kv.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_kv.setDataRegionName("Near_Caches_Region_Eviction");
        cfg_kv.setName("index_ast");
        cfg_kv.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_kv);

        // insert 语句中要找到表的定义 "table_ast"
        CacheConfiguration<MyViewAstPK, PersistentArrayMap> cfg_sys_view_ast = new CacheConfiguration<>();
        cfg_sys_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_sys_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_sys_view_ast.setDataRegionName("Near_Caches_Region_Eviction");
        cfg_sys_view_ast.setName("my_sys_view_ast");
        cfg_sys_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_sys_view_ast);

        // insert 语句中要找到表的定义 "table_ast"
        CacheConfiguration<MyViewAstPK, PersistentArrayMap> cfg_my_select_view_ast = new CacheConfiguration<>();
        cfg_my_select_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_select_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_select_view_ast.setDataRegionName("Near_Caches_Region_Eviction");
        cfg_my_select_view_ast.setName("my_select_view_ast");
        cfg_my_select_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_select_view_ast);

        CacheConfiguration<MySchemaTable, LazySeq> cfg_my_insert_view_ast = new CacheConfiguration<>();
        cfg_my_insert_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_insert_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_insert_view_ast.setDataRegionName("Near_Caches_Region_Eviction");
        cfg_my_insert_view_ast.setName("my_insert_view_ast");
        cfg_my_insert_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_insert_view_ast);

        CacheConfiguration<MySchemaTable, LazySeq> cfg_my_update_view_ast = new CacheConfiguration<>();
        cfg_my_update_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_update_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_update_view_ast.setDataRegionName("Near_Caches_Region_Eviction");
        cfg_my_update_view_ast.setName("my_update_view_ast");
        cfg_my_update_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_update_view_ast);

        CacheConfiguration<MySchemaTable, LazySeq> cfg_my_delete_view_ast = new CacheConfiguration<>();
        cfg_my_delete_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_delete_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_delete_view_ast.setDataRegionName("Near_Caches_Region_Eviction");
        cfg_my_delete_view_ast.setName("my_delete_view_ast");
        cfg_my_delete_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_delete_view_ast);

        MyViewAstPK baseline_nodes = new MyViewAstPK("sys", "BASELINE_NODES");
        if (!ignite.cache("my_sys_view_ast").containsKey(baseline_nodes))
        {
            ignite.cache("my_sys_view_ast").put(baseline_nodes, MySqlToAst.getInstance().getSqlToAst().mySqlToAst("SELECT * FROM sys.baseline_nodes WHERE false"));
        }
    }
}
