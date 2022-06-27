package org.tools;

import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;
import cn.plus.model.ddl.MySchemaTable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
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
        //cfg.setDataRegionName("40MB_Region_Eviction");
        cfg.setName("table_ast");
        cfg.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg);

        // insert 语句中要找到表的定义 "table_ast"
        CacheConfiguration<MyViewAstPK, PersistentArrayMap> cfg_my_select_view_ast = new CacheConfiguration<>();
        cfg_my_select_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_select_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_select_view_ast.setDataRegionName("40MB_Region_Eviction");
        cfg_my_select_view_ast.setName("my_select_view_ast");
        cfg_my_select_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_select_view_ast);

        CacheConfiguration<MySchemaTable, LazySeq> cfg_my_insert_view_ast = new CacheConfiguration<>();
        cfg_my_insert_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_insert_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_insert_view_ast.setDataRegionName("40MB_Region_Eviction");
        cfg_my_insert_view_ast.setName("my_insert_view_ast");
        cfg_my_insert_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_insert_view_ast);

        CacheConfiguration<MySchemaTable, LazySeq> cfg_my_update_view_ast = new CacheConfiguration<>();
        cfg_my_update_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_update_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_update_view_ast.setDataRegionName("40MB_Region_Eviction");
        cfg_my_update_view_ast.setName("my_insert_view_ast");
        cfg_my_update_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_update_view_ast);

        CacheConfiguration<MySchemaTable, LazySeq> cfg_my_delete_view_ast = new CacheConfiguration<>();
        cfg_my_delete_view_ast.setCacheMode(CacheMode.REPLICATED);
        cfg_my_delete_view_ast.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cfg_my_delete_view_ast.setDataRegionName("40MB_Region_Eviction");
        cfg_my_delete_view_ast.setName("my_insert_view_ast");
        cfg_my_delete_view_ast.setReadFromBackup(true);
        ignite.getOrCreateCache(cfg_my_delete_view_ast);
    }
}
