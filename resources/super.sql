/**
记录各个节点 rpc 的信息
应该剔除 editport 这个接口
*/
CREATE TABLE IF NOT EXISTS rpc_info (
    PRIMARY KEY (node_name),
    node_name varchar,
    ip varchar,
    clsport int,
    serviceport int
) WITH "template=MyMeta_template,cache_name=rpc_info,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
insert into rpc_info (node_name, ip, clsport, serviceport) values ('node_1', '192.168.93.128', 9091, 9092);
*/

/**

方法和场景的区别：
方法是程序员写的
场景是业务描述的

method_name 是唯一的
cls_name: namespace.class_name
java_method_name: java 的方法
*/
DROP TABLE IF EXISTS my_func;
CREATE TABLE IF NOT EXISTS my_func (
                method_name VARCHAR(30),
                java_method_name VARCHAR(30),
                cls_name VARCHAR,
                return_type VARCHAR(20),
                ps_code VARCHAR,
                descrip VARCHAR,
                PRIMARY KEY (method_name)
) WITH "template=MyMeta_template,VALUE_TYPE=cn.plus.model.ddl.MyFunc,cache_name=my_func,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP INDEX IF EXISTS my_func_method_name_idx;
CREATE INDEX IF NOT EXISTS my_func_method_name_idx ON my_func (method_name);

/**
存储元表，因为元表一般是从 oracle 中导出来的，所以就命名为 my_meta_tables
*/
DROP TABLE IF EXISTS my_meta_tables;
CREATE TABLE IF NOT EXISTS my_meta_tables (
                id BIGINT,
                table_name VARCHAR(50),
                descrip VARCHAR,
                --让创建 table 的语句不可见
                --code VARCHAR,
                data_set_id BIGINT,
                PRIMARY KEY (id)
) WITH "template=MyMeta_template,cache_name=my_meta_tables,VALUE_TYPE=cn.plus.model.ddl.MyTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";
/*
  data_set_id 和 table_name 唯一确定一个值
  添加 data_set_id 的作用是在该数据集的用户组可以查看该数据组的表集合
*/
CREATE INDEX IF NOT EXISTS ot_ds_tname_idx ON my_meta_tables (table_name, data_set_id);

DROP TABLE IF EXISTS table_item;
CREATE TABLE IF NOT EXISTS table_item (
                id BIGINT,
                column_name VARCHAR(50),
                column_len INT,
                scale INT,
                column_type VARCHAR(50),
                not_null BOOLEAN DEFAULT true,
                pkid BOOLEAN DEFAULT false,
                comment VARCHAR(50),
                auto_increment BOOLEAN DEFAULT false,
                table_id BIGINT,
                PRIMARY KEY (id, table_id)
) WITH "template=MyMeta_template,cache_name=table_item,affinityKey=table_id,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableItem,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";


DROP TABLE IF EXISTS table_index;
CREATE TABLE IF NOT EXISTS table_index (
                id BIGINT,
                index_name VARCHAR(50),
                spatial BOOLEAN DEFAULT false,
                table_id BIGINT,
                --ex_table_id_ BIGINT,
                PRIMARY KEY (id, table_id)
) WITH "template=MyMeta_template,cache_name=table_index,affinityKey=table_id,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableIndex,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP TABLE IF EXISTS table_index_item;
CREATE TABLE IF NOT EXISTS table_index_item (
                id BIGINT,
                index_item VARCHAR(50),
                sort_order VARCHAR(4),
                index_no BIGINT,
                -- table_id BIGINT,
                PRIMARY KEY (id, index_no)
) WITH "template=MyMeta_template,cache_name=table_index_item,affinityKey=index_no,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableIndexItem,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";


/**
 记录 scenes 使用表和表字段的情况，
 当一个 scenes 一旦创建成功后，就要拜使用的信息都填充到 my_scenes_table 表中。
 如果删除表和表的某个行时，如果有 scenes 使用，那么先删除 scenes 在修改 ddl

 table_id：table id
 table_item_id：table item 的 id

 1、在删除 table 的时候，通过 scenes_id 查询 table_id 是否使用了
 2、在删除 column 的时候，通过 table_item_id 查询 column 是否使用了
*/
DROP TABLE IF EXISTS my_scenes_table;
CREATE TABLE IF NOT EXISTS my_scenes_table (
                  id BIGINT,
                  table_id BIGINT,
                  table_item_id BIGINT,
                  scenes_id BIGINT,
                  PRIMARY KEY (id, scenes_id)
                ) WITH "template=MyMeta_template,cache_name=my_scenes_table,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
  记录 scenes 使用的所有索引，
  1、在删除 index 的时候，查询 index_id 是否在，场景中有使用
*/
DROP TABLE IF EXISTS my_scenes_index;
CREATE TABLE IF NOT EXISTS my_scenes_index (
                  id BIGINT,
                  index_id BIGINT,
                  scenes_id BIGINT,
                  PRIMARY KEY (id, scenes_id)
                ) WITH "template=MyMeta_template,cache_name=my_scenes_index,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";


DROP INDEX IF EXISTS my_meta_tables_idx;
DROP INDEX IF EXISTS table_item_idx;
DROP INDEX IF EXISTS table_index_idx;

CREATE INDEX IF NOT EXISTS my_meta_tables_idx ON my_meta_tables (table_name);
CREATE INDEX IF NOT EXISTS table_item_idx ON table_item (table_id, column_name);
CREATE INDEX IF NOT EXISTS table_index_idx ON table_index (table_id, index_name);

/**
剔除原版本，状态，是否激活
让 scenes_name 成为 key 值，同时新建一个 my_scenes_log 来记录整个 my_scenes 的增加，
这样就可以结合 my_log 将任意时间节点的数据和场景恢复出来，并且能够运行。

输入参数 ps_code: [{'ps_index': 0, 'ps_type': 'String'}, {'ps_index': 2, 'ps_type': 'String'}]

*/
DROP TABLE IF EXISTS my_scenes;
CREATE TABLE IF NOT EXISTS my_scenes (
                  schema_name VARCHAR,
                  --group_id BIGINT,
                  scenes_name VARCHAR(40),
                  sql_code VARCHAR,
                  descrip VARCHAR,
                  params VARCHAR,
                  PRIMARY KEY (scenes_name, schema_name)
                ) WITH "template=MyMeta_template,KEY_TYPE=cn.plus.model.db.MyScenesCachePk,VALUE_TYPE=cn.plus.model.db.MyScenesCache,cache_name=my_scenes,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
  记录场景操作的表
*/
DROP TABLE IF EXISTS my_scenes_log;
CREATE TABLE IF NOT EXISTS my_scenes_log (
                  id BIGINT,
                  mycacheex VARBINARY,
                  create_date TIMESTAMP,
                  PRIMARY KEY (id)
                ) WITH "template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.my_scenes_log,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=my_log,cache_group=my_data";

DROP INDEX IF EXISTS my_scenes_log_create_date_idx;
CREATE INDEX IF NOT EXISTS my_scenes_log_create_date_idx ON my_scenes_log (create_date DESC);


/**
将本用户组场景的使用权限，赋给其它用户组
将 scenes_id 场景的使用权，提交给 to_group_id。因为并非是本用户组下面的，所以要使用函数
实际上是把 to_group_id，替换为 group_id。这样使用方的用户组 id 为 to_group_id，替换为
group_id ，那么 to_group_id 就有了使用 group_id 的权限了
*/
DROP TABLE IF EXISTS call_scenes;
CREATE TABLE IF NOT EXISTS call_scenes (
                  id BIGINT,
                  group_id BIGINT,
                  to_group_id BIGINT,
                  scenes_name VARCHAR(40),
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,VALUE_TYPE=cn.plus.model.db.MyCallScenes,cache_name=call_scenes,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";
CREATE INDEX IF NOT EXISTS call_scenes_idx ON call_scenes (to_group_id, scenes_name);


/**
记录场景资源

my_scenes_table 中用到的表
my_scenes_ss 中用到的场景
my_scenes_func 中用到的场景

这个的作用是当 view 被修改，删除的时候，先查询 scenes 使用的资源，同步更新场景所用到的是 view。
因为修改权限视图，不会那么频繁，所以直接把权限视图编译进场景是较优选择！
*/
DROP TABLE IF EXISTS my_scenes_table;
CREATE TABLE IF NOT EXISTS my_scenes_table (
                  id BIGINT,
                  scenes_id BIGINT,
                  table_id BIGINT,
                  data_set_id BIGINT,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_scenes_table,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP TABLE IF EXISTS my_scenes_ss;
CREATE TABLE IF NOT EXISTS my_scenes_ss (
                  id BIGINT,
                  scenes_id BIGINT,
                  ss_id BIGINT,
                  data_set_id BIGINT,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_scenes_ss,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP TABLE IF EXISTS my_scenes_func;
CREATE TABLE IF NOT EXISTS my_scenes_func (
                  id BIGINT,
                  scenes_id BIGINT,
                  func_id BIGINT,
                  data_set_id BIGINT,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_scenes_func,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/*
视图：

func_name: 表示查询出来后在处理，例如：脱敏
目前只有 select 操作可以对具体的列添加方法

*/

DROP TABLE IF EXISTS my_select_views;
CREATE TABLE IF NOT EXISTS my_select_views (
                  id BIGINT,
                  view_name VARCHAR(40),
                  table_name VARCHAR(40),
                  data_set_id BIGINT DEFAULT 0,
                  code VARCHAR,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_select_views,VALUE_TYPE=cn.plus.model.ddl.MySelectViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP TABLE IF EXISTS my_update_views;
CREATE TABLE IF NOT EXISTS my_update_views (
                  id BIGINT,
                  view_name VARCHAR(40),
                  table_name VARCHAR(40),
                  data_set_id BIGINT DEFAULT 0,
                  code VARCHAR,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_update_views,VALUE_TYPE=cn.plus.model.ddl.MyUpdateViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP TABLE IF EXISTS my_insert_views;
CREATE TABLE IF NOT EXISTS my_insert_views (
                  id BIGINT,
                  view_name VARCHAR(40),
                  table_name VARCHAR(40),
                  data_set_id BIGINT DEFAULT 0,
                  code VARCHAR,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_insert_views,VALUE_TYPE=cn.plus.model.ddl.MyInsertViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

DROP TABLE IF EXISTS my_delete_views;
CREATE TABLE IF NOT EXISTS my_delete_views (
                id BIGINT,
                view_name VARCHAR(40),
                table_name VARCHAR(40),
                data_set_id BIGINT DEFAULT 0,
                code VARCHAR,
                PRIMARY KEY (id)
              ) WITH "template=MyMeta_template,cache_name=my_delete_views,VALUE_TYPE=cn.plus.model.ddl.MyDeleteViews,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
  数据集
*/
DROP TABLE IF EXISTS my_dataset;
CREATE TABLE IF NOT EXISTS my_dataset (
                  id BIGINT,
                  dataset_name VARCHAR,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_dataset,VALUE_TYPE=cn.plus.model.ddl.MyDataSet,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";
CREATE INDEX IF NOT EXISTS my_dataset_idx ON my_dataset (dataset_name);

/**
  记录操作的表
*/
DROP TABLE IF EXISTS my_log;
CREATE TABLE IF NOT EXISTS my_log (
                  id VARCHAR,
                  table_name VARCHAR,
                  mycacheex VARBINARY,
                  create_date TIMESTAMP,
                  PRIMARY KEY (id)
                ) WITH "template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.MyLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=my_log,cache_group=my_data";

DROP INDEX IF EXISTS my_log_idx;
CREATE INDEX IF NOT EXISTS my_log_idx ON my_log (table_name, create_date);

/**
  记录DDL操作的表，作用是回溯复盘
  数据集中 ddl 的 log 即 table 的 ddl
  更新数据集时，先执行 更新数据集、在更新表、在更新表中的数据

  取消掉整个 ddl_log 合并到 my_log 里面去 table_name：ddl_log, mycacheex: 为处理过后的 sql_code
*/
--DROP TABLE IF EXISTS ddl_log;
--CREATE TABLE IF NOT EXISTS ddl_log (
--                  id BIGINT,
--                  group_id BIGINT,
--                  data_set_id BIGINT,
--                  sql_code VARCHAR,
--                  create_date TIMESTAMP,
--                  PRIMARY KEY (id)
--                ) WITH "template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.DdlLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=ddl_log,cache_group=my_data";
--
--DROP INDEX IF EXISTS ddl_log_group_id_idx;
--CREATE INDEX IF NOT EXISTS ddl_log_group_id_idx ON ddl_log (group_id, create_date);
--CREATE INDEX IF NOT EXISTS ddl_log_ds_id_idx ON ddl_log (data_set_id, create_date);
--CREATE INDEX IF NOT EXISTS ddl_log_gp_ds_id_idx ON ddl_log (group_id, data_set_id, create_date);

/**
  数据集的 ddl log 即 data set 的 ddl
  更新数据集时，先执行 更新数据集、在更新表、在更新表中的数据
*/
--DROP TABLE IF EXISTS dataset_ddl_log;
--CREATE TABLE IF NOT EXISTS dataset_ddl_log (
--                  id BIGINT,
--                  data_set_name VARCHAR,
--                  ds_ddl_type VARCHAR,
--                  sql_code VARCHAR,
--                  create_date TIMESTAMP,
--                  PRIMARY KEY (id)
--                ) WITH "template=partitioned,backups=3,VALUE_TYPE=cn.plus.model.DataSetDdlLog,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=dataset_ddl_log,cache_group=my_data";
--
--DROP INDEX IF EXISTS ds_ddl_log_idx;
--CREATE INDEX IF NOT EXISTS ds_ddl_log_idx ON dataset_ddl_log (data_set_name, create_date);

/**
用户表 my_user
这个用户表是基础用户表，可以扩展
*/
DROP TABLE IF EXISTS my_user;
CREATE TABLE IF NOT EXISTS my_user (
                  id BIGINT,
                  user_name VARCHAR(40),
                  pass_word VARCHAR(40),
                  group_id BIGINT,
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_user,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
 用户组中添加 数据集 id
 group_type: DDL, DML, ALL DDL和DML
*/
DROP TABLE IF EXISTS my_users_group;
CREATE TABLE IF NOT EXISTS my_users_group (
                  id BIGINT,
                  -- 用户组名称
                  group_name VARCHAR(40),
                  -- 数据集
                  data_set_id BIGINT DEFAULT 0,
                  -- userToken
                  user_token VARCHAR,
                  -- 用户组类型
                  group_type VARCHAR(8),
                  PRIMARY KEY (id)
                ) WITH "template=MyMeta_template,cache_name=my_users_group,VALUE_TYPE=cn.plus.model.MyUsersGroup,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
1：增
2：删
3：改
4：查
5: 场景
6：方法
*/
DROP TABLE IF EXISTS my_group_view;
CREATE TABLE IF NOT EXISTS my_group_view (
                  id BIGINT,
                  my_group_id BIGINT,
                  view_id BIGINT,
                  view_type VARCHAR(2),
                  PRIMARY KEY (id, my_group_id)
                ) WITH "template=MyMeta_template,affinityKey=my_group_id,cache_name=my_group_view,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
记录 no sql
*/
DROP TABLE IF EXISTS my_cache;
CREATE TABLE IF NOT EXISTS my_cache (
                sql_line VARCHAR,
                data_regin VARCHAR,
                cache_name VARCHAR(20),
                group_id BIGINT,
                PRIMARY KEY (cache_name, group_id)
) WITH "template=MyMeta_template,KEY_TYPE=cn.plus.model.nosql.MyCacheGroup,VALUE_TYPE=cn.plus.model.nosql.MyCacheValue,cache_name=my_cache,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta";

/**
对 DDL 的操作:
DDL 只允许流程优化的人来执行，因为要改变底层数据结构。
一般要提交签报，层层审批！
*/

/*
用 admin 为用户名和密码登录后，设置为不可用 （如果用户组和用户不为空）
1、添加用户组 my_users_group
2、添加用户到用户组
3、创建表
4、创建数据集，数据集包含表
5、通过 my_group_view 将用户组关联到，数据集

case：当用户查询一个表时
select * from A as a, B as b where a.name = b.name;

执行过程：
1、获取用户的 my_group_id 和 data_set_id
2、如果 data_set_id <> 0; 获取 data_set_name 这个前缀，
查询 A, B 是否在 data_set_name 中，例如：
如果 A 在 data_set_name 中，
替换 A 的名字为 data_set_name_A

3、获取 my_select_views code 获取权限

4、在将获取的 code 和处理过的 sql 组合在一起查询
*/







































































