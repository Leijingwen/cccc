create table dwd.dwd_erms_invm_plan_pro_relation_d
(
    pappid        string comment '投资立项id',
    ppcid         string comment '立项子表id',
    datasource    string comment '数据来源',
--     glpappid      string comment '关联其他系统的立项申报id',
--     glppcid       string comment '关联其他系统的立项申报子表id',
--     dj_unit_coid  string comment '填写对接单位的coid',
--     receivetime   string comment '接收时间',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_立项与子表关系表'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_立项与子表关系表
with t1 as (select *
            from ods.ods_cccc_erms_invm_plan_pro_relation_f_d)
-- insert overwrite table dwd.dwd_erms_invm_plan_pro_relation_d partition(etl_date = '${etl_date}')
select t1.pappid                                              as pappid       --投资立项id
     , t1.ppcid                                               as ppcid        --立项子表id
     , t1.datasource                                          as datasource   --数据来源
--      , t1.glpappid                                            as glpappid     --关联其他系统的立项申报id
--      , t1.glppcid                                             as glppcid      --关联其他系统的立项申报子表id
--      , t1.djcoid                                              as dj_unit_coid --填写对接单位的coid
--      , t1.receivetime                                         as receivetime  --接收时间
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_invm_plan_pro_relation_f_d'             as source_table
from t1
;