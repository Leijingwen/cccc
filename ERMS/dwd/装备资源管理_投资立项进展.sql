create table dwd.dwd_erms_invm_plan_progress_d
(
    ppcid          string comment '投资立项子表id',
    fill_cycle     string comment '填报周期',
    curamount      Decimal(19, 2) comment '本期完成投资金(元)',
    nodedesc       string comment '关键节点进展描述',
    plancompletion string comment '时间计划完成情况',
    curfinishwork  Decimal(8, 4) comment '本期工作量计划完成比(%)',
    improveway     string comment '改进措施',
    ctime          string comment '创建时间',
    mtime          string comment '修改时间',
    start_date     string comment '开始日期',
    etl_time       string comment 'etl_时间',
    source_system  string comment '来源系统',
    source_table   string comment '来源表名'
) comment '装备资源管理_投资立项进展'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资立项进展
with t1 as (select *
            from ods.ods_cccc_erms_invm_plan_progress_i_d
            where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_invm_plan_progress_d partition(etl_date = '${etl_date}')
select t1.ppcid                                               as ppcid          --投资立项子表id
     , t1.rcycle                                              as fill_cycle     --填报周期
     , t1.curamount                                           as curamount      --本期完成投资金(元)
     , t1.nodedesc                                            as nodedesc       --关键节点进展描述(非直取,请注意查看文档进行调整)
     , t1.plancompletion                                      as plancompletion --时间计划完成情况
     , t1.curfinishwork                                       as curfinishwork  --本期工作量计划完成比(%)(非直取,请注意查看文档进行调整)
     , t1.improveway                                          as improveway     --改进措施
     , t1.ctime                                               as ctime          --创建时间
     , t1.mtime                                               as mtime          --修改时间
     , t1.start_date                                          as start_date     --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_invm_plan_progress_i_d'                 as source_table
from t1
;