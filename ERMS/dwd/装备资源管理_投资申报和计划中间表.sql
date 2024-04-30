alter table stg.stg_cccc_erms_invm_plan_year_apply2plan_i_d
    rename to stg.stg_cccc_erms_invm_plan_year_apply2plan_f_d;

--show create table stg.stg_cccc_erms_invm_plan_year_apply2plan_f_d;

--drop table ods.ods_cccc_erms_invm_plan_year_apply2plan_f_d;
CREATE TABLE `ods.ods_cccc_erms_invm_plan_year_apply2plan_f_d`
(
    `a2pid`    string DEFAULT NULL COMMENT 'A2PID',
    `apid`     string DEFAULT NULL COMMENT '申报ID',
    `plid`     string DEFAULT NULL COMMENT '计划ID',
    `etl_time` string DEFAULT NULL COMMENT 'etl_加载时间'
)
    COMMENT '投资_计划_投资申报和计划中间表'
    stored as orc;

insert overwrite table ods.ods_cccc_erms_invm_plan_year_apply2plan_f_d
select a2pid
     , apid
     , plid
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
from stg.stg_cccc_erms_invm_plan_year_apply2plan_f_d
where etl_date = '${etl_date}';


create table dwd.dwd_erms_invm_plan_year_apply2plan_d
(
    apid          string comment '申报id',
    plid          string comment '计划id',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_投资申报和计划中间表'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资申报和计划中间表
with a as (select *
           from ods.ods_cccc_erms_invm_plan_year_apply2plan_f_d)
--insert overwrite table dwd.dwd_erms_invm_plan_year_apply2plan_d partition(etl_date = '${etl_date}')
select a.apid                                                 as apid --申报id
     , a.plid                                                 as plid --计划id
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_invm_plan_year_apply2plan_f_d'          as source_table
from a
;
