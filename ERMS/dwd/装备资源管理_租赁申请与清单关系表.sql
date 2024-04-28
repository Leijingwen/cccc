--装备资源管理_租赁申请与清单关系表
create table dwd.dwd_erms_out_lease_apply_relation_d
(
    llid          string comment '租赁装备清单id',
    leaseid       string comment '租赁申请id',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_租赁申请与清单关系表'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;

--装备资源管理_租赁申请与清单关系表
with a as (select *
            from ods.ods_cccc_erms_out_lease_apply_relation_f_d)
--insert overwrite table dwd.dwd_erms_out_lease_apply_relation_d partition(etl_date = '${etl_date}')
select a.llid                                                as llid    --租赁装备清单id
     , a.leaseid                                             as leaseid --租赁申请id
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_out_lease_apply_relation_f_d'           as source_table
from a
;