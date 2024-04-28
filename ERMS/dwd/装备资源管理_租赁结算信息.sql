--装备资源管理_租赁结算信息
create table dwd.dwd_erms_out_settle_info_d
(
    settleid      string comment '租赁结算id',
    sno           Decimal(38, 0) comment '序号',
    period        string comment '期次',
    shreamount    Decimal(19, 6) comment '应结算金额（元）',
    realreamount  Decimal(19, 6) comment '实际结算金额（元）',
    shpamount     Decimal(19, 6) comment '应支付金额（元）',
    realpamount   Decimal(19, 6) comment '实际支付金额（元）',
    rsid          string comment '租赁装备ID',
    ctime         string comment '创建时间',
    mtime         string comment '修改时间',
    llid          string comment '装备租赁清单id',
    start_date    string comment '开始日期',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_租赁结算信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_租赁结算信息
with a as (select *
           from ods.ods_cccc_erms_out_settle_info_i_d
           where end_date = '2999-12-31'
             and isdelete != '1')
--insert overwrite table dwd.dwd_erms_out_settle_info_d partition(etl_date = '${etl_date}')
select a.settleid                                             as settleid     --租赁结算id
     , a.sno                                                  as sno          --序号
     , a.period                                               as period       --期次
     , a.shreamount                                           as shreamount   --应结算金额（元）
     , a.realreamount                                         as realreamount --实际结算金额（元）
     , a.shpamount                                            as shpamount    --应支付金额（元）
     , a.realpamount                                          as realpamount  --实际支付金额（元）
     , a.rsid                                                 as rsid         --租赁装备ID
     , a.ctime                                                as ctime        --创建时间
     , a.mtime                                                as mtime        --修改时间
     , a.llid                                                 as llid         --装备租赁清单id
     , a.start_date                                           as start_date   --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_out_settle_info_i_d'                    as source_table
from a
;


