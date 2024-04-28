alter table stg.stg_cccc_erms_invm_plan_year_amountdet_i_d
    rename to stg.stg_cccc_erms_invm_plan_year_amountdet_f_d;

drop table ods.ods_cccc_erms_invm_plan_year_amountdet_i_d;
show create table stg.STG_CCCC_ERMS_INVM_PLAN_YEAR_AMOUNTDET_I_D;
CREATE TABLE `ods.ods_cccc_erms_invm_plan_year_amountdet_f_d`
(
    `mdid`         string          DEFAULT NULL COMMENT 'MDID',
    `plid`         string          DEFAULT NULL COMMENT '投资计划ID',
    `year`         string          DEFAULT NULL COMMENT '年度',
    `amount`       decimal(20, 6)  DEFAULT NULL COMMENT '投资金额',
    `taxamount`    decimal(20, 6)  DEFAULT NULL COMMENT '不含税金额(万元)',
    `equnum`       decimal(38, 15) DEFAULT NULL COMMENT '装备数量',
    `ownfunds`     decimal(20, 6)  DEFAULT NULL COMMENT '自有资金',
    `debtfinanc`   decimal(20, 6)  DEFAULT NULL COMMENT '债务融资(外部)',
    `equityfinanc` decimal(20, 6)  DEFAULT NULL COMMENT '权益融资(其他)',
    `estpayamount` decimal(20, 6)  DEFAULT NULL COMMENT '预计支付金额(万元)',
    `etl_time`     string          DEFAULT NULL COMMENT 'etl_加载时间'
)
    COMMENT '投资_计划_投资计划金额明细'
    stored as orc;

insert overwrite table ods.ods_cccc_erms_invm_plan_year_amountdet_f_d
select `mdid`
     , `plid`
     , `year`
     , `amount`
     , `taxamount`
     , `equnum`
     , `ownfunds`
     , `debtfinanc`
     , `equityfinanc`
     , `estpayamount`
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
from stg.stg_cccc_erms_invm_plan_year_amountdet_f_d
WHERE etl_date = '${etl_date}';


--装备资源管理_投资计划年度预算
drop table dwd.dwd_erms_invm_plan_year_amountdet_d;
create table dwd.dwd_erms_invm_plan_year_amountdet_d
(
    mdid                string comment '年度预算id',
    plid                string comment '投资计划ID',
    year                string comment '年度',
    tax_included_amount Decimal(20, 6) comment '含税投资金额(万元)',
    tax_excluded_amount Decimal(20, 6) comment '不含税金额(万元)',
    equnum              Decimal(38, 15) comment '装备数量',
    ownfunds            Decimal(20, 6) comment '自有资金',
    debtfinanc          Decimal(20, 6) comment '债务融资(外部)',
    equityfinanc        Decimal(20, 6) comment '权益融资(其他)',
    estpayamount        Decimal(20, 6) comment '预计支付金额(万元)',
    etl_time            string comment 'etl_时间',
    source_system       string comment '来源系统',
    source_table        string comment '来源表名'
) comment '装备资源管理_投资计划年度预算'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资计划年度预算
with a as (select *
           from ods.ods_cccc_erms_invm_plan_year_amountdet_f_d)
-- insert overwrite table dwd.dwd_erms_invm_plan_year_amountdet_d partition(etl_date = '${etl_date}')
select a.mdid                                                 as mdid                --年度预算id
     , a.plid                                                 as plid                --投资计划ID
     , a.`year`                                               as `year`              --年度
     , a.amount                                               as tax_included_amount --含税投资金额(万元)
     , a.taxamount                                            as tax_excluded_amount --不含税金额(万元)
     , a.equnum                                               as equnum              --装备数量
     , a.ownfunds                                             as ownfunds            --自有资金
     , a.debtfinanc                                           as debtfinanc          --债务融资(外部)
     , a.equityfinanc                                         as equityfinanc        --权益融资(其他)
     , a.estpayamount                                         as estpayamount        --预计支付金额(万元)
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_invm_plan_year_amountdet_f_d'           as source_table
from a
;


desc dwd.dwd_erms_invm_plan_year_amountdet_d;

