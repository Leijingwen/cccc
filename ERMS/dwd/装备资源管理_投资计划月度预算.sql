-- 源表没有ctime以及mtime
-- 拉链改全量

-- stg
alter table stg.stg_cccc_erms_invm_plan_month_amountdet_i_d
    rename to stg.stg_cccc_erms_invm_plan_month_amountdet_f_d;


-- ods
drop table ods.ods_cccc_erms_invm_plan_month_amountdet_f_d;

CREATE TABLE ods.ods_cccc_erms_invm_plan_month_amountdet_f_d
(
    `mtid`           string          DEFAULT NULL COMMENT '主键',
    `mdid`           string          DEFAULT NULL COMMENT '年度分列id',
    `plid`           string          DEFAULT NULL COMMENT '计划ID',
    `year`           string          DEFAULT NULL COMMENT '年份',
    `month`          string          DEFAULT NULL COMMENT '月份',
    `amount`         decimal(20, 6)  DEFAULT NULL COMMENT '含税金额(元)',
    `equnum`         decimal(38, 15) DEFAULT NULL COMMENT '装备数量',
    `taxamount`      decimal(20, 6)  DEFAULT NULL COMMENT '不含税金额(元)',
    `estpayamount`   decimal(20, 6)  DEFAULT NULL COMMENT '预计支付金额(元)',
    `ispushiniti`    string          DEFAULT NULL COMMENT '推送财务初始化',
    `executedbudget` decimal(20, 6)  DEFAULT NULL COMMENT '已执行预算',
    `futurebudget`   decimal(20, 6)  DEFAULT NULL COMMENT '未来30天资金预算',
    `yearplid`       string          DEFAULT NULL COMMENT '父计划id',
    `etl_time`       string          DEFAULT NULL COMMENT 'etl_加载时间'
)
    COMMENT '投资_计划_投资计划月度预算表'
    stored as orc;


insert overwrite table ods.ods_cccc_erms_invm_plan_month_amountdet_f_d
select mtid
     , mdid
     , plid
     , year
     , month
     , amount
     , equnum
     , taxamount
     , estpayamount
     , ispushiniti
     , executedbudget
     , futurebudget
     , yearplid
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
from stg.stg_cccc_erms_invm_plan_month_amountdet_f_d
where etl_date = '${etl_date}';


-- dwd
drop table dwd.dwd_erms_invm_plan_month_amountdet_d;
create table dwd.dwd_erms_invm_plan_month_amountdet_d
(
    mtid                string comment '月度预算id',
    plid                string comment '计划ID',
    year                string comment '年份',
    month               string comment '月份',
    equnum              Decimal(38, 15) comment '装备数量',
    tax_included_amount Decimal(20, 6) comment '含税金额(元)',
    tax_excluded_amount Decimal(20, 6) comment '不含税金额(元)',
    estpayamount        Decimal(20, 6) comment '预计支付金额(元)',
    ispushiniti         string comment '是否推送财务初始化',
    executedbudget      Decimal(20, 6) comment '已执行预算',
    futurebudget        Decimal(20, 6) comment '未来30天资金预算',
    yearplid            string comment '父计划id',
    etl_time            string comment 'etl_时间',
    source_system       string comment '来源系统',
    source_table        string comment '来源表名'
) comment '装备资源管理_投资计划月度预算'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资计划月度预算
with t1 as (select *
            from ods.ods_cccc_erms_invm_plan_month_amountdet_f_d)
-- insert overwrite table dwd.dwd_erms_invm_plan_month_amountdet_d partition(etl_date = '${etl_date}')
select t1.mtid                                                as mtid                --月度预算id
     , t1.plid                                                as plid                --计划ID
     , t1.year                                                as year                --年份
     , t1.month                                               as month               --月份
     , t1.equnum                                              as equnum              --装备数量
     , t1.amount                                              as tax_included_amount --含税金额(元)
     , t1.taxamount                                           as tax_excluded_amount --不含税金额(元)
     , t1.estpayamount                                        as estpayamount        --预计支付金额(元)
     , t1.ispushiniti                                         as ispushiniti         --是否推送财务初始化
     , t1.executedbudget                                      as executedbudget      --已执行预算
     , t1.futurebudget                                        as futurebudget        --未来30天资金预算
     , t1.yearplid                                            as yearplid            --父计划id
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_invm_plan_month_amountdet_f_d'          as source_table
from t1
;
