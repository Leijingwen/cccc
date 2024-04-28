--
-- 发现表是拉链表,但是源表没有ctime或者mtime, 改为全量表
-- 1. 调整stg 表名 , 修改加载任务
alter table stg.stg_cccc_erms_handle_dis_equ_disposal_i_d
    rename to stg.stg_cccc_erms_handle_dis_equ_disposal_f_d;
-- 2. ods重新建表  修改 stg_to_ods.sql
drop table ods.ods_cccc_erms_handle_dis_equ_disposal_i_d;
CREATE TABLE `ods.ods_cccc_erms_handle_dis_equ_disposal_f_d`
(
    `id`              string         DEFAULT NULL COMMENT 'ID',
    `equ_master_code` string         DEFAULT NULL COMMENT '装备主数据编码',
    `equ_name`        string         DEFAULT NULL COMMENT '装备名称',
    `net_worth`       decimal(20, 6) DEFAULT NULL COMMENT '净值',
    `original_value`  decimal(20, 6) DEFAULT NULL COMMENT '原值',
    `property_unit`   string         DEFAULT NULL COMMENT '产权单位',
    `equ_life`        string         DEFAULT NULL COMMENT '装备年限',
    `status`          string         DEFAULT NULL COMMENT '状态',
    `disid`           string         DEFAULT NULL COMMENT '处置信息台账表ID',
    `asid`            string         DEFAULT NULL COMMENT '装备id',
    `code`            string         DEFAULT NULL COMMENT '管理编号',
    `amount`          decimal(20, 6) DEFAULT NULL COMMENT '处置金额(元)',
    `etl_time`        string         DEFAULT NULL COMMENT 'etl_加载时间'
)
    COMMENT '处置管理_装备处置信息'
    stored as orc;

insert overwrite table ods.ods_cccc_erms_handle_dis_equ_disposal_f_d
select id
     , equ_master_code
     , equ_name
     , net_worth
     , original_value
     , property_unit
     , equ_life
     , status
     , disid
     , asid
     , code
     , amount
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
from stg.stg_cccc_erms_handle_dis_equ_disposal_f_d
where etl_date = '${etl_date}';

-- 4.文档维护

-- 5.dwd_开发
DROP TABLE  dwd.dwd_erms_handle_dis_equ_disposal_d;
create table dwd.dwd_erms_handle_dis_equ_disposal_d
(
    hdequid          string comment '处置装备id',
    asid             string comment '资产id',
    equ_mastercode  string comment '装备主数据编码',
    disid            string comment '处置信息台账id',
    equ_name         string comment '装备名称',
    net_worth        Decimal(20, 6) comment '净值',
    original_value   Decimal(20, 6) comment '原值',
    property_unit_id string comment '产权单位id',
    equ_life         string comment '装备年限',
    status_code      string comment '状态编码',
    status_name      string comment '状态名称',
    manager_code     string comment '管理编号',
    amount           Decimal(20, 6) comment '处置金额(元)',
    etl_time         string comment 'etl_时间',
    source_system    string comment '来源系统',
    source_table     string comment '来源表名'
) comment '装备资源管理_装备处置信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;

with dis_equ as (select *
                 from ods.ods_cccc_erms_handle_dis_equ_disposal_f_d),
     dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname = '施工状态'),
     pub as (select asid,
                    cnname,
                    mastercode
             from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
             where end_date = '2999-12-31')
insert overwrite table dwd.dwd_erms_handle_dis_equ_disposal_d partition(etl_date = '${etl_date}')
select dis_equ.id                                                                                            as hdequid          --处置装备id
     , dis_equ.asid                                                                                          as asid             --资产id
     , pub.mastercode                                                                                        as equ_mastercode   --装备主数据编码
     , dis_equ.disid                                                                                         as disid            --处置信息台账id
     , pub.cnname                                                                                            as equ_name         --装备名称
     , dis_equ.net_worth                                                                                     as net_worth        --净值
     , dis_equ.original_value                                                                                as original_value   --原值
     , dis_equ.property_unit                                                                                 as property_unit_id --产权单位id
     , dis_equ.equ_life                                                                                      as equ_life         --装备年限
     , dis_equ.status                                                                                        as status_code      --状态编码
     , dict.diname                                                                                           as status_name      --状态名称
     , dis_equ.code                                                                                          as manager_code     --管理编号
     , dis_equ.amount                                                                                        as amount           --处置金额(元)
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                as etl_time
     , 'ERMS'                                                                                                as source_system
     , 'ods_cccc_erms_handle_dis_equ_disposal_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d,dim_erms_dictitem_d' as source_table
from dis_equ
         left join dict on dis_equ.status = dict.dicode
         left join pub on dis_equ.asid = pub.asid
;




