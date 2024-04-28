--
-- 发现表是拉链表,但是源表没有ctime或者mtime, 改为全量表
-- 1. 调整stg 表名 , 修改加载任务
alter table stg.stg_cccc_erms_handle_dis_disposal_infor_account_i_d
    rename to stg.stg_cccc_erms_handle_dis_disposal_infor_account_f_d;
-- 2. ods重新建表  修改 stg_to_ods.sql
drop table ods.ods_cccc_erms_handle_dis_disposal_infor_account_i_d;
CREATE TABLE `ods.ods_cccc_erms_handle_dis_disposal_infor_account_f_d`
(
    `disid`             string         DEFAULT NULL COMMENT 'ID',
    `disposal_order_no` string         DEFAULT NULL COMMENT '处置单号',
    `describe`          string         DEFAULT NULL COMMENT '描述',
    `disposal_type`     string         DEFAULT NULL COMMENT '处置类型',
    `informant`         string         DEFAULT NULL COMMENT '提报人',
    `informant_time`    string         DEFAULT NULL COMMENT '提报时间',
    `unit_name`         string         DEFAULT NULL COMMENT '单位名称',
    `disposal_amount`   decimal(20, 6) DEFAULT NULL COMMENT '处置金额',
    `disposal_time`     string         DEFAULT NULL COMMENT '处置时间',
    `department_name`   string         DEFAULT NULL COMMENT '部门名称',
    `business_type`     string         DEFAULT NULL COMMENT '业务类型',
    `status`            string         DEFAULT NULL COMMENT '状态',
    `disposal_instru`   string         DEFAULT NULL COMMENT '处置说明',
    `before_net_value`  decimal(20, 6) DEFAULT NULL COMMENT '处置前净值',
    `ctime`             string         DEFAULT NULL COMMENT '创建时间',
    `hdaid`             string         DEFAULT NULL COMMENT '处置申请id',
    `code`              string         DEFAULT NULL COMMENT '处置编号',
    `cuoid`             string         DEFAULT NULL COMMENT '创建人所在机构id',
    `etl_time`          string         DEFAULT NULL COMMENT 'etl_加载时间'
)
    COMMENT '处置管理_处置信息台账'
    stored as orc
;

insert overwrite table ods.ods_cccc_erms_handle_dis_disposal_infor_account_f_d
select disid,
       disposal_order_no,
       `describe`,
       disposal_type,
       informant,
       informant_time,
       unit_name,
       disposal_amount,
       disposal_time,
       department_name,
       business_type,
       status,
       disposal_instru,
       before_net_value,
       ctime,
       hdaid,
       code,
       cuoid,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
from stg.stg_cccc_erms_handle_dis_disposal_infor_account_f_d
where etl_date = '${etl_date}';

-- 4.文档维护

-- 5.dwd_开发

create table dwd.dwd_erms_handle_dis_disposal_infor_account_d
(
    disid              string comment '处置信息台账id',
    hdaid              string comment '处置申请id',
    disposal_order_no  string comment '处置单号',
    discode            string comment '处置编号',
    describe           string comment '描述',
    disposal_type_code string comment '处置类型编码',
    disposal_type_name string comment '处置类型名称',
    informant_name     string comment '提报人名称',
    informant_date     string comment '提报日期',
    disposal_amount    Decimal(20, 6) comment '处置金额',
    disposal_date      string comment '处置日期',
    disposal_instru    string comment '处置说明',
    before_net_value   Decimal(20, 6) comment '处置前净值',
    unit_id            string comment '单位id',
    unit_name          string comment '单位名称',
    second_unit_id     string comment '二级单位id',
    second_unit_name   string comment '二级单位名称',
    third_unit_id      string comment '三级单位id',
    third_unit_name    string comment '三级单位名称',
    status_code        string comment '状态编码',
    status_name        string comment '状态名称',
    ctime              string comment '创建时间',
    etl_time           string comment 'etl_时间',
    source_system      string comment '来源系统',
    source_table       string comment '来源表名'
) comment '装备资源管理_处置信息台账'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;

with dis_inac as (select *
                  from ods.ods_cccc_erms_handle_dis_disposal_infor_account_f_d),
     org as (select oid
                  , oname
                  , second_unit_id
                  , second_unit_name
                  , third_unit_id
                  , third_unit_name
             from dwd.dim_erms_orgext_d)
insert overwrite table dwd.dwd_erms_handle_dis_disposal_infor_account_d partition ( etl_date = '${etl_date}' )
select dis_inac.disid                                         as disid              --处置信息台账id
     , dis_inac.hdaid                                         as hdaid              --处置申请id
     , dis_inac.disposal_order_no                             as disposal_order_no  --处置单号
     , dis_inac.code                                          as discode            --处置编号
     , dis_inac.describe                                      as describe           --描述
     , dis_inac.disposal_type                                 as disposal_type_code --处置类型编码
     , case
           when dis_inac.disposal_type = '01' then '让售'
           when dis_inac.disposal_type = '02' then '报废' end as disposal_type_name --处置类型名称
     , dis_inac.informant                                     as informant_name     --提报人名称
     , dis_inac.informant_time                                as informant_date     --提报日期
     , dis_inac.disposal_amount                               as disposal_amount    --处置金额
     , dis_inac.disposal_time                                 as disposal_date      --处置日期
     , dis_inac.disposal_instru                               as disposal_instru    --处置说明
     , dis_inac.before_net_value                              as before_net_value   --处置前净值
     , dis_inac.unit_name                                     as unit_id            --单位id
     , org.oname                                              as unit_name          --单位名称
     , org.second_unit_id                                     as second_unit_id     --二级单位id
     , org.second_unit_name                                   as second_unit_name   --二级单位名称
     , org.third_unit_id                                      as third_unit_id      --三级单位id
     , org.third_unit_name                                    as third_unit_name    --三级单位名称
     , dis_inac.status                                        as status_code        --状态编码
     , case
           when dis_inac.status = '01' then '新建'
           when dis_inac.status = '02' then '提交财务'
           when dis_inac.status = '03' then '处置完成' end    as status_name        --状态名称
     , dis_inac.ctime                                         as ctime              --创建时间
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_handle_dis_disposal_infor_account_i_d'  as source_table
from dis_inac
         left join org on dis_inac.unit_name = org.oid
;




