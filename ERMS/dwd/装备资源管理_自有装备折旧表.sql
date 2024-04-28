--装备资源管理_自有装备折旧表
create table dwd.dwd_erms_own_depr_d
(
    equ_mastercode    string comment '装备主数据编码',
    depr_id           string comment '折旧id',
    asid              string comment '资产id',
    depre_period      string comment '折旧期次',
    depre_open_amount Decimal(20, 6) comment '折旧期初金额',
    depre_end_amount  Decimal(20, 6) comment '折旧期末金额',
    cur_res_value     Decimal(20, 6) comment '当前净残值',
    net_worth         Decimal(20, 6) comment '净值',
    ctime             string comment '创建时间',
    mtime             string comment '修改时间',
    depamount         Decimal(20, 6) comment '折旧金额',
    start_date        string comment '开始日期',
    etl_time          string comment 'etl_时间',
    source_system     string comment '来源系统',
    source_table      string comment '来源表名'
) comment '装备资源管理_自有装备折旧表'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_自有装备折旧表
with a as (select *
           from ods.ods_cccc_erms_own_depr_i_d
           where end_date = '2999-12-31'),
     c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_own_depr_d partition(etl_date = '${etl_date}')
select c.mastercode                                                       as equ_mastercode    --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.id                                                               as depr_id           --折旧id
     , a.asid                                                             as asid              --资产id
     , a.depre_period                                                     as depre_period      --折旧期次
     , a.depre_open_amount                                                as depre_open_amount --折旧期初金额
     , a.depre_end_amount                                                 as depre_end_amount  --折旧期末金额
     , a.cur_res_value                                                    as cur_res_value     --当前净残值
     , a.net_worth                                                        as net_worth         --净值
     , a.ctime                                                            as ctime             --创建时间
     , a.mtime                                                            as mtime             --修改时间
     , a.depamount                                                        as depamount         --折旧金额
     , a.start_date                                                       as start_date        --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')             as etl_time
     , 'ERMS'                                                             as source_system
     , 'ods_cccc_erms_own_depr_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d' as source_table
from a
         left join c on a.asid = c.asid
;

