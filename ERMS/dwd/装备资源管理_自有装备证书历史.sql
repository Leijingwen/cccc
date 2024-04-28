--装备资源管理_自有装备证书历史
create table dwd.dwd_erms_own_certificate_his_d
(
    certificate_his_id       string comment '证书历史id',
    equ_mastercode           string comment '装备主数据编码',
    asid                     string comment '资产id',
    certificate_number       string comment '证书编号',
    effective_date           string comment '生效日期',
    expiry_date              string comment '失效日期',
    certificate_manager_name string comment '证书管理人姓名',
    ctime                    string comment '创建时间',
    mtime                    string comment '修改时间',
    start_date               string comment '开始日期',
    etl_time                 string comment 'etl_时间',
    source_system            string comment '来源系统',
    source_table             string comment '来源表名'
) comment '装备资源管理_自有装备证书历史'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_自有装备证书历史
with c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31'),
     a as (select *
           from ods.ods_cccc_erms_own_certificate_his_i_d
           where end_date = '2999-12-31')
--insert overwrite table dwd.dwd_erms_own_certificate_his_d partition(etl_date = '${etl_date}')
select a.id                                                                          as certificate_his_id       --证书历史id
     , c.mastercode                                                                  as equ_mastercode           --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.asid                                                                        as asid                     --资产id
     , a.certificate_number                                                          as certificate_number       --证书编号
     , a.effective_date                                                              as effective_date           --生效日期
     , a.expiry_date                                                                 as expiry_date              --失效日期
     , a.certificate_manager                                                         as certificate_manager_name --证书管理人姓名
     , a.ctime                                                                       as ctime                    --创建时间
     , a.mtime                                                                       as mtime                    --修改时间
     , a.start_date                                                                  as start_date               --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                        as etl_time
     , 'ERMS'                                                                        as source_system
     , 'ods_cccc_erms_base_pub_assetsinfo_i_d,ods_cccc_erms_own_certificate_his_i_d' as source_table
from a
         left join c on a.asid = c.asid
;