--装备资源管理_装备技术指标
create table dwd.dwd_erms_equ_tech_d
(
    equ_mastercode string comment '装备主数据编码',
    asid           string comment '资产id',
    techcode       string comment '技术指标编号',
    techname       string comment '技术指标名称',
    value          string comment '技术指标值',
    listuse        string comment '列表是否展示',
    sno            string comment '列表展示顺序',
    etl_time       string comment 'etl_时间',
    source_system  string comment '来源系统',
    source_table   string comment '来源表名'
) comment '装备资源管理_装备技术指标'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_装备技术指标
with c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31'),
     a as (select *
           from ods.ods_cccc_erms_base_equ_tech_f_d)
-- insert overwrite table dwd.dwd_erms_equ_tech_d partition(etl_date = '${etl_date}')
select c.mastercode                                                            as equ_mastercode --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.asid                                                                  as asid           --资产id
     , a.techcode                                                              as techcode       --技术指标编号
     , a.techname                                                              as techname       --技术指标名称
     , a.value                                                                 as value          --技术指标值
     , a.listuse                                                               as listuse        --列表是否展示
     , a.sno                                                                   as sno            --列表展示顺序
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                  as etl_time
     , 'ERMS'                                                                  as source_system
     , 'ods_cccc_erms_base_pub_assetsinfo_i_d,ods_cccc_erms_base_equ_tech_f_d' as source_table
from a
         left join c on a.asid = c.asid
;