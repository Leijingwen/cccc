--装备资源管理_自有试验检测设备信息
create table dwd.dwd_erms_own_test_detection_d
(
    equ_mastercode string comment '装备主数据编码',
    asid           string comment '主数据asid',
    ctime          string comment '创建时间',
    mtime          string comment '修改时间',
    depr           string comment '折旧年限',
    note           string comment '备注',
    ldle_stime     string comment '闲置开始时间',
    ldle_etime     string comment '闲置结束时间',
    carnumber      string comment '车牌号',
    start_date     string comment '开始日期',
    etl_time       string comment 'etl_时间',
    source_system  string comment '来源系统',
    source_table   string comment '来源表名'
) comment '装备资源管理_自有试验检测设备信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_自有试验检测设备信息
with c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31'),
     a as (select *
           from ods.ods_cccc_erms_own_test_detection_equipment_i_d
           where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_own_test_detection_d partition(etl_date = '${etl_date}')
select c.mastercode                                                                        as equ_mastercode --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.asid                                                                              as asid           --主数据asid
     , a.ctime                                                                             as ctime          --创建时间
     , a.mtime                                                                             as mtime          --修改时间
     , a.depr                                                                              as depr           --折旧年限
     , a.note                                                                              as note           --备注
     , a.ldle_stime                                                                        as ldle_stime     --闲置开始时间
     , a.ldle_etime                                                                        as ldle_etime     --闲置结束时间
     , a.carnumber                                                                         as carnumber      --车牌号
     , a.start_date                                                                        as start_date     --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                              as etl_time
     , 'ERMS'                                                                              as source_system
     , 'ods_cccc_erms_base_pub_assetsinfo_i_d,ods_cccc_erms_own_test_detection_equipm_i_d' as source_table
from a
         left join c on a.asid = c.asid
;