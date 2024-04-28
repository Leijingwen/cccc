create table dwd.dwd_erms_deploy_apply_equ_d
(
    daeid          string comment '调配申请装备清单id',
    asid           string comment '主数据asid',
    equ_mastercode string comment '装备主数据编码',
    daid           string comment '调配申请id',
    equcode        string comment '装备编码',
    equname        string comment '装备名称',
    equtype_code   string comment '装备类型编码',
    equtype_name   string comment '装备类型名称',
    model          string comment '规格型号',
    workarea       string comment '施工区域及工作内容',
    equsource      string comment '设备来源',
    equnum         Decimal(38, 0) comment '装备数量',
    decost         Decimal(19, 2) comment '调配费用（元）',
    planintime     string comment '计划进场时间',
    planouttime    string comment '计划退场时间',
    ctime          string comment '创建时间',
    mtime          string comment '修改时间',
    manager_code   string comment '管理编号',
    start_date     string comment '开始日期',
    etl_time       string comment 'etl_时间',
    source_system  string comment '来源系统',
    source_table   string comment '来源表名'
) comment '装备资源管理_调配申请装备'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_调配申请装备
with t1 as (select *
            from ods.ods_cccc_erms_deploy_apply_equ_i_d
            where end_date = '2999-12-31'
              and isdelete != '1'),
     t2 as (select asid, mastercode, cnname
            from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
            where end_date = '2999-12-31'),
     t3 as (select *
            from dwd.dim_erms_equip_type_mapping_d)
-- insert overwrite table dwd.dwd_erms_deploy_apply_equ_d partition(etl_date = '${etl_date}')
select t1.daeid                                                                                                 as daeid          --调配申请装备清单id
     , t1.asid                                                                                                  as asid           --主数据asid
     , t2.mastercode                                                                                            as equ_mastercode --装备主数据编码
     , t1.daid                                                                                                  as daid           --调配申请id
     , t1.equcode                                                                                               as equcode        --装备编码
     , t1.equname                                                                                               as equname        --装备名称
     , t1.equtype                                                                                               as equtype_code   --装备类型编码
     , t3.equtype_name                                                                                          as equtype_name   --装备类型名称(非直取,请注意查看文档进行调整)
     , t1.model                                                                                                 as model          --规格型号
     , t1.workarea                                                                                              as workarea       --施工区域及工作内容
     , t1.equsource                                                                                             as equsource      --设备来源
     , t1.equamount                                                                                             as equnum         --装备数量
     , t1.decost                                                                                                as decost         --调配费用（元）
     , t1.planintime                                                                                            as planintime     --计划进场时间
     , t1.planouttime                                                                                           as planouttime    --计划退场时间
     , t1.ctime                                                                                                 as ctime          --创建时间
     , t1.mtime                                                                                                 as mtime          --修改时间
     , t1.code                                                                                                  as manager_code   --管理编号
     , t1.start_date                                                                                            as start_date     --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                   as etl_time
     , 'ERMS'                                                                                                   as source_system
     , 'ods_cccc_erms_deploy_apply_equ_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d,dim_erms_equip_type_mapping_d' as source_table
from t1
         left join t2 on t1.asid = t2.asid
         left join t3 on t1.equtype = t3.equtype_code
;