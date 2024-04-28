create table dwd.dwd_erms_deploy_dispatch_equ_information_d
(
    disequid        string comment '调遣装备清单id',
    disid           string comment '调遣单id',
    asid            string comment '资产id',
    equ_mastercode  string comment '装备主数据编码',
    equcode         string comment '装备编码',
    equname         string comment '装备名称',
--     type_code       string comment '装备大类编码',
--     type_name       string comment '装备大类名称',
    model           string comment '规格型号',
    expenses        Decimal(20, 6) comment '调遣费用',
    plan_start_date string comment '计划进场日期',
    plan_end_date   string comment '计划退场日期',
    act_start_date  string comment '实际进场日期',
    ctime           string comment '创建时间',
    mtime           string comment '修改时间',
    manager_code    string comment '管理编号',
    start_date      string comment '开始日期',
    etl_time        string comment 'etl_时间',
    source_system   string comment '来源系统',
    source_table    string comment '来源表名'
) comment '装备资源管理_调配装备记录'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_调配装备记录
with t1 as (select *
            from ods.ods_cccc_erms_deploy_dispatch_equ_information_i_d
            where end_date = '2999-12-31'
              and isdelete != '1'),
     t2 as (select asid,
                   mastercode,
                   cnname
            from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
            where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_deploy_dispatch_equ_information_d partition(etl_date = '${etl_date}')
select t1.id                                                                                     as disequid        --调遣装备清单id
     , t1.disid                                                                                  as disid           --调遣单id
     , t1.asid                                                                                   as asid            --资产id
     , t2.mastercode                                                                             as equ_mastercode  --装备主数据编码
     , t1.equcode                                                                                as equcode         --装备编码
     , t1.shipname                                                                               as equname         --装备名称
--      , t1.type                                                                                   as type_code       --装备大类编码
--      , t1.type                                                                                   as type_name       --装备大类名称(非直取,请注意查看文档进行调整)
     , t1.model                                                                                  as model           --规格型号
     , t1.expenses                                                                               as expenses        --调遣费用
     , t1.plan_start_date                                                                        as plan_start_date --计划进场日期
     , t1.plan_end_date                                                                          as plan_end_date   --计划退场日期
     , t1.act_start_date                                                                         as act_start_date  --实际进场日期
     , t1.ctime                                                                                  as ctime           --创建时间
     , t1.mtime                                                                                  as mtime           --修改时间
     , t1.code                                                                                   as manager_code    --管理编号
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                    as etl_time
     , t1.start_date                                                                             as start_date      --开始日期
     , 'ERMS'                                                                                    as source_system
     , 'ods_cccc_erms_deploy_dispatch_equ_information_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d' as source_table
from t1
         left join t2 on t1.asid = t2.asid
;