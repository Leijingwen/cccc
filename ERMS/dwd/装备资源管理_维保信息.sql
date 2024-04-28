-- 因表里没有mtime 2024-04-26 调整为全量表

-- stg 修改表名 , stg层修改加载任务, 调整文档
alter table stg.stg_cccc_erms_use_maintenance_i_d
    rename to stg.stg_cccc_erms_use_maintenance_f_d;

-- ods调整表, 调整sql
DROP TABLE ODS.ods_cccc_erms_use_maintenance_i_d;
CREATE TABLE ODS.ods_cccc_erms_use_maintenance_f_d
(
    `id`               string         DEFAULT NULL COMMENT 'ID',
    `mana_unit`        string         DEFAULT NULL COMMENT '管理单位',
    `project`          string         DEFAULT NULL COMMENT '所属项目',
    `main_number`      string         DEFAULT NULL COMMENT '维保编号',
    `equicode`         string         DEFAULT NULL COMMENT '装备编号',
    `equ_name`         string         DEFAULT NULL COMMENT '装备名称',
    `main_category`    string         DEFAULT NULL COMMENT '维保类别',
    `main_date`        string         DEFAULT NULL COMMENT '维保日期',
    `prepared_by`      string         DEFAULT NULL COMMENT '编制人',
    `spe_and_model`    string         DEFAULT NULL COMMENT '规格型号',
    `main_unit`        string         DEFAULT NULL COMMENT '维保单位',
    `main_cost`        decimal(19, 2) DEFAULT NULL COMMENT '维保费用',
    `value_add_tax`    decimal(19, 2) DEFAULT NULL COMMENT '增值税',
    `total_tax_inclu`  decimal(19, 2) DEFAULT NULL COMMENT '含税合计',
    `warra_start_time` string         DEFAULT NULL COMMENT '保修开始时间',
    `warra_end_time`   string         DEFAULT NULL COMMENT '保修结束时间',
    `main_quali`       string         DEFAULT NULL COMMENT '维保资质',
    `main_effect`      string         DEFAULT NULL COMMENT '维保效果',
    `brand_name`       string         DEFAULT NULL COMMENT '品牌名称',
    `contact_infor`    string         DEFAULT NULL COMMENT '联系方式',
    `node`             string         DEFAULT NULL COMMENT '说明',
    `ctime`            string         DEFAULT NULL COMMENT '创建时间',
    `isdelete`         string         DEFAULT NULL COMMENT '是否删除',
    `asid`             string         DEFAULT NULL COMMENT '主数据id',
    `mana_unitname`    string         DEFAULT NULL COMMENT '管理单位名称',
    `couid`            string         DEFAULT NULL COMMENT '暂未用到',
    `cuname`           string         DEFAULT NULL COMMENT '创建人姓名',
    `cuoid`            string         DEFAULT NULL COMMENT '创建人所在单位',
    `cuoname`          string         DEFAULT NULL COMMENT '暂未用到',
    `muid`             string         DEFAULT NULL COMMENT '修改人',
    `muoid`            string         DEFAULT NULL COMMENT '修改人所在单位',
    `cuid`             string         DEFAULT NULL COMMENT '创建人',
    `code`             string         DEFAULT NULL COMMENT '管理编号',
    `zproject`         string         DEFAULT NULL COMMENT '项目主数据id',
    `etl_time`         string         DEFAULT NULL COMMENT 'etl_加载时间'
)
    COMMENT '使用管理_维保信息'
    STORED AS ORC;


--开发 /SQL文件/数据湖建设工作区/数据开发/ODS/装备资源管理系统/每日脚本_拉链 下删除对应任务
--/SQL文件/数据湖建设工作区/数据开发/ODS/装备资源管理系统/首日脚本_拉链 移动到 /SQL文件/数据湖建设工作区/数据开发/ODS/装备资源管理系统/全量_纯增量_统一化脚本 下
-- 修改文件名字 021_首日_使用管理_维保信息 -> 021_全量_使用管理_维保信息

insert overwrite table ods.ods_cccc_erms_use_maintenance_f_d
select id
     , mana_unit
     , project
     , main_number
     , equicode
     , equ_name
     , main_category
     , main_date
     , prepared_by
     , spe_and_model
     , main_unit
     , main_cost
     , value_add_tax
     , total_tax_inclu
     , warra_start_time
     , warra_end_time
     , main_quali
     , main_effect
     , brand_name
     , contact_infor
     , node
     , ctime
     , isdelete
     , asid
     , mana_unitname
     , couid
     , cuname
     , cuoid
     , cuoname
     , muid
     , muoid
     , cuid
     , code
     , zproject
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
from stg.stg_cccc_erms_use_maintenance_f_d
WHERE etl_date = '${etl_date}';


--dwd
create table dwd.dwd_erms_use_maintenance_d
(
    mainid             string comment '维保id',
    asid               string comment '资产id',
    equ_mastercode     string comment '装备主数据编码',
    zproject           string comment '项目主数据id',
    project_name       string comment '所属项目名称',
    main_number        string comment '维保编号',
    equ_code           string comment '装备编号',
    equ_name           string comment '装备名称',
    main_category_code string comment '维保类别编码',
    main_category_name string comment '维保类别名称',
    main_date          string comment '维保日期',
    prepared_by_name   string comment '编制人名称',
    spe_and_model      string comment '规格型号',
    main_cost          Decimal(19, 2) comment '维保费用',
    value_add_tax      Decimal(19, 2) comment '增值税',
    total_tax_inclu    Decimal(19, 2) comment '含税合计',
    warra_start_time   string comment '保修开始时间',
    warra_end_time     string comment '保修结束时间',
    main_quali         string comment '维保资质',
    main_effect        string comment '维保效果',
    brand_name         string comment '品牌名称',
    contact_infor      string comment '联系方式',
    node               string comment '说明',
    ctime              string comment '创建时间',
    manager_code       string comment '管理编号',
    main_unit_name     string comment '维保单位名称',
    mana_unit_id       string comment '管理单位id',
    mana_unit_name     string comment '管理单位名称',
    etl_time           string comment 'etl_时间',
    source_system      string comment '来源系统',
    source_table       string comment '来源表名'
) comment '装备资源管理_维保信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_维保信息
with t1 as (select *
            from ods.ods_cccc_erms_use_maintenance_f_d
            where  isdelete = '0'),
     t2 as (select asid, mastercode,cnname
            from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
            where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_use_maintenance_d partition(etl_date = '${etl_date}')
select t1.id                                                                     as mainid             --维保id
     , t1.asid                                                                   as asid               --资产id
     , t2.mastercode                                                             as equ_mastercode     --装备主数据编码(非直取,请注意查看文档进行调整)
     , t1.zproject                                                               as zproject           --项目主数据id
     , t1.project                                                                as project_name       --所属项目名称
     , t1.main_number                                                            as main_number        --维保编号
     , t1.equicode                                                               as equ_code           --装备编号
     , t1.equ_name                                                               as equ_name           --装备名称
     , t1.main_category                                                          as main_category_code --维保类别编码
     , case
           when t1.main_category = '01' then '维修'
           when t1.main_category = '02' then '保养' end                          as main_category_name --维保类别名称
     , t1.main_date                                                              as main_date          --维保日期
     , t1.prepared_by                                                            as prepared_by_name   --编制人名称
     , t1.spe_and_model                                                          as spe_and_model      --规格型号
     , t1.main_cost                                                              as main_cost          --维保费用
     , t1.value_add_tax                                                          as value_add_tax      --增值税
     , t1.total_tax_inclu                                                        as total_tax_inclu    --含税合计
     , t1.warra_start_time                                                       as warra_start_time   --保修开始时间
     , t1.warra_end_time                                                         as warra_end_time     --保修结束时间
     , t1.main_quali                                                             as main_quali         --维保资质
     , t1.main_effect                                                            as main_effect        --维保效果
     , t1.brand_name                                                             as brand_name         --品牌名称
     , t1.contact_infor                                                          as contact_infor      --联系方式
     , t1.node                                                                   as node               --说明
     , t1.ctime                                                                  as ctime              --创建时间
     , t1.code                                                                   as manager_code       --管理编号
     , t1.main_unit                                                              as main_unit_name     --维保单位名称
     , t1.mana_unit                                                              as mana_unit_id       --管理单位id
     , t1.mana_unitname                                                          as mana_unit_name     --管理单位名称
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                    as etl_time
     , 'ERMS'                                                                    as source_system
     , 'ods_cccc_erms_use_maintenance_f_d,ods_cccc_erms_base_pub_assetsinfo_i_d' as source_table
from t1
         left join t2 on t1.asid = t2.asid
;
