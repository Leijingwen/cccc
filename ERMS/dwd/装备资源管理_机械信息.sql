--装备资源管理_机械信息
create table dwd.dwd_erms_mechaninfo_d
(
    equ_mastercode string comment '装备主数据编码',
    asid           string comment '资产ID',
    equtype_code   string comment '装备类型编码',
    equtype_name   string comment '装备类型名称',
    nameplate      string comment '动力厂牌',
    power          string comment '动力功率（千瓦）',
    powername      string comment '动力名称',
    powercode      string comment '动力型号',
    powertype      string comment '动力类型',
    weight         Decimal(10, 2) comment '整体重量',
    specs          string comment '整体尺寸',
    license        string comment '牌照',
    npv            Decimal(20, 6) comment '净值',
    noo            Decimal(20, 2) comment '新旧系数',
    depr           Decimal(10, 2) comment '折旧年限',
    ctime          string comment '创建时间',
    mtime          string comment '修改时间',
    note           string comment '备注',
    enginepower    Decimal(10, 2) comment '发动机功率',
    major_equ      string comment '是否重大装备:0,否 1,是',
    fixed_id       string comment '固定资产ID',
    dist           Decimal(10, 2) comment '闲置装备与登录人的距离',
    carnumber      string comment '车牌号',
    start_date     string comment '开始日期',
    etl_time       string comment 'etl_时间',
    source_system  string comment '来源系统',
    source_table   string comment '来源表名'
) comment '装备资源管理_机械信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;






--装备资源管理_机械信息
--27
with b as (select *
            from ods.ods_cccc_erms_equip_type_mapping_f_d),
     c as (select *
            from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
            where end_date = '2999-12-31'),
     a as (select *
            from ods.ods_cccc_erms_base_equ_mechaninfo_i_d
            where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_mechaninfo_d partition(etl_date = '${etl_date}')
select c.mastercode                                                                                                      as equ_mastercode --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.asid                                                                                                            as asid           --资产ID
     , a.type                                                                                                            as equtype_code   --装备类型编码
     , b.etypename                                                                                                       as equtype_name   --装备类型名称(非直取,请注意查看文档进行调整)
     , a.nameplate                                                                                                       as nameplate      --动力厂牌
     , a.power                                                                                                           as power          --动力功率（千瓦）
     , a.powername                                                                                                       as powername      --动力名称
     , a.powercode                                                                                                       as powercode      --动力型号
     , a.powertype                                                                                                       as powertype      --动力类型
     , a.weight                                                                                                          as weight         --整体重量
     , a.specs                                                                                                           as specs          --整体尺寸
     , a.license                                                                                                         as license        --牌照
     , a.npv                                                                                                             as npv            --净值
     , a.noo                                                                                                             as noo            --新旧系数
     , a.depr                                                                                                            as depr           --折旧年限
     , a.ctime                                                                                                           as ctime          --创建时间
     , a.mtime                                                                                                           as mtime          --修改时间
     , a.note                                                                                                            as note           --备注
     , a.enginepower                                                                                                     as enginepower    --发动机功率
     , a.major_equ                                                                                                       as major_equ      --是否重大装备:0,否 1,是
     , a.fixed_id                                                                                                        as fixed_id       --固定资产ID
     , a.dist                                                                                                            as dist           --闲置装备与登录人的距离
     , a.carnumber                                                                                                       as carnumber      --车牌号
     , a.start_date                                                                                                      as start_date     --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                             as etl_time
     , 'ERMS'                                                                                                             as source_system
     , 'ods_cccc_erms_equip_type_mapping_f_d,ods_cccc_erms_base_pub_assetsinfo_i_d,ods_cccc_erms_base_equ_mechaninfo_i_d' as source_table
from a
         left join c on a.asid = c.asid
         left join b on a.type = b.etypeid
;