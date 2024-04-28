--装备资源管理_船舶信息
--45
create table dwd.dwd_erms_boatinfo_d
(
    equ_mastercode string comment '装备主数据编码',
    asid           string comment '资产ID',
    equtype_code   string comment '装备类型编码',
    equtype_name   string comment '装备类型名称',
    futype         string comment '燃料类型',
    mmsicode       string comment 'MMSI号码',
    sacode         string comment '卫星通信号码',
    bdcode         string comment '北斗卫星号码',
    secode         string comment '卫星紧急无线电示位标序号',
    imocode        string comment 'IMO编号',
    shipcall       string comment '船舶呼号码',
    length         Decimal(10, 2) comment '船舶长度（米）',
    width          Decimal(10, 2) comment '船舶宽度（米）',
    depth          Decimal(10, 2) comment '船舶型深（米）',
    loaddepth      Decimal(10, 2) comment '满载吃水深度（米）',
    loaddisp       Decimal(10, 2) comment '满载排水量（立方米）',
    load           Decimal(10, 2) comment '载重量（吨）',
    capacity       Decimal(10, 2) comment '舱容（立方米）',
    weight         Decimal(10, 2) comment '净吨/总重量（吨）',
    allpower       string comment '总装机功率（千瓦）',
    narea          string comment '航区',
    speed          Decimal(10, 2) comment '满载航速（节）',
    enginetype     string comment '主机类型',
    engineamount   Decimal(38, 15) comment '主机台数',
    enginepower    string comment '主机总功率(千瓦)',
    crewamount     Decimal(38, 15) comment '定员（人）',
    selfsupport    Decimal(38, 15) comment '自持力（天）',
    auxpower       string comment '辅机总功率（千瓦）',
    nv             Decimal(20, 6) comment '净值（元）',
    noo            Decimal(10, 2) comment '新旧系数',
    depr           string comment '折旧年限',
    recordno       string comment '船舶登记号',
    mreform        string comment '重大改造',
    captain        string comment '船长',
    captaintel     string comment '船长联系方式',
    ctime          string comment '创建时间',
    mtime          string comment '修改时间',
    note           string comment '备注',
    homeport       string comment '船籍港',
    major_equ      string comment '是否重大装备',
    dist           Decimal(10, 2) comment '闲置装备与登录人的距离',
    start_date     string comment '开始日期',
    etl_time       string comment 'etl_时间',
    source_system  string comment '来源系统',
    source_table   string comment '来源表名'
) comment '装备资源管理_船舶信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_船舶信息
with b as (select *
           from ods.ods_cccc_erms_equip_type_mapping_f_d),
     c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31'),
     a as (select *
           from ods.ods_cccc_erms_base_equ_boatinfo_i_d
           where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_boatinfo_d partition(etl_date = '${etl_date}')
select c.mastercode                                                                                                     as equ_mastercode --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.asid                                                                                                           as asid           --资产ID
     , a.type                                                                                                           as equtype_code   --装备类型编码
     , b.etypename                                                                                                      as equtype_name   --装备类型名称(非直取,请注意查看文档进行调整)
     , a.futype                                                                                                         as futype         --燃料类型
     , a.mmsicode                                                                                                       as mmsicode       --MMSI号码
     , a.sacode                                                                                                         as sacode         --卫星通信号码
     , a.bdcode                                                                                                         as bdcode         --北斗卫星号码
     , a.secode                                                                                                         as secode         --卫星紧急无线电示位标序号
     , a.imocode                                                                                                        as imocode        --IMO编号
     , a.shipcall                                                                                                       as shipcall       --船舶呼号码
     , a.length                                                                                                         as length         --船舶长度（米）
     , a.width                                                                                                          as width          --船舶宽度（米）
     , a.depth                                                                                                          as depth          --船舶型深（米）
     , a.loaddepth                                                                                                      as loaddepth      --满载吃水深度（米）
     , a.loaddisp                                                                                                       as loaddisp       --满载排水量（立方米）
     , a.load                                                                                                           as load           --载重量（吨）
     , a.capacity                                                                                                       as capacity       --舱容（立方米）
     , a.weight                                                                                                         as weight         --净吨/总重量（吨）
     , a.allpower                                                                                                       as allpower       --总装机功率（千瓦）
     , a.narea                                                                                                          as narea          --航区
     , a.speed                                                                                                          as speed          --满载航速（节）
     , a.enginetype                                                                                                     as enginetype     --主机类型
     , a.engineamount                                                                                                   as engineamount   --主机台数
     , a.enginepower                                                                                                    as enginepower    --主机总功率(千瓦)
     , a.crewamount                                                                                                     as crewamount     --定员（人）
     , a.selfsupport                                                                                                    as selfsupport    --自持力（天）
     , a.auxpower                                                                                                       as auxpower       --辅机总功率（千瓦）
     , a.nv                                                                                                             as nv             --净值（元）
     , a.noo                                                                                                            as noo            --新旧系数
     , a.depr                                                                                                           as depr           --折旧年限
     , a.recordno                                                                                                       as recordno       --船舶登记号
     , a.mreform                                                                                                        as mreform        --重大改造
     , a.captain                                                                                                        as captain        --船长
     , a.captaintel                                                                                                     as captaintel     --船长联系方式
     , a.ctime                                                                                                          as ctime          --创建时间
     , a.mtime                                                                                                          as mtime          --修改时间
     , a.note                                                                                                           as note           --备注
     , a.homeport                                                                                                       as homeport       --船籍港
     , a.major_equ                                                                                                      as major_equ      --是否重大装备
     , a.dist                                                                                                           as dist           --闲置装备与登录人的距离
     , a.start_date                                                                                                     as start_date     --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                           as etl_time
     , 'ERMS'                                                                                                           as source_system
     , 'ods_cccc_erms_equip_type_mapping_f_d,ods_cccc_erms_base_pub_assetsinfo_i_d,ods_cccc_erms_base_equ_boatinfo_i_d' as source_table
from a
         left join c on a.asid = c.asid
         left join b on a.type = b.etypeid
;