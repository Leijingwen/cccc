drop table dwd.dwd_erms_own_equ_pub_assetsinfo_d;

create table dwd.dwd_erms_own_equ_pub_assetsinfo_d
(
    equ_mastercode           string comment '装备主数据编码',
    asid                     string comment '资产id',
    equcode                  string comment '装备编码(赋码后)',
    equ_cnname               string comment '装备中文名称',
    equ_enname               string comment '装备英文名称',
    equtype_code             string comment '装备类型编码',
    equtype_name             string comment '装备类型名称',
    financial_code           string comment '财务编码',
    equ_class_name           string comment '装备分类名称',
    ascode                   string comment '资产编号',
    fcode                    string comment '出厂编号',
    manufacturer_name        string comment '建造/生产厂家名称',
    designunit_name          string comment '设计单位名称',
    buydate                  string comment '购买日期',
    exfdate                  string comment '出厂日期',
    model                    string comment '规格型号',
    ovalue                   Decimal(20, 6) comment '原值',
    ovalueyb                 Decimal(20, 6) comment '原值（原币）',
    nvalue                   Decimal(20, 6) comment '净值',
    nvalueyb                 Decimal(20, 6) comment '净值（原币）',
    equstate_code            string comment '装备状态编码',
    equstate_name            string comment '装备状态名称',
    location_code            string comment '所在地编码',
    location_name            string comment '所在地名称',
    ability                  string comment '能力',
    own_unit_oid             string comment '权属单位oid',
    own_unit_coid            string comment '权属单位coid',
    own_unit_name            string comment '权属单位名称',
    own_second_unit_id       string comment '权属二级单位id',
    own_second_unit_name     string comment '权属二级单位名称',
    own_third_unit_id        string comment '权属三级单位id',
    own_third_unit_name      string comment '权属三级单位名称',
    manage_unit_oid          string comment '管理单位oid',
    manage_unit_coid         string comment '管理单位coid',
    manage_unit_name         string comment '管理单位名称',
    manager_second_unit_id   string comment '管理二级单位id',
    manager_second_unit_name string comment '管理二级单位名称',
    manager_third_unit_id    string comment '管理三级单位id',
    manager_third_unit_name  string comment '管理三级单位名称',
    use_unit_oid             string comment '使用单位oid',
    use_unit_coid            string comment '使用单位coid',
    use_unit_name            string comment '使用单位名称',
    use_second_unit_id       string comment '使用二级单位id',
    use_second_unit_name     string comment '使用二级单位名称',
    use_third_unit_id        string comment '使用三级单位id',
    use_third_unit_name      string comment '使用三级单位名称',
    outuse_unit_oid          string comment '外部使用单位oid',
    outuse_unit_name         string comment '外部使用单位名称',
    evapply_unit_oid         string comment '评价申请单位oid',
    evapply_unit_oname       string comment '评价申请单位名称',
    equpurpose               string comment '装备用途',
    invtype_code             string comment '投资方式编码',
    invtype_name             string comment '投资方式名称',
    meteringunit             string comment '计量单位',
    billcode                 string comment '验收单编号',
    billname                 string comment '验收单名称',
    contractcode             string comment '合同编号',
    contractname             string comment '合同名称',
    iccode                   string comment '检验合格证编号',
    mmsinumber               string comment 'mmsi号码',
    datasource_name          string comment '数据来源名称',
    codestate_code           string comment '赋码状态编码',
    codestate_name           string comment '赋码状态名称',
    asfile_name              string comment '附件名称',
    plid                     string comment '计划ID',
    plname                   string comment '计划名称',
    pappid                   string comment '立项id',
    pappname                 string comment '立项名称',
    ppcid                    string comment '关联立项子表id',
    mainreasons              string comment '主要原因',
    evapplydate              string comment '评价申请日期',
    billstate_code           string comment '单据状态编码',
    billstate_name           string comment '单据状态名称',
    majorequ                 string comment '是否重大',
    endfileid                string comment '最终报告附件id',
    ispush                   string comment '是否推送财务云',
    ysoid                    string comment '预算oid',
    bz_code                  string comment '币种编码',
    bz_name                  string comment '币种名称',
    noo                      Decimal(10, 2) comment '新旧系数',
    note                     string comment '备注',
    pushtime                 string comment '推送时间',
    acceptancetime           string comment '验收时间',
    rettime                  string comment '主数据编码返回时间',
    codetime                 string comment '赋码时间',
    ctime                    string comment '创建时间',
    mtime                    string comment '修改时间',
    ywmtime                  string comment '运维-修改时间',
    start_date               string comment '开始日期',
    etl_time                 string comment 'etl_时间',
    source_system            string comment '来源系统',
    source_table             string comment '来源表名'
) comment '装备资源管理_自有装备公用信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_基础信息_自有装备公用信息
with mapping as (select *
                 from dwd.dim_erms_equip_type_mapping_d),
     pub as (select *
             from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
             where end_date = '2999-12-31'
               and isdelete != '1'),
     org_rule as (select *
                  from dwd.dim_erms_orgext_d),
     dict as (select dname, dicode, diname from dwd.dim_erms_dictitem_d where dname in ('币种', '后评价当前环节'))
-- insert overwrite table dwd.dwd_erms_own_equ_pub_assetsinfo_d partition ( etl_date = '${etl_date}' )
select pub.mastercode                                                                                              as equ_mastercode           --装备主数据编码
     , pub.asid                                                                                                    as asid                     --资产id
     , pub.equcode                                                                                                 as equcode                  --装备编码(赋码后)
     , pub.cnname                                                                                                  as equ_cnname               --装备中文名称
     , pub.enname                                                                                                  as equ_enname               --装备英文名称
     , pub.equtype                                                                                                 as equtype_code             --装备类型编码
     , mapping.equtype_name                                                                                        as equtype_name             --装备类型名称(非直取,请注意查看文档进行调整)
     , pub.financial_code                                                                                          as financial_code           --财务编码
     , pub.type                                                                                                    as equ_class_name           --装备分类名称
     , pub.code                                                                                                    as ascode                   --资产编号
     , pub.fcode                                                                                                   as fcode                    --出厂编号
     , pub.manufacturer                                                                                            as manufacturer_name        --建造/生产厂家名称
     , pub.designunit                                                                                              as designunit_name          --设计单位名称
     , pub.buydate                                                                                                 as buydate                  --购买日期
     , pub.exfdate                                                                                                 as exfdate                  --出厂日期
     , pub.model                                                                                                   as model                    --规格型号
     , pub.ovalue                                                                                                  as ovalue                   --原值
     , pub.ovalueyb                                                                                                as ovalueyb                 --原值（原币）
     , pub.nvalue                                                                                                  as nvalue                   --净值
     , pub.nvalueyb                                                                                                as nvalueyb                 --净值（原币）
     , pub.equstate                                                                                                as equstate_code            --装备状态编码
     , case
           when pub.equstate = '00' then '调遣'
           when pub.equstate = '01' then '施工'
           when pub.equstate = '02' then '闲置'
           when pub.equstate = '03' then '修理'
           when pub.equstate = '04' then '待命'
           when pub.equstate = '05' then '封存'
           when pub.equstate = '06' then '改造'
           when pub.equstate = '08' then '让售'
           when pub.equstate = '09' then '报废'
    end                                                                                                            as equstate_name            --装备状态名称(非直取,请注意查看文档进行调整)
     , pub.location                                                                                                as location_code            --所在地编码
     , pub.LOCATIONNAME                                                                                            as location_name            --所在地名称
     , pub.ability                                                                                                 as ability                  --能力
     , pub.ownoid                                                                                                  as own_unit_oid             --权属单位oid
     , pub.owncoid                                                                                                 as own_unit_coid            --权属单位coid
     , pub.ownname                                                                                                 as own_unit_name            --权属单位名称
     , own.second_unit_id                                                                                          as own_second_unit_id       --权属二级单位id(非直取,请注意查看文档进行调整)
     , own.second_unit_name                                                                                        as own_second_unit_name     --权属二级单位名称(非直取,请注意查看文档进行调整)
     , own.third_unit_id                                                                                           as own_third_unit_id        --权属三级单位id(非直取,请注意查看文档进行调整)
     , own.third_unit_name                                                                                         as own_third_unit_name      --权属三级单位名称(非直取,请注意查看文档进行调整)
     , pub.conoid                                                                                                  as manage_unit_oid          --管理单位oid
     , pub.concoid                                                                                                 as manage_unit_coid         --管理单位coid
     , pub.conname                                                                                                 as manage_unit_name         --管理单位名称
     , con.second_unit_id                                                                                          as manager_second_unit_id   --管理二级单位id(非直取,请注意查看文档进行调整)
     , con.second_unit_name                                                                                        as manager_second_unit_name --管理二级单位名称(非直取,请注意查看文档进行调整)
     , con.third_unit_id                                                                                           as manager_third_unit_id    --管理三级单位id(非直取,请注意查看文档进行调整)
     , con.third_unit_name                                                                                         as manager_third_unit_name  --管理三级单位名称(非直取,请注意查看文档进行调整)
     , pub.useid                                                                                                   as use_unit_oid             --使用单位oid
     , pub.usecoid                                                                                                 as use_unit_coid            --使用单位coid
     , pub.usename                                                                                                 as use_unit_name            --使用单位名称
     , use_org.second_unit_id                                                                                      as use_second_unit_id       --使用二级单位id(非直取,请注意查看文档进行调整)
     , use_org.second_unit_name                                                                                    as use_second_unit_name     --使用二级单位名称(非直取,请注意查看文档进行调整)
     , use_org.third_unit_id                                                                                       as use_third_unit_id        --使用三级单位id(非直取,请注意查看文档进行调整)
     , use_org.third_unit_name                                                                                     as use_third_unit_name      --使用三级单位名称(非直取,请注意查看文档进行调整)
     , pub.outuseid                                                                                                as outuse_unit_oid          --外部使用单位oid
     , pub.outusename                                                                                              as outuse_unit_name         --外部使用单位名称
     , pub.evapplyoid                                                                                              as evapply_unit_oid         --评价申请单位oid
     , pub.evapplyoname                                                                                            as evapply_unit_oname       --评价申请单位名称
     , pub.equpurpose                                                                                              as equpurpose               --装备用途
     , pub.invtype                                                                                                 as invtype_code             --投资方式编码
     , case
           when pub.invtype = '1' then '建造'
           when pub.invtype = '2' then '购买'
           when pub.invtype = '3' then '改造'
    end                                                                                                            as invtype_name             --投资方式名称(非直取,请注意查看文档进行调整)
     , pub.meteringunit                                                                                            as meteringunit             --计量单位
     , pub.billcode                                                                                                as billcode                 --验收单编号
     , pub.billname                                                                                                as billname                 --验收单名称
     , pub.contractcode                                                                                            as contractcode             --合同编号
     , pub.contractname                                                                                            as contractname             --合同名称
     , pub.iccode                                                                                                  as iccode                   --检验合格证编号
     , pub.mmsinumber                                                                                              as mmsinumber               --mmsi号码
     , pub.datasource                                                                                              as datasource_name          --数据来源名称
     , pub.codestate                                                                                               as codestate_code           --赋码状态编码
     , case
           when pub.codestate = '0' then '未赋码'
           when pub.codestate = '1' then '已赋码'
    end                                                                                                            as codestate_name           --赋码状态名称(非直取,请注意查看文档进行调整)
     , pub.asfile                                                                                                  as asfile_name              --附件名称
     , pub.plid                                                                                                    as plid                     --计划ID
     , pub.plname                                                                                                  as plname                   --计划名称
     , pub.pappid                                                                                                  as pappid                   --立项id
     , pub.pappname                                                                                                as pappname                 --立项名称
     , pub.ppcid                                                                                                   as ppcid                    --关联立项子表id
     , pub.mainreasons                                                                                             as mainreasons              --主要原因
     , pub.evapplydate                                                                                             as evapplydate              --评价申请日期
     , pub.billstate                                                                                               as billstate_code           --单据状态编码
     , dict_hpj.diname                                                                                              as billstate_name           --单据状态名称
     , pub.majorequ                                                                                                as majorequ                 --是否重大
     , pub.endfileid                                                                                               as endfileid                --最终报告附件id
     , pub.ispush                                                                                                  as ispush                   --是否推送财务云
     , pub.ysoid                                                                                                   as ysoid                    --预算oid
     , pub.bz                                                                                                      as bz_code                  --币种编码
     , dict_bz.diname                                                                                              as bz_name                  --币种名称
     , pub.noo                                                                                                     as noo                      --新旧系数
     , pub.note                                                                                                    as note                     --备注
     , pub.pushtime                                                                                                as pushtime                 --推送时间
     , pub.acceptancetime                                                                                          as acceptancetime           --验收时间
     , pub.rettime                                                                                                 as rettime                  --主数据编码返回时间
     , pub.codetime                                                                                                as codetime                 --赋码时间
     , pub.ctime                                                                                                   as ctime                    --创建时间
     , pub.mtime                                                                                                   as mtime                    --修改时间
     , pub.ywmtime                                                                                                 as ywmtime                  --运维-修改时间
     , pub.start_date                                                                                              as start_date               --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                      as etl_time
     , 'ERMS'                                                                                                      as source_system
     , 'dim_erms_equip_type_mapping_d,ods_cccc_erms_base_pub_assetsinfo_i_d,dim_erms_orgext_d,dim_erms_dictitem_d' as source_table
from pub
         left join mapping on pub.equtype = mapping.equtype_code
         left join org_rule own on pub.ownoid = own.oid
         left join org_rule con on pub.conoid = con.oid
         left join org_rule use_org on pub.useid = use_org.oid
         left join dict dict_bz on pub.bz = dict_bz.dicode and dict_bz.dname = '币种'
         left join dict dict_hpj on pub.billstate = dict_hpj.dicode and dict_hpj.dname = '后评价当前环节'
;