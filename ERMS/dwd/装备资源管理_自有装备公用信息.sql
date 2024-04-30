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
with t1 as (select *
             from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
             where end_date = '2999-12-31'
               and isdelete != '1'),
    mapping as (select *
                 from dwd.dim_erms_equip_type_mapping_d),
     org as (select oid
                  , oname
                  , second_unit_id
                  , second_unit_name
                  , third_unit_id
                  , third_unit_name
             from dwd.dim_erms_orgext_d),
     dict as (select dname, dicode, diname from dwd.dim_erms_dictitem_d where dname in ('币种', '后评价当前环节'))
-- insert overwrite table dwd.dwd_erms_own_equ_pub_assetsinfo_d partition ( etl_date = '${etl_date}' )
select t1.mastercode                                                                                              as equ_mastercode           --装备主数据编码
     , t1.asid                                                                                                    as asid                     --资产id
     , t1.equcode                                                                                                 as equcode                  --装备编码(赋码后)
     , t1.cnname                                                                                                  as equ_cnname               --装备中文名称
     , t1.enname                                                                                                  as equ_enname               --装备英文名称
     , t1.equtype                                                                                                 as equtype_code             --装备类型编码
     , mapping.equtype_name                                                                                        as equtype_name             --装备类型名称(非直取,请注意查看文档进行调整)
     , t1.financial_code                                                                                          as financial_code           --财务编码
     , t1.type                                                                                                    as equ_class_name           --装备分类名称
     , t1.code                                                                                                    as ascode                   --资产编号
     , t1.fcode                                                                                                   as fcode                    --出厂编号
     , t1.manufacturer                                                                                            as manufacturer_name        --建造/生产厂家名称
     , t1.designunit                                                                                              as designunit_name          --设计单位名称
     , t1.buydate                                                                                                 as buydate                  --购买日期
     , t1.exfdate                                                                                                 as exfdate                  --出厂日期
     , t1.model                                                                                                   as model                    --规格型号
     , t1.ovalue                                                                                                  as ovalue                   --原值
     , t1.ovalueyb                                                                                                as ovalueyb                 --原值（原币）
     , t1.nvalue                                                                                                  as nvalue                   --净值
     , t1.nvalueyb                                                                                                as nvalueyb                 --净值（原币）
     , t1.equstate                                                                                                as equstate_code            --装备状态编码
     , case
           when t1.equstate = '00' then '调遣'
           when t1.equstate = '01' then '施工'
           when t1.equstate = '02' then '闲置'
           when t1.equstate = '03' then '修理'
           when t1.equstate = '04' then '待命'
           when t1.equstate = '05' then '封存'
           when t1.equstate = '06' then '改造'
           when t1.equstate = '08' then '让售'
           when t1.equstate = '09' then '报废'
    end                                                                                                            as equstate_name            --装备状态名称(非直取,请注意查看文档进行调整)
     , t1.location                                                                                                as location_code            --所在地编码
     , t1.LOCATIONNAME                                                                                            as location_name            --所在地名称
     , t1.ability                                                                                                 as ability                  --能力
     , t1.ownoid                                                                                                  as own_unit_oid             --权属单位oid
     , t1.owncoid                                                                                                 as own_unit_coid            --权属单位coid
     , t1.ownname                                                                                                 as own_unit_name            --权属单位名称
     , own.second_unit_id                                                                                          as own_second_unit_id       --权属二级单位id(非直取,请注意查看文档进行调整)
     , own.second_unit_name                                                                                        as own_second_unit_name     --权属二级单位名称(非直取,请注意查看文档进行调整)
     , own.third_unit_id                                                                                           as own_third_unit_id        --权属三级单位id(非直取,请注意查看文档进行调整)
     , own.third_unit_name                                                                                         as own_third_unit_name      --权属三级单位名称(非直取,请注意查看文档进行调整)
     , t1.conoid                                                                                                  as manage_unit_oid          --管理单位oid
     , t1.concoid                                                                                                 as manage_unit_coid         --管理单位coid
     , t1.conname                                                                                                 as manage_unit_name         --管理单位名称
     , con.second_unit_id                                                                                          as manager_second_unit_id   --管理二级单位id(非直取,请注意查看文档进行调整)
     , con.second_unit_name                                                                                        as manager_second_unit_name --管理二级单位名称(非直取,请注意查看文档进行调整)
     , con.third_unit_id                                                                                           as manager_third_unit_id    --管理三级单位id(非直取,请注意查看文档进行调整)
     , con.third_unit_name                                                                                         as manager_third_unit_name  --管理三级单位名称(非直取,请注意查看文档进行调整)
     , t1.useid                                                                                                   as use_unit_oid             --使用单位oid
     , t1.usecoid                                                                                                 as use_unit_coid            --使用单位coid
     , t1.usename                                                                                                 as use_unit_name            --使用单位名称
     , use_org.second_unit_id                                                                                      as use_second_unit_id       --使用二级单位id(非直取,请注意查看文档进行调整)
     , use_org.second_unit_name                                                                                    as use_second_unit_name     --使用二级单位名称(非直取,请注意查看文档进行调整)
     , use_org.third_unit_id                                                                                       as use_third_unit_id        --使用三级单位id(非直取,请注意查看文档进行调整)
     , use_org.third_unit_name                                                                                     as use_third_unit_name      --使用三级单位名称(非直取,请注意查看文档进行调整)
     , t1.outuseid                                                                                                as outuse_unit_oid          --外部使用单位oid
     , t1.outusename                                                                                              as outuse_unit_name         --外部使用单位名称
     , t1.evapplyoid                                                                                              as evapply_unit_oid         --评价申请单位oid
     , t1.evapplyoname                                                                                            as evapply_unit_oname       --评价申请单位名称
     , t1.equpurpose                                                                                              as equpurpose               --装备用途
     , t1.invtype                                                                                                 as invtype_code             --投资方式编码
     , case
           when t1.invtype = '1' then '建造'
           when t1.invtype = '2' then '购买'
           when t1.invtype = '3' then '改造'
    end                                                                                                            as invtype_name             --投资方式名称(非直取,请注意查看文档进行调整)
     , t1.meteringunit                                                                                            as meteringunit             --计量单位
     , t1.billcode                                                                                                as billcode                 --验收单编号
     , t1.billname                                                                                                as billname                 --验收单名称
     , t1.contractcode                                                                                            as contractcode             --合同编号
     , t1.contractname                                                                                            as contractname             --合同名称
     , t1.iccode                                                                                                  as iccode                   --检验合格证编号
     , t1.mmsinumber                                                                                              as mmsinumber               --mmsi号码
     , t1.datasource                                                                                              as datasource_name          --数据来源名称
     , t1.codestate                                                                                               as codestate_code           --赋码状态编码
     , case
           when t1.codestate = '0' then '未赋码'
           when t1.codestate = '1' then '已赋码'
    end                                                                                                            as codestate_name           --赋码状态名称(非直取,请注意查看文档进行调整)
     , t1.asfile                                                                                                  as asfile_name              --附件名称
     , t1.plid                                                                                                    as plid                     --计划ID
     , t1.plname                                                                                                  as plname                   --计划名称
     , t1.pappid                                                                                                  as pappid                   --立项id
     , t1.pappname                                                                                                as pappname                 --立项名称
     , t1.ppcid                                                                                                   as ppcid                    --关联立项子表id
     , t1.mainreasons                                                                                             as mainreasons              --主要原因
     , t1.evapplydate                                                                                             as evapplydate              --评价申请日期
     , t1.billstate                                                                                               as billstate_code           --单据状态编码
     , hpj.diname                                                                                                  as billstate_name           --单据状态名称
     , t1.majorequ                                                                                                as majorequ                 --是否重大
     , t1.endfileid                                                                                               as endfileid                --最终报告附件id
     , t1.ispush                                                                                                  as ispush                   --是否推送财务云
     , t1.ysoid                                                                                                   as ysoid                    --预算oid
     , t1.bz                                                                                                      as bz_code                  --币种编码
     , bz.diname                                                                                                   as bz_name                  --币种名称
     , t1.noo                                                                                                     as noo                      --新旧系数
     , t1.note                                                                                                    as note                     --备注
     , t1.pushtime                                                                                                as pushtime                 --推送时间
     , t1.acceptancetime                                                                                          as acceptancetime           --验收时间
     , t1.rettime                                                                                                 as rettime                  --主数据编码返回时间
     , t1.codetime                                                                                                as codetime                 --赋码时间
     , t1.ctime                                                                                                   as ctime                    --创建时间
     , t1.mtime                                                                                                   as mtime                    --修改时间
     , t1.ywmtime                                                                                                 as ywmtime                  --运维-修改时间
     , t1.start_date                                                                                              as start_date               --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                      as etl_time
     , 'ERMS'                                                                                                      as source_system
     , 'ods_cccc_erms_base_pub_assetsinfo_i_d,dim_erms_equip_type_mapping_d,dim_erms_orgext_d,dim_erms_dictitem_d' as source_table
from t1
         left join mapping on t1.equtype = mapping.equtype_code
         left join org own on t1.ownoid = own.oid
         left join org con on t1.conoid = con.oid
         left join org use_org on t1.useid = use_org.oid
         left join dict bz on t1.bz = bz.dicode and bz.dname = '币种'
         left join dict hpj on t1.billstate = hpj.dicode and hpj.dname = '后评价当前环节'
;