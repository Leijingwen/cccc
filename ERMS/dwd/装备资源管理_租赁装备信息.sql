--装备资源管理_租赁装备信息
create table dwd.dwd_erms_rent_equinfo_d
(
    equ_mastercode           string comment '装备主数据编码',
    rsid                     string comment '租赁装备id',
    equcode                  string comment '装备编码(赋码后)',
    equ_cnname               string comment '装备中文名称',
    equtype_code             string comment '装备类型编码',
    equtype_name             string comment '装备类型名称',
    fcode                    string comment '出厂编号',
    manufacturer_name        string comment '建造/生产厂家名称',
    exfdate                  string comment '出厂日期',
    model                    string comment '规格型号',
    ovalue                   Decimal(19, 6) comment '原值',
    equstate_code            string comment '装备状态编码',
    equstate_name            string comment '装备状态名称',
    location_code            string comment '所在地编码',
    location_name            string comment '所在地名称',
    ability                  string comment '能力',
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
    rent_unit_oid            string comment '出租单位oid',
    rent_unit_coid           string comment '出租单位coid',
    rent_unit_name           string comment '出租单位名称',
    rent_second_unit_id      string comment '出租二级单位id',
    rent_second_unit_name    string comment '出租二级单位名称',
    rent_third_unit_id       string comment '出租三级单位id',
    rent_third_unit_name     string comment '出租三级单位名称',
    contractcode             string comment '合同编号',
    contractname             string comment '合同名称',
    mmsinumber               string comment 'mmsi号码',
    datasource_name          string comment '数据来源名称',
    codestate_code           string comment '赋码状态编码',
    codestate_name           string comment '赋码状态名称',
    asfile_name              string comment '附件名称',
    equipcode                string comment '装备编码(弃用)',
    rentamountyb             Decimal(19, 6) comment '租赁金额（原币）',
    rentamount               Decimal(19, 6) comment '租赁金额（元）',
    bz_code                  string comment '币种编码',
    bz_name                  string comment '币种名称',
    zproject_code            string comment '主项目数据编码',
    proname                  string comment '所属项目名称',
    rentalebegindate         string comment '租赁开始日期',
    rentaleenddate           string comment '租赁结束日期',
    leaseid                  string comment '租赁申请单id',
    leasename                string comment '租赁申请单名称',
    rentaletype_name         string comment '租赁状态名称',
    islock                   string comment '锁定状态',
    canceldate               string comment '退租日期',
    hosttype                 string comment '主机类型',
    carnumber                string comment '车牌号',
    prodyear                 Decimal(38, 0) comment '生产年限',
    note                     string comment '备注',
    codetime                 string comment '赋码时间',
    ctime                    string comment '创建时间',
    mtime                    string comment '修改时间',
    start_date               string comment '开始日期',
    etl_time                 string comment 'etl_时间',
    source_system            string comment '来源系统',
    source_table             string comment '来源表名'
) comment '装备资源管理_租赁装备信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_租赁装备信息
with mapping as (select *
                 from dwd.dim_erms_equip_type_mapping_d),
     rent_equ as (select *
                  from ods.ods_cccc_erms_master_rentalequip_i_d
                  where end_date = '2999-12-31' and isdelete != '1'),
     org_rule as (select * from dwd.dim_erms_orgext_d),
     dict as (select dname, dicode, diname from dwd.dim_erms_dictitem_d where dname = '币种')
-- insert overwrite table dwd.dwd_erms_rent_equinfo_d partition(etl_date = '${etl_date}')
select NULL                                                                                                       as equ_mastercode           --装备主数据编码(非直取,请注意查看文档进行调整)
     , rent_equ.rsid                                                                                              as rsid                     --租赁装备id
     , rent_equ.equcode                                                                                           as equcode                  --装备编码(赋码后)
     , rent_equ.equipname                                                                                         as equ_cnname               --装备中文名称
     , rent_equ.equiptype                                                                                         as equtype_code             --装备类型编码
     , mapping.equtype_name                                                                                       as equtype_name             --装备类型名称(非直取,请注意查看文档进行调整)
     , rent_equ.fcode                                                                                             as fcode                    --出厂编号
     , rent_equ.manufacturer                                                                                      as manufacturer_name        --建造/生产厂家名称
     , rent_equ.ftime                                                                                             as exfdate                  --出厂日期
     , rent_equ.model                                                                                             as model                    --规格型号
     , rent_equ.ovalue                                                                                            as ovalue                   --原值
     , rent_equ.equipstate                                                                                        as equstate_code            --装备状态编码
     , case
           when rent_equ.equipstate = '00' then '调遣'
           when rent_equ.equipstate = '01' then '施工'
           when rent_equ.equipstate = '02' then '闲置'
           when rent_equ.equipstate = '03' then '修理'
           when rent_equ.equipstate = '04' then '待命'
           when rent_equ.equipstate = '05' then '封存'
           when rent_equ.equipstate = '06' then '改造'
           when rent_equ.equipstate = '08' then '让售'
           when rent_equ.equipstate = '09' then '报废'
    end                                                                                                           as equstate_name            --装备状态名称(非直取,请注意查看文档进行调整)
     , rent_equ.location                                                                                          as location_code            --所在地编码
     , rent_equ.locationname                                                                                      as location_name            --所在地名称
     , rent_equ.ability                                                                                           as ability                  --能力
     , rent_equ.manageid                                                                                          as manage_unit_oid          --管理单位oid
     , con.coid                                                                                                   as manage_unit_coid         --管理单位coid(非直取,请注意查看文档进行调整)
     , rent_equ.managename                                                                                        as manage_unit_name         --管理单位名称
     , con.second_unit_id                                                                                         as manager_second_unit_id   --管理二级单位id(非直取,请注意查看文档进行调整)
     , con.second_unit_name                                                                                       as manager_second_unit_name --管理二级单位名称(非直取,请注意查看文档进行调整)
     , con.third_unit_id                                                                                          as manager_third_unit_id    --管理三级单位id(非直取,请注意查看文档进行调整)
     , con.third_unit_name                                                                                        as manager_third_unit_name  --管理三级单位名称(非直取,请注意查看文档进行调整)
     , rent_equ.useid                                                                                             as use_unit_oid             --使用单位oid
     , use_org.coid                                                                                               as use_unit_coid            --使用单位coid(非直取,请注意查看文档进行调整)
     , rent_equ.usename                                                                                           as use_unit_name            --使用单位名称
     , use_org.second_unit_id                                                                                     as use_second_unit_id       --使用二级单位id(非直取,请注意查看文档进行调整)
     , use_org.second_unit_name                                                                                   as use_second_unit_name     --使用二级单位名称(非直取,请注意查看文档进行调整)
     , use_org.third_unit_id                                                                                      as use_third_unit_id        --使用三级单位id(非直取,请注意查看文档进行调整)
     , use_org.third_unit_name                                                                                    as use_third_unit_name      --使用三级单位名称(非直取,请注意查看文档进行调整)
     , rent_equ.rentoid                                                                                           as rent_unit_oid            --出租单位oid
     , rent.coid                                                                                                  as rent_unit_coid           --出租单位coid(非直取,请注意查看文档进行调整)
     , rent_equ.rentname                                                                                          as rent_unit_name           --出租单位名称
     , rent.second_unit_id                                                                                        as rent_second_unit_id      --出租二级单位id(非直取,请注意查看文档进行调整)
     , rent.second_unit_name                                                                                      as rent_second_unit_name    --出租二级单位名称(非直取,请注意查看文档进行调整)
     , rent.third_unit_id                                                                                         as rent_third_unit_id       --出租三级单位id(非直取,请注意查看文档进行调整)
     , rent.third_unit_name                                                                                       as rent_third_unit_name     --出租三级单位名称(非直取,请注意查看文档进行调整)
     , rent_equ.contractcode                                                                                      as contractcode             --合同编号
     , rent_equ.contractname                                                                                      as contractname             --合同名称
     , rent_equ.mmsinumber                                                                                        as mmsinumber               --mmsi号码
     , rent_equ.datasource                                                                                        as datasource_name          --数据来源名称
     , rent_equ.codestate                                                                                         as codestate_code           --赋码状态编码
     , case
           when rent_equ.codestate = '0' then '未赋码'
           when rent_equ.codestate = '1' then '已赋码'
    end                                                                                                           as codestate_name           --赋码状态名称(非直取,请注意查看文档进行调整)
     , rent_equ.rsfile                                                                                            as asfile_name              --附件名称
     , rent_equ.equipcode                                                                                         as equipcode                --装备编码(弃用)
     , rent_equ.rentamountyb                                                                                      as rentamountyb             --租赁金额（原币）
     , rent_equ.rentamount                                                                                        as rentamount               --租赁金额（元）
     , rent_equ.bz                                                                                                as bz_code                  --币种编码
     , dict.diname                                                                                                as bz_name                  --币种名称(非直取,请注意查看文档进行调整)
     , rent_equ.zproject                                                                                          as zproject_code            --主项目数据编码
     , rent_equ.proname                                                                                           as proname                  --所属项目名称
     , rent_equ.rentalebegintime                                                                                  as rentalebegindate         --租赁开始日期
     , rent_equ.rentaleendtime                                                                                    as rentaleenddate           --租赁结束日期
     , rent_equ.leaseid                                                                                           as leaseid                  --租赁申请单id
     , rent_equ.leasename                                                                                         as leasename                --租赁申请单名称
     , rent_equ.rentaletype                                                                                       as rentaletype_name         --租赁状态名称
     , rent_equ.islock                                                                                            as islock                   --锁定状态
     , rent_equ.canceldate                                                                                        as canceldate               --退租日期
     , rent_equ.hosttype                                                                                          as hosttype                 --主机类型
     , rent_equ.carnumber                                                                                         as carnumber                --车牌号
     , rent_equ.prodyear                                                                                          as prodyear                 --生产年限
     , rent_equ.note                                                                                              as note                     --备注
     , rent_equ.codetime                                                                                          as codetime                 --赋码时间
     , rent_equ.ctime                                                                                             as ctime                    --创建时间
     , rent_equ.mtime                                                                                             as mtime                    --修改时间
     , rent_equ.start_date                                                                                        as start_date               --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                     as etl_time
     , 'ERMS'                                                                                                     as source_system
     , 'dim_erms_equip_type_mapping_d,ods_cccc_erms_master_rentalequip_i_d,dim_erms_orgext_d,dim_erms_dictitem_d' as source_table
from rent_equ
         left join mapping on rent_equ.equiptype = mapping.equtype_code
         left join org_rule con on rent_equ.manageid = con.oid
         left join org_rule use_org on rent_equ.useid = use_org.oid
         left join org_rule rent on rent_equ.rentoid = rent.oid
         left join dict on rent_equ.bz = dict.dicode
;





