--装备资源管理_分包装备信息

create table dwd.dwd_erms_subpackage_equinfo_d
(
    equ_mastercode           string comment '装备主数据编码',
    subpak_id                string comment '分包装备id',
    equcode                  string comment '装备编码(赋码后)',
    equ_cnname               string comment '装备中文名称',
    equtype_code             string comment '装备类型编码',
    equtype_name             string comment '装备类型名称',
    model                    string comment '规格型号',
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
    opp_unit_oid             string comment '分包单位oid',
    opp_unit_coid            string comment '分包单位coid',
    opp_unit_name            string comment '分包单位名称',
    opp_second_unit_id       string comment '分包二级单位id',
    opp_second_unit_name     string comment '分包二级单位名称',
    opp_third_unit_id        string comment '分包三级单位id',
    opp_third_unit_name      string comment '分包三级单位名称',
    zproject_code            string comment '主数据项目编码',
    proname                  string comment '所属项目名称',
    exitid                   string comment '进退场信息id',
    accepdate                string comment '进场日期',
    exitdate                 string comment '退场日期',
    exitstate_code           string comment '进退场状态编码',
    exitstate_name           string comment '进退场状态名称',
    ctime                    string comment '创建时间',
    mtime                    string comment '修改时间',
    start_date               string comment '开始日期',
    etl_time                 string comment 'etl_时间',
    source_system            string comment '来源系统',
    source_table             string comment '来源表名'
) comment '装备资源管理_分包装备信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_分包装备信息
with equ as (select *
             from ods.ods_cccc_erms_base_equ_subpack_i_d
             where end_date = '2999-12-31'
               and isdelete != '1'),
     org_rule as (select * from dwd.dim_erms_orgext_d)
-- insert overwrite table dwd.dwd_erms_subpackage_equinfo_d partition(etl_date = '${etl_date}')
select NULL                                                   as equ_mastercode           --装备主数据编码(非直取,请注意查看文档进行调整)
     , equ.id                                                 as subpak_id                --分包装备id
     , equ.equcode                                            as equcode                  --装备编码(赋码后)
     , equ.equname                                            as equ_cnname               --装备中文名称
     , equ.equtype                                            as equtype_code             --装备类型编码
     , equ.equtypename                                        as equtype_name             --装备类型名称
     , equ.model                                              as model                    --规格型号
     , equ.ability                                            as ability                  --能力
     , equ.oid                                                as own_unit_oid             --权属单位oid
     , own.coid                                               as own_unit_coid            --权属单位coid(非直取,请注意查看文档进行调整)
     , own.oname                                              as own_unit_name            --权属单位名称(非直取,请注意查看文档进行调整)
     , own.second_unit_id                                     as own_second_unit_id       --权属二级单位id(非直取,请注意查看文档进行调整)
     , own.second_unit_name                                   as own_second_unit_name     --权属二级单位名称(非直取,请注意查看文档进行调整)
     , own.third_unit_id                                      as own_third_unit_id        --权属三级单位id(非直取,请注意查看文档进行调整)
     , own.third_unit_name                                    as own_third_unit_name      --权属三级单位名称(非直取,请注意查看文档进行调整)
     , equ.manaunit                                           as manage_unit_oid          --管理单位oid
     , con.coid                                               as manage_unit_coid         --管理单位coid(非直取,请注意查看文档进行调整)
     , equ.manaunitname                                       as manage_unit_name         --管理单位名称
     , con.second_unit_id                                     as manager_second_unit_id   --管理二级单位id(非直取,请注意查看文档进行调整)
     , con.second_unit_name                                   as manager_second_unit_name --管理二级单位名称(非直取,请注意查看文档进行调整)
     , con.third_unit_id                                      as manager_third_unit_id    --管理三级单位id(非直取,请注意查看文档进行调整)
     , con.third_unit_name                                    as manager_third_unit_name  --管理三级单位名称(非直取,请注意查看文档进行调整)
     , equ.oppunit                                            as opp_unit_oid             --分包单位oid
     , pack.coid                                              as opp_unit_coid            --分包单位coid
     , equ.oppunitname                                        as opp_unit_name            --分包单位名称(非直取,请注意查看文档进行调整)
     , pack.second_unit_id                                    as opp_second_unit_id       --分包二级单位id(非直取,请注意查看文档进行调整)
     , pack.second_unit_name                                  as opp_second_unit_name     --分包二级单位名称(非直取,请注意查看文档进行调整)
     , pack.third_unit_id                                     as opp_third_unit_id        --分包三级单位id(非直取,请注意查看文档进行调整)
     , pack.third_unit_name                                   as opp_third_unit_name      --分包三级单位名称(非直取,请注意查看文档进行调整)
     , equ.zproject                                           as zproject_code            --主数据项目编码
     , equ.project                                            as proname                  --所属项目名称
     , equ.exitid                                             as exitid                   --进退场信息id
     , equ.accepdate                                          as accepdate                --进场日期
     , equ.exitdate                                           as exitdate                 --退场日期
     , equ.exitstate                                          as exitstate_code           --进退场状态编码
     , case
           when equ.exitstate = '02' then '已进场'
           when equ.exitstate = '03' then '已退场'
    end                                                       as exitstate_name           --进退场状态名称(非直取,请注意查看文档进行调整)
     , equ.ctime                                              as ctime                    --创建时间
     , equ.mtime                                              as mtime                    --修改时间
     , equ.start_date                                         as start_date               --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_deploy_apply_equ_i_d'                   as source_table
from equ
         left join org_rule own on equ.oid = own.oid
         left join org_rule con on equ.manaunit = con.oid
         left join org_rule pack on equ.oppunit = pack.oid
;
