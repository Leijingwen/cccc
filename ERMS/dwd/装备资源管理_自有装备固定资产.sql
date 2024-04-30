--装备资源管理_自有装备固定资产
create table dwd.dwd_erms_own_fixed_assets_d
(
    asid               string comment '资产ID',
    equ_mastercode     string comment '装备主数据编码',
    con_no             string comment '合同编号',
    con_name           string comment '合同名称',
    push_time          string comment '推送时间',
    acc_no             string comment '验收单编号',
    acc_name           string comment '验收单名称',
    con_time           string comment '转固时间',
    cod_time           string comment '赋转时间',
    order_sn           string comment '订单号',
    invoce_no          string comment '发票号',
    pur_date           string comment '购入日期',
    acc_id             string comment '验收部门',
    acceptor           string comment '验收人',
    pre_name           string comment '制单人',
    res_date           string comment '登记日期',
    vou_date           string comment '凭证生成日期',
    use_date           string comment '使用日期',
    cur_year_depre     string comment '入账当年折旧',
    net_res_value      Decimal(38, 0) comment '净残值',
    net_res_rate       string comment '净残值率（%）',
    rec_month          string comment '已入账月份',
    mea_unit           string comment '计量单位',
    pro_month          string comment '计提延期月份',
    acc_month          string comment '新增当月计提',
    acc_value          Decimal(38, 0) comment '计提残值',
    dep_method_code    string comment '折旧方法（摊销）编码',
    dep_method_name    string comment '折旧方法（摊销）名称',
    ann_dep            Decimal(38, 0) comment '年折旧（摊销）额',
    mon_dep            Decimal(38, 0) comment '月折旧（摊销）额',
    pro_imp            string comment '减值准备',
    rec_dep            string comment '入账折旧（摊销）',
    ann_dep_rate       string comment '年折旧（摊销）率（%）',
    mon_dep_rate       string comment '月折旧（摊销）率（%）',
    dep_per            string comment '折旧（摊销）周期',
    mon_rai            string comment '已提月份',
    in_de_month        string comment '增减月份',
    lo_ter_exp         Decimal(38, 0) comment '长期待摊费用',
    ctime              string comment '创建时间',
    mtime              string comment '修改时间',
    impair_can_still   Decimal(38, 0) comment '减值后尚可使用月份',
    cumu_impair_adjust Decimal(38, 0) comment '减值调整时累计折旧（摊销）',
    acceler_depre      string comment '加速折旧（摊销）原因(code)',
    mateaccount        string comment '折旧(摊销)对应科目',
    usebooks           string comment '使用账簿',
    startdate          string comment '启用日期',
    durableyears       string comment '使用年限',
    asscount           Decimal(38, 15) comment '资产数量',
    assused            string comment '资产用途',
    assno              string comment '资产编号',
    equtype_code       string comment '装备类型编码',
    equtype_name       string comment '装备类型名称',
    state_code         string comment '资产状态编码',
    datasource_code    string comment '资产来源编码',
    mateaccountname    string comment '折旧(摊销)对应科目(name)',
    owndepartment_oid  string comment '所属部门oid',
    owndepartment_coid string comment '所属部门coid',
    owndepartment_name string comment '所属部门公司简称',
    usedepartment_oid  string comment '使用部门oid',
    usedepartment_coid string comment '使用部门coid',
    usedepartment_name string comment '使用部门公司简称',
    start_date         string comment '开始日期',
    etl_time           string comment 'etl_时间',
    source_system      string comment '来源系统',
    source_table       string comment '来源表名'
) comment '装备资源管理_自有装备固定资产'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_自有装备固定资产
with b as (select *
           from stg.stg_cccc_erms_waf_core_dictitem_f_d
           where did = '16613894589102'),
     c as (select *
           from dwd.dim_erms_equip_type_mapping_d),
     a as (select *
           from ods.ods_cccc_erms_own_fixed_assets_i_d
           where end_date = '2999-12-31'),
     d as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_own_fixed_assets_d partition(etl_date = '${etl_date}')
select a.asid                                                                                                                                       as asid               --资产ID
     , d.mastercode                                                                                                                                 as equ_mastercode     --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.con_no                                                                                                                                     as con_no             --合同编号
     , a.con_name                                                                                                                                   as con_name           --合同名称
     , a.push_time                                                                                                                                  as push_time          --推送时间
     , a.acc_no                                                                                                                                     as acc_no             --验收单编号
     , a.acc_name                                                                                                                                   as acc_name           --验收单名称
     , a.con_time                                                                                                                                   as con_time           --转固时间
     , a.cod_time                                                                                                                                   as cod_time           --赋转时间
     , a.order_sn                                                                                                                                   as order_sn           --订单号
     , a.invoce_no                                                                                                                                  as invoce_no          --发票号
     , a.pur_date                                                                                                                                   as pur_date           --购入日期
     , a.acc_id                                                                                                                                     as acc_id             --验收部门
     , a.acceptor                                                                                                                                   as acceptor           --验收人
     , a.pre_name                                                                                                                                   as pre_name           --制单人
     , a.res_date                                                                                                                                   as res_date           --登记日期
     , a.vou_date                                                                                                                                   as vou_date           --凭证生成日期
     , a.use_date                                                                                                                                   as use_date           --使用日期
     , a.cur_year_depre                                                                                                                             as cur_year_depre     --入账当年折旧
     , a.net_res_value                                                                                                                              as net_res_value      --净残值
     , a.net_res_rate                                                                                                                               as net_res_rate       --净残值率（%）
     , a.rec_month                                                                                                                                  as rec_month          --已入账月份
     , a.mea_unit                                                                                                                                   as mea_unit           --计量单位
     , a.pro_month                                                                                                                                  as pro_month          --计提延期月份
     , a.acc_month                                                                                                                                  as acc_month          --新增当月计提
     , a.acc_value                                                                                                                                  as acc_value          --计提残值
     , a.dep_method                                                                                                                                 as dep_method_code    --折旧方法（摊销）编码
     , b.name                                                                                                                                       as dep_method_name    --折旧方法（摊销）名称(非直取,请注意查看文档进行调整)
     , a.ann_dep                                                                                                                                    as ann_dep            --年折旧（摊销）额
     , a.mon_dep                                                                                                                                    as mon_dep            --月折旧（摊销）额
     , a.pro_imp                                                                                                                                    as pro_imp            --减值准备
     , a.rec_dep                                                                                                                                    as rec_dep            --入账折旧（摊销）
     , a.ann_dep_rate                                                                                                                               as ann_dep_rate       --年折旧（摊销）率（%）
     , a.mon_dep_rate                                                                                                                               as mon_dep_rate       --月折旧（摊销）率（%）
     , a.dep_per                                                                                                                                    as dep_per            --折旧（摊销）周期
     , a.mon_rai                                                                                                                                    as mon_rai            --已提月份
     , a.in_de_month                                                                                                                                as in_de_month        --增减月份
     , a.lo_ter_exp                                                                                                                                 as lo_ter_exp         --长期待摊费用
     , a.ctime                                                                                                                                      as ctime              --创建时间
     , a.mtime                                                                                                                                      as mtime              --修改时间
     , a.impair_can_still                                                                                                                           as impair_can_still   --减值后尚可使用月份
     , a.cumu_impair_adjust                                                                                                                         as cumu_impair_adjust --减值调整时累计折旧（摊销）
     , a.acceler_depre                                                                                                                              as acceler_depre      --加速折旧（摊销）原因(code)
     , a.mateaccount                                                                                                                                as mateaccount        --折旧(摊销)对应科目
     , a.usebooks                                                                                                                                   as usebooks           --使用账簿
     , a.startdate                                                                                                                                  as startdate          --启用日期
     , a.durableyears                                                                                                                               as durableyears       --使用年限
     , a.asscount                                                                                                                                   as asscount           --资产数量
     , a.assused                                                                                                                                    as assused            --资产用途
     , a.equnum                                                                                                                                     as assno              --资产编号
     , a.type                                                                                                                                       as equtype_code       --装备类型编码
     , c.equtype_name                                                                                                                               as equtype_name       --装备类型名称(非直取,请注意查看文档进行调整)
     , a.state                                                                                                                                      as state_code         --资产状态编码
     , a.datasource                                                                                                                                 as datasource_code    --资产来源编码
     , a.mateaccountname                                                                                                                            as mateaccountname    --折旧(摊销)对应科目(name)
     , a.owndepartment                                                                                                                              as owndepartment_oid  --所属部门oid
     , a.owndepartmentcoid                                                                                                                          as owndepartment_coid --所属部门coid
     , a.owndepartmentname                                                                                                                          as owndepartment_name --所属部门公司简称
     , a.usedepartment                                                                                                                              as usedepartment_oid  --使用部门oid
     , a.usedepartmentcoid                                                                                                                          as usedepartment_coid --使用部门coid
     , a.usedepartmentname                                                                                                                          as usedepartment_name --使用部门公司简称
     , a.start_date                                                                                                                                 as start_date         --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                                                       as etl_time
     , 'ERMS'                                                                                                                                       as source_system
     , 'stg_cccc_erms_waf_core_dictitem_f_d,dim_erms_equip_type_mapping_d,ods_cccc_erms_own_fixed_assets_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d' as source_table
from a
         left join b on a.dep_method = b.code
         left join c on a.type = c.equtype_code
         left join d on a.asid = d.asid
;