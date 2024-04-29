-- 装备资源管理_组织规则扩展表
create table dwd.dim_erms_orgext_d
(
    oid              STRING comment '机构id',
    poid             STRING comment '父机构id',
    coid             STRING comment '隶属单位id',
    orule            STRING comment '行政组织规则码',
    sno              decimal(38, 0) comment '节点序号',
    type             STRING comment '机构类型',
    typeext          STRING comment '组织属性',
    goid             STRING comment '分组所属的实体机构id',
    gpoid            STRING comment '分组所属的实体父机构id',
    grule            STRING comment '分组所属的实体机构规则码',
    mrut             STRING comment '最近更新时间',
    oper             STRING comment '操作员',
    note             STRING comment '备注',
    oname            STRING comment '单位名称',
    second_unit_id   STRING comment '二级单位id',
    second_unit_name STRING comment '二级单位名称',
    third_unit_id    STRING comment '三级单位id',
    third_unit_name  STRING comment '三级单位名称',
    etl_time         STRING comment 'etl_时间',
    source_system    STRING comment '来源系统',
    source_table     STRING comment '来源表名'
) comment '装备资源管理_组织规则扩展表'
    STORED AS ORC;



insert overwrite table dwd.dim_erms_orgext_d
select org_rule.oid                                                        as oid,              --机构id
       org_rule.poid                                                       as poid,             --父机构id
       org_rule.coid                                                       as coid,             --隶属单位id
       org_rule.orule                                                      as orule,            --行政组织规则码
       org_rule.sno                                                        as sno,              --节点序号
       org_rule.type                                                       as type,             --机构类型
       org_rule.typeext                                                    as typeext,          --组织属性
       org_rule.goid                                                       as goid,             --分组所属的实体机构id
       org_rule.gpoid                                                      as gpoid,            --分组所属的实体父机构id
       org_rule.grule                                                      as grule,            --分组所属的实体机构规则码
       org_rule.mrut                                                       as mrut,             --最近更新时间
       org_rule.oper                                                       as oper,             --操作员
       org_rule.note                                                       as note,             --备注
       org.name                                                            as oname,            --单位名称
       org_rule.secend_unit_id                                             as second_unit_id,   --二级单位id
       org2.name                                                           as second_unit_name, --二级单位名称
       org_rule.third_unit_id                                              as third_unit_id,    --三级单位id
       org3.name                                                           as third_unit_name,  --三级单位名称
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')              as etl_time,
       'ERMS'                                                              as source_system,
       'ods_cccc_erms_waf_ac_organ2biz_f_d,ods_cccc_erms_waf_ac_organ_f_d' as source_table
from (select oid,
             poid,
             gpoid,
             goid,
             coid,
             orule,
             sno,
             type,
             typeext,
             mrut,
             oper,
             note,
             grule,
             if(split(orule, '-')[1] = '104396', split(orule, '-')[2], split(orule, '-')[1]) as secend_unit_id,
             if(split(orule, '-')[1] = '104396', split(orule, '-')[3], split(orule, '-')[2]) as third_unit_id
      from ods.ods_cccc_erms_waf_ac_organ2biz_f_d) org_rule
         left join ods.ods_cccc_erms_waf_ac_organ_f_d org
                   on org_rule.oid = org.oid
         left join ods.ods_cccc_erms_waf_ac_organ_f_d org2
                   on org_rule.secend_unit_id = org2.oid
         left join ods.ods_cccc_erms_waf_ac_organ_f_d org3
                   on org_rule.third_unit_id = org3.oid;



