--装备资源管理_投资计划申报
create table dwd.dwd_erms_invm_plan_year_apply_d
(
    apid            string comment '申报id',
    apname          string comment '申报名称',
    apyear          string comment '申报年度',
    attid_ref_word  string comment '附件_编制说明_WORD',
    attid_ref_pdf   string comment '附件_编制说明_PDF',
    own_unit_oid    string comment '所属机构oid',
    own_unit_name   string comment '所属机构名称',
    state_code      string comment '流程状态编码',
    state_name      string comment '流程状态名称',
    ctime           string comment '创建时间',
    mtime           string comment '修改时间',
    note            string comment '备注',
    utime           string comment '提交到上级时间',
    procinstid      string comment '流程ID',
    yeartype_code   string comment '年度类型编码',
    yeartype_name   string comment '年度类型名称',
    bpmsstatus_code string comment '审批状态编码',
    bpmsstatus_name string comment '审批状态名称',
    canpush         string comment '是否可以推送',
    fixoid          string comment '核算单位id',
    ptime           string comment '全面预算到上级时间',
    pfname          string comment '批复名称',
    pfzwid          string comment '批复正文id',
    pfinfo          string comment '批复意见',
    start_date      string comment '开始日期',
    etl_time        string comment 'etl_时间',
    source_system   string comment '来源系统',
    source_table    string comment '来源表名'
) comment '装备资源管理_投资计划申报'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;



--装备资源管理_投资计划申报
with org_rule as (select *
                  from dwd.dim_erms_orgext_d),
     a as (select *
           from ods.ods_cccc_erms_invm_plan_year_apply_i_d
           where end_date = '2999-12-31'
             and isdelete != '1'),
     dict as (select dcode, dname, dicode, diname
              from dwd.dim_erms_dictitem_d
              where dname in ('审批结果', '投资年度类型'))
--insert overwrite table dwd.dwd_erms_invm_plan_year_apply_d partition(etl_date = '${etl_date}')
select a.apid                                                                         as apid            --申报id
     , a.name                                                                         as apname          --申报名称
     , a.year                                                                         as apyear          --申报年度
     , a.attid_ref_word                                                               as attid_ref_word  --附件_编制说明_WORD
     , a.attid_ref_pdf                                                                as attid_ref_pdf   --附件_编制说明_PDF
     , a.oid                                                                          as own_unit_oid    --所属机构oid
     , org_rule.oname                                                                 as own_unit_name   --所属机构名称(非直取,请注意查看文档进行调整)
     , a.state                                                                        as state_code      --流程状态编码
     , dict_lczt.diname                                                               as state_name      --流程状态名称(非直取,请注意查看文档进行调整)
     , a.ctime                                                                        as ctime           --创建时间
     , a.mtime                                                                        as mtime           --修改时间
     , a.note                                                                         as note            --备注
     , a.utime                                                                        as utime           --提交到上级时间
     , a.procinstid                                                                   as procinstid      --流程ID
     , a.yeartype                                                                     as yeartype_code   --年度类型编码
     , dict_nd.diname                                                                 as yeartype_name   --年度类型名称(非直取,请注意查看文档进行调整)
     , a.bpmsstatus                                                                   as bpmsstatus_code --审批状态编码
     , dict_sp.diname                                                                 as bpmsstatus_name --审批状态名称(非直取,请注意查看文档进行调整)
     , a.canpush                                                                      as canpush         --是否可以推送
     , a.fixoid                                                                       as fixoid          --核算单位id
     , a.ptime                                                                        as ptime           --全面预算到上级时间
     , a.pfname                                                                       as pfname          --批复名称
     , a.pfzwid                                                                       as pfzwid          --批复正文id
     , a.pfinfo                                                                       as pfinfo          --批复意见
     , a.start_date                                                                   as start_date      --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                         as etl_time
     , 'ERMS'                                                                         as source_system
     , 'dim_erms_orgext_d,dim_erms_dictitem_d,ods_cccc_erms_invm_plan_year_apply_i_d' as source_table
from a
         left join org_rule on a.oid = org_rule.oid
         left join dict dict_lczt on a.state = dict_lczt.dicode and dict_lczt.dname = '审批结果'
         left join dict dict_nd on a.yeartype = dict_nd.dicode and dict_nd.dname = '投资年度类型'
         left join dict dict_sp on a.bpmsstatus = dict_sp.dicode and dict_sp.dname = '审批结果'
;
