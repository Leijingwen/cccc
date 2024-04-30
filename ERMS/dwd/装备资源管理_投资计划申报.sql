--装备资源管理_投资计划申报
DROP TABLE dwd.dwd_erms_invm_plan_year_apply_d;
create table dwd.dwd_erms_invm_plan_year_apply_d
(
    apid                string comment '申报id',
    apname              string comment '申报名称',
    apyear              string comment '申报年度',
    attid_ref_word      string comment '附件_编制说明_WORD',
    attid_ref_pdf       string comment '附件_编制说明_PDF',
    own_unit_id         string comment '所属机构id',
    own_unit_name       string comment '所属机构名称',
    process_state_code  string comment '流程状态编码',
    process_state_name  string comment '流程状态名称',
    ctime               string comment '创建时间',
    mtime               string comment '修改时间',
    note                string comment '备注',
    utime               string comment '提交到上级时间',
    procinstid          string comment '流程ID',
    yeartype_code       string comment '年度类型编码',
    yeartype_name       string comment '年度类型名称',
    bpmsstatus_code     string comment '审批状态编码',
    bpmsstatus_name     string comment '审批状态名称',
    canpush             string comment '是否可以推送',
    calculate_unit_id   string comment '核算单位id',
    calculate_unit_name string comment '核算单位名称',
    ptime               string comment '全面预算到上级时间',
    pfname              string comment '批复名称',
    pfzwid              string comment '批复正文id',
    pfinfo              string comment '批复意见',
    start_date          string comment '开始日期',
    etl_time            string comment 'etl_时间',
    source_system       string comment '来源系统',
    source_table        string comment '来源表名'
) comment '装备资源管理_投资计划申报'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资计划申报
with t1 as (select *
            from ods.ods_cccc_erms_invm_plan_year_apply_i_d
            where end_date = '2999-12-31'
              and isdelete != '1'),
     org as (select oid
                  , oname
                  , second_unit_id
                  , second_unit_name
                  , third_unit_id
                  , third_unit_name
             from dwd.dim_erms_orgext_d),
     dict as (select dcode
                   , dname
                   , dicode
                   , diname
              from dwd.dim_erms_dictitem_d
              where dname in ('审批结果', '投资年度类型')),
     cal as (select zaco
                  , zacorgno
                  , zacname_chs
                  , zacname_en
                  , zacid
             from ods.ods_cccc_erms_zmdgs_actorginf_f_d)
--insert overwrite table dwd.dwd_erms_invm_plan_year_apply_d partition(etl_date = '${etl_date}')
select t1.apid                                                                        as apid                --申报id
     , t1.name                                                                        as apname              --申报名称
     , t1.year                                                                        as apyear              --申报年度
     , t1.attid_ref_word                                                              as attid_ref_word      --附件_编制说明_WORD
     , t1.attid_ref_pdf                                                               as attid_ref_pdf       --附件_编制说明_PDF
     , t1.oid                                                                         as own_unit_id         --所属机构id
     , t2.oname                                                                       as own_unit_name       --所属机构名称
     , t1.state                                                                       as state_code          --流程状态编码
     , lc.diname                                                                      as state_name          --流程状态名称
     , t1.ctime                                                                       as ctime               --创建时间
     , t1.mtime                                                                       as mtime               --修改时间
     , t1.note                                                                        as note                --备注
     , t1.utime                                                                       as utime               --提交到上级时间
     , t1.procinstid                                                                  as procinstid          --流程ID
     , t1.yeartype                                                                    as yeartype_code       --年度类型编码
     , nd.diname                                                                      as yeartype_name       --年度类型名称
     , t1.bpmsstatus                                                                  as bpmsstatus_code     --审批状态编码
     , sp.diname                                                                      as bpmsstatus_name     --审批状态名称
     , t1.canpush                                                                     as canpush             --是否可以推送
     , t1.fixoid                                                                      as calculate_unit_id   --核算单位id
     , cal.zacname_chs                                                                as calculate_unit_name --核算单位名称
     , t1.ptime                                                                       as ptime               --全面预算到上级时间
     , t1.pfname                                                                      as pfname              --批复名称
     , t1.pfzwid                                                                      as pfzwid              --批复正文id
     , t1.pfinfo                                                                      as pfinfo              --批复意见
     , t1.start_date                                                                  as start_date          --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                         as etl_time
     , 'ERMS'                                                                         as source_system
     , 'dim_erms_orgext_d,dim_erms_dictitem_d,ods_cccc_erms_invm_plan_year_apply_i_d,ods_cccc_erms_zmdgs_actorginf_f_d' as source_table
from t1
         left join org t2 on t1.oid = t2.oid
    --    left join org t3 on t1.fixoid = t3.oid
         left join dict lc on t1.state = lc.dicode and lc.dname = '审批结果'
         left join dict nd on t1.yeartype = nd.dicode and nd.dname = '投资年度类型'
         left join dict sp on t1.bpmsstatus = sp.dicode and sp.dname = '审批结果'
         left join cal on t1.fixoid = cal.zaco
;

