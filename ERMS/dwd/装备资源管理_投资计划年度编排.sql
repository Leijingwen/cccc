--装备资源管理_投资计划年度编排

create table dwd.dwd_erms_invm_plan_year_planedit_d
(
    plid                 string comment '投资计划id',
    plname               string comment '投资计划名称',
    invest_year          string comment '投资年度',
    equtype_code         string comment '装备类型编码',
    equtype_name         string comment '装备类型名称',
    invtype_code         string comment '投资方式编码',
    invtype_name         string comment '投资方式名称',
    equnum               Decimal(38, 15) comment '装备数量',
    pap                  string comment '主要参数及性能（简述）',
    invamount            Decimal(20, 6) comment '投资总金额',
    source               string comment '资金来源',
    firr                 string comment '财务内部收益率',
    fnpv                 Decimal(20, 6) comment '财务净现值',
    pt                   Decimal(38, 15) comment '投资回收期（年）',
    necessity            string comment '投资必要性',
    risk                 string comment '投资风险因素',
    compleasing          string comment '租赁对比分析',
    edstate_code         string comment '编辑状态编码',
    edstate_name         string comment '编辑状态名称',
    sno                  Decimal(38, 15) comment '排序号',
    isdepproject         string comment '是否依托项目',
    proid                string comment '依托项目ID',
    attid_bd             string comment '附件_重大设备投资简要说明',
    attid_ed             string comment '附件_编制说明',
    ctime                string comment '创建时间',
    mtime                string comment '修改时间',
    note                 string comment '备注',
    appstate_code        string comment '计划审批状态编码',
    appstate_name        string comment '计划审批状态名称',
    repstate_code        string comment '计划最终集团批复状态编码',
    repstate_name        string comment '计划最终集团批复状态',
    yeartype_code        string comment '年度类型编码',
    yeartype_name        string comment '年度类型名称',
    stage_code           string comment '项目阶段编码',
    stage_name           string comment '项目阶段名称',
    ownfunds             Decimal(20, 6) comment '自有资金',
    debtfinanc           Decimal(20, 6) comment '债务融资',
    equityfinanc         Decimal(20, 6) comment '权益融资',
    majorequ             string comment '装备分类',
    own_orgid            string comment '所属机构编码',
    own_orgname          string comment '所属机构名称',
    own_second_unit_id   string comment '所属二级单位id',
    own_second_unit_name string comment '所属二级单位名称',
    own_third_unit_id    string comment '所属三级单位id',
    own_third_unit_name  string comment '所属三级单位名称',
    adjustplid           string comment '调整计划ID',
    isedit_code          string comment '调整状态编码',
    isedit_name          string comment '调整状态名称',
    est_equnum           Decimal(38, 15) comment '可立项数量',
    est_invamount        Decimal(38, 15) comment '可立项金额',
    ested_equnum         Decimal(38, 15) comment '已立项数量',
    ested_invamount      Decimal(38, 15) comment '已立项金额',
    yearplid             string comment '关联的年度投资计划',
    reportnote           string comment '报表备注',
    proctime             string comment '提交流程时间',
    budgetaccount        string comment '预算科目',
    adjustaccount        string comment '核算科目',
    cashflow             string comment '现金流量项目',
    contractname         string comment '合同名称',
    taxinvamount         Decimal(20, 6) comment '投资不含税金额',
    commencement         string comment '开工情况',
    keyproject           string comment '是否重点项目',
    schedate             string comment '预计开工时间',
    schecompdate         string comment '预计完工时间',
    actualcommtime       string comment '实际开工时间',
    projecttype          string comment '项目类型',
    citeplid             string comment '引用的计划(续建)',
    fixoid               string comment '核算单位',
    ispushiniti          string comment '年度预算是否推送至财务系统',
    zcontract            string comment '主数据合同编码',
    zproject             string comment '主数据项目编码',
    ppdate               string comment '预计采购时间',
    ddate                string comment '交货时间',
    zmaterial            string comment '资产明细编码',
    zmname               string comment '资产明细名称',
    pk_purmatdetl        Decimal(38, 0) comment '物资明细PK',
    psid                 string comment '采购计划id',
    pk_firstdetl         Decimal(38, 0) comment '物资明细首版PK',
    iscurtver            string comment '是否当前版本',
    isnewver             string comment '是否最新版本',
    uom                  string comment '标准计量单位',
    pk_requunit          string comment '需求单位编号',
    location_code        string comment '所在区域编码',
    province_code        string comment '所在省编码',
    city_code            string comment '所在市编码',
    county_code          string comment '所在县编码',
    dplace               string comment '详细地址(交货地址)',
    start_date           string comment '开始日期',
    etl_time             string comment 'etl_时间',
    source_system        string comment '来源系统',
    source_table         string comment '来源表名'
) comment '装备资源管理_投资计划年度编排'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资计划年度编排
with dict as (select dcode, dname, dicode, diname
              from dwd.dim_erms_dictitem_d
              where dname in ('投资方式', '投资计划编制编辑状态', '投资年度类型', '调整状态')),
     t1 as (select *
            from ods.ods_cccc_erms_invm_plan_year_planedit_i_d
            where end_date = '2999-12-31'
              and isdelete != '1'),
     org as (select *
             from dwd.dim_erms_orgext_d)
--insert overwrite table dwd.dwd_erms_invm_plan_year_planedit_d partition(etl_date = '${etl_date}')
select t1.plid                                                                           as plid                 --投资计划id
     , t1.name                                                                           as plname               --投资计划名称
     , t1.year                                                                           as invest_year          --投资年度
     , t1.equtype                                                                        as equtype_code         --装备类型编码
     , t1.equtypename                                                                    as equtype_name         --装备类型名称
     , t1.invtype                                                                        as invtype_code         --投资方式编码
     , dict_tzfs.diname                                                                  as invtype_name         --投资方式名称(非直取,请注意查看文档进行调整)
     , t1.equamount                                                                      as equnum               --装备数量
     , t1.pap                                                                            as pap                  --主要参数及性能（简述）
     , t1.invamount                                                                      as invamount            --投资总金额
     , t1.source                                                                         as source               --资金来源
     , t1.firr                                                                           as firr                 --财务内部收益率
     , t1.fnpv                                                                           as fnpv                 --财务净现值
     , t1.pt                                                                             as pt                   --投资回收期（年）
     , t1.necessity                                                                      as necessity            --投资必要性
     , t1.risk                                                                           as risk                 --投资风险因素
     , t1.compleasing                                                                    as compleasing          --租赁对比分析
     , t1.edstate                                                                        as edstate_code         --编辑状态编码
     , dict_bjzt.diname                                                                  as edstate_name         --编辑状态名称(非直取,请注意查看文档进行调整)
     , t1.sno                                                                            as sno                  --排序号
     , t1.isdepproject                                                                   as isdepproject         --是否依托项目
     , t1.proid                                                                          as proid                --依托项目ID
     , t1.attid_bd                                                                       as attid_bd             --附件_重大设备投资简要说明
     , t1.attid_ed                                                                       as attid_ed             --附件_编制说明
     , t1.ctime                                                                          as ctime                --创建时间
     , t1.mtime                                                                          as mtime                --修改时间
     , t1.note                                                                           as note                 --备注(非直取,请注意查看文档进行调整)
     , t1.appstate                                                                       as appstate_code        --计划审批状态编码
     , t1.appstatename                                                                   as appstate_name        --计划审批状态名称
     , t1.repstate                                                                       as repstate_code        --计划最终集团批复状态编码
     , t1.repstatename                                                                   as repstate_name        --计划最终集团批复状态
     , t1.yeartype                                                                       as yeartype_code        --年度类型编码
     , dict_ndlx.diname                                                                  as yeartype_name        --年度类型名称(非直取,请注意查看文档进行调整)
     , t1.stage                                                                          as stage_code           --项目阶段编码
     , t1.stagename                                                                      as stage_name           --项目阶段名称
     , t1.ownfunds                                                                       as ownfunds             --自有资金
     , t1.debtfinanc                                                                     as debtfinanc           --债务融资
     , t1.equityfinanc                                                                   as equityfinanc         --权益融资
     , t1.majorequ                                                                       as majorequ             --装备分类
     , t1.coid                                                                           as own_orgid            --所属机构编码
     , org.oname                                                                         as own_orgname          --所属机构名称(非直取,请注意查看文档进行调整)
     , org.second_unit_id                                                                as own_second_unit_id   --所属二级单位id(非直取,请注意查看文档进行调整)
     , org.second_unit_name                                                              as own_second_unit_name --所属二级单位名称(非直取,请注意查看文档进行调整)
     , org.third_unit_id                                                                 as own_third_unit_id    --所属三级单位id(非直取,请注意查看文档进行调整)
     , org.third_unit_name                                                               as own_third_unit_name  --所属三级单位名称(非直取,请注意查看文档进行调整)
     , t1.adjustplid                                                                     as adjustplid           --调整计划ID
     , t1.isedit                                                                         as isedit_code          --调整状态编码
     , dict_tzzt.diname                                                                  as isedit_name          --调整状态名称(非直取,请注意查看文档进行调整)
     , t1.est_equamount                                                                  as est_equnum           --可立项数量
     , t1.est_invamount                                                                  as est_invamount        --可立项金额
     , t1.ested_equamount                                                                as ested_equnum         --已立项数量
     , t1.ested_invamount                                                                as ested_invamount      --已立项金额
     , t1.yearplid                                                                       as yearplid             --关联的年度投资计划
     , t1.reportnote                                                                     as reportnote           --报表备注
     , t1.proctime                                                                       as proctime             --提交流程时间
     , t1.budgetaccount                                                                  as budgetaccount        --预算科目
     , t1.adjustaccount                                                                  as adjustaccount        --核算科目
     , t1.cashflow                                                                       as cashflow             --现金流量项目
     , t1.contractname                                                                   as contractname         --合同名称
     , t1.taxinvamount                                                                   as taxinvamount         --投资不含税金额
     , t1.commencement                                                                   as commencement         --开工情况
     , t1.keyproject                                                                     as keyproject           --是否重点项目
     , t1.schedate                                                                       as schedate             --预计开工时间
     , t1.schecompdate                                                                   as schecompdate         --预计完工时间
     , t1.actualcommtime                                                                 as actualcommtime       --实际开工时间
     , t1.projecttype                                                                    as projecttype          --项目类型
     , t1.citeplid                                                                       as citeplid             --引用的计划(续建)
     , t1.fixoid                                                                         as fixoid               --核算单位
     , t1.ispushiniti                                                                    as ispushiniti          --年度预算是否推送至财务系统
     , t1.zcontract                                                                      as zcontract            --主数据合同编码
     , t1.zproject                                                                       as zproject             --主数据项目编码
     , t1.ppdate                                                                         as ppdate               --预计采购时间
     , t1.ddate                                                                          as ddate                --交货时间
     , t1.zmaterial                                                                      as zmaterial            --资产明细编码
     , t1.zmname                                                                         as zmname               --资产明细名称
     , t1.pk_purmatdetl                                                                  as pk_purmatdetl        --物资明细PK
     , t1.psid                                                                           as psid                 --采购计划id
     , t1.pk_firstdetl                                                                   as pk_firstdetl         --物资明细首版PK
     , t1.iscurtver                                                                      as iscurtver            --是否当前版本
     , t1.isnewver                                                                       as isnewver             --是否最新版本
     , t1.uom                                                                            as uom                  --标准计量单位
     , t1.pk_requunit                                                                    as pk_requunit          --需求单位编号
     , t1.location                                                                       as location_code        --所在区域编码
     , t1.province                                                                       as province_code        --所在省编码
     , t1.city                                                                           as city_code            --所在市编码
     , t1.county                                                                         as county_code          --所在县编码
     , t1.dplace                                                                         as dplace               --详细地址(交货地址)(非直取,请注意查看文档进行调整)
     , t1.start_date                                                                     as start_date           --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                            as etl_time
     , 'ERMS'                                                                            as source_system
     , 'dim_erms_dictitem_d,dim_erms_orgext_d,ods_cccc_erms_invm_plan_year_planedit_i_d' as source_table
from t1
         left join dict dict_tzfs on t1.invtype = dict_tzfs.dicode and dict_tzfs.dname = '投资方式'
         left join dict dict_bjzt on t1.edstate = dict_bjzt.dicode and dict_bjzt.dname = '投资计划编制编辑状态'
         left join dict dict_ndlx on t1.yeartype = dict_ndlx.dicode and dict_ndlx.dname = '投资年度类型'
         left join dict dict_tzzt on t1.isedit = dict_tzzt.dicode and dict_tzzt.dname = '调整状态'
         left join org on t1.coid = org.oid
;





