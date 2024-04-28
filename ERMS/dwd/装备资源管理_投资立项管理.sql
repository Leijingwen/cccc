create table dwd.dwd_erms_invm_plan_proapply_d
(
    pappid             string comment '投资立项id',
    apname             string comment '申报名称',
    equtype_code       string comment '装备类型编码',
    equtype_name       string comment '装备类型名称',
    equnum             Decimal(38, 0) comment '投资装备数量',
    invtype_code       string comment '投资方式编码',
    invtype_name       string comment '投资方式名称',
    invamount          Decimal(20, 6) comment '总体投资金额(万元)',
    attid_fea_word     string comment '附件_可行研究报告_WORD',
    attid_fea_pdf      string comment '附件_可行研究报告_PDF',
    attid_reso         string comment '附件_董事会决议',
    ctime              string comment '创建时间',
    mtime              string comment '修改时间',
    note               string comment '备注',
    approve_state_code string comment '审批状态编码',
    approve_state_name string comment '审批状态名称',
    procinstid         string comment '流程实例ID，为了匹配任务表',
    attid_plan         string comment '附件_投资立项申请报告',
    own_unit_coid      string comment '所属机构id',
    own_unit_name      string comment '所属单位名称',
    majorequ           string comment '装备分类',
    endfinishwork      Decimal(8, 4) comment '累计工程量计划完成比(%)',
    endfinishamount    Decimal(19, 6) comment '累计完成投资金额(万元)',
    new_fill_cycle     string comment '最新填报周期',
    planprogress_code  string comment '投资进度编码',
    planprogress_name  string comment '投资进度名称',
    applycode          string comment '申报编号',
    replydate          string comment '批复日期',
    pequnum            Decimal(38, 0) comment '计划装备总数量',
    pinvamount         Decimal(22, 6) comment '计划总体投资金额',
    title              string comment '标题(用于发起签报)',
    --glpappid           string comment '关联其他系统的立项申报id',
    --dj_unit_coid       string comment '填写对接单位的id',
    --receivetime        string comment '接收时间',
    pfattach           string comment '批复附件id',
    issendapproval     string comment '是否发起批复',
    start_date         string comment '开始日期',
    etl_time           string comment 'etl_时间',
    source_system      string comment '来源系统',
    source_table       string comment '来源表名'
) comment '装备资源管理_投资立项管理'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资立项管理
with t1 as (select *
            from ods.ods_cccc_erms_invm_plan_proapply_i_d
            where end_date = '2999-12-31'),
     mapping as (select equtype_code,
                        equtype_name
                 from dwd.dim_erms_equip_type_mapping_d),
     dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname in ('投资方式', '投资立项状态'))
-- insert overwrite table dwd.dwd_erms_invm_plan_proapply_d partition(etl_date = '${etl_date}')
select t1.pappid                                                                                as pappid             --投资立项id
     , t1.name                                                                                  as apname             --申报名称
     , t1.equtype                                                                               as equtype_code       --装备类型编码
     , mapping.equtype_name                                                                     as equtype_name       --装备类型名称
     , t1.equamount                                                                             as equnum             --投资装备数量
     , t1.invtype                                                                               as invtype_code       --投资方式编码
     , t2.diname                                                                                as invtype_name       --投资方式名称
     , t1.invamount                                                                             as invamount          --总体投资金额(万元)
     , t1.attid_fea_word                                                                        as attid_fea_word     --附件_可行研究报告_WORD
     , t1.attid_fea_pdf                                                                         as attid_fea_pdf      --附件_可行研究报告_PDF
     , t1.attid_reso                                                                            as attid_reso         --附件_董事会决议
     , t1.ctime                                                                                 as ctime              --创建时间
     , t1.mtime                                                                                 as mtime              --修改时间
     , t1.note                                                                                  as note               --备注
     , t1.state                                                                                 as approve_state_code --审批状态编码
     , t3.diname                                                                                as approve_state_name --审批状态名称
     , t1.procinstid                                                                            as procinstid         --流程实例ID，为了匹配任务表
     , t1.attid_plan                                                                            as attid_plan         --附件_投资立项申请报告
     , t1.coid                                                                                  as own_unitid         --所属机构id
     , t1.oidname                                                                               as own_unitname       --所属单位名称
     , t1.majorequ                                                                              as majorequ           --装备分类
     , t1.endfinishwork                                                                         as endfinishwork      --累计工程量计划完成比(%)
     , t1.endfinishamount                                                                       as endfinishamount    --累计完成投资金额(万元)
     , t1.newdata                                                                               as new_fill_cycle     --最新填报周期
     , t1.planprogress                                                                          as planprogress_code  --投资进度编码
     , case
           when t1.planprogress = '0' then '未完成'
           when t1.planprogress = '1' then '已完成'
    end                                                                                         as planprogress_name  --投资进展状态名称
     , t1.applycode                                                                             as applycode          --申报编号
     , t1.replydate                                                                             as replydate          --批复日期
     , t1.pequamount                                                                            as pequnum            --计划装备总数量
     , t1.pinvamount                                                                            as pinvamount         --计划总体投资金额
     , t1.title                                                                                 as title              --标题(用于发起签报)
     --, t1.glpappid                                            as glpappid           --关联其他系统的立项申报id
     --, t1.djcoid                                              as dj_unit_coid       --填写对接单位的coid
     --, t1.receivetime                                         as receivetime        --接收时间
     , t1.pfattach                                                                              as pfattach           --批复附件id
     , t1.issendapproval                                                                        as issendapproval     --是否发起批复
     , t1.start_date                                                                            as start_date         --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                   as etl_time
     , 'ERMS'                                                                                   as source_system
     , 'ods_cccc_erms_invm_plan_proapply_i_d,dim_erms_equip_type_mapping_d,dim_erms_dictitem_d' as source_table
from t1
         left join mapping on t1.equtype = mapping.equtype_code
         left join dict t2 on t1.invtype = t2.dicode and t2.dname = '投资方式'
         left join dict t3 on t1.state = t3.dicode and t3.dname = '投资立项状态'
;