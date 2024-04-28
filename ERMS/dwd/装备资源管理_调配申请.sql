create table dwd.dwd_erms_deploy_apply_d
(
    daid              string comment '调配申请id',
    applyno           string comment '申请单编号',
    deapply_name      string comment '调配申请名称',
    deapply_coid      string comment '调配申请单位coid',
    deapply_coname    string comment '调配申请单位名称',
    deout_coid        string comment '调出单位coid',
    deout_coname      string comment '调出单位名称',
    dein_coid         string comment '调入单位coid',
    dein_coname       string comment '调入单位名称',
    deployoutpro_name string comment '调出项目名称',
    deployinpro_name  string comment '调入项目名称',
    zprojectout       string comment '调出项目主数据编码',
    zprojectin        string comment '调入项目主数据编码',
    equamount         Decimal(38, 0) comment '装备数量',
    deploycost        Decimal(19, 2) comment '调配费用',
    range             Decimal(38, 0) comment '距离',
    curnode_code      string comment '当前节点编码',
    curnode_name      string comment '当前节点名称',
    state_code        string comment '审批状态编码',
    state_name        string comment '审批状态名称',
    apply_date        string comment '申请日期',
    applycu_id        string comment '申请人id',
    applycu_name      string comment '申请人名称',
    apply_phone       string comment '申请人联系方式',
    ctime             string comment '创建时间',
    mtime             string comment '修改时间',
    procinstid        string comment '流程ID',
    type_code         string comment '装备大类编码',
    type_name         string comment '装备大类名称',
    start_date        string comment '开始日期',
    etl_time          string comment 'etl_时间',
    source_system     string comment '来源系统',
    source_table      string comment '来源表名'
) comment '装备资源管理_调配申请'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_调配申请
with t1 as (select *
            from ods.ods_cccc_erms_deploy_apply_i_d
            where end_date = '2999-12-31'
              and isdelete != '1'),
     dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname in ('调配审批节点', '审批进度'))
--insert overwrite table dwd.dwd_erms_deploy_apply_d partition(etl_date = '${etl_date}')
select t1.daid                                                as daid              --调配申请id
     , t1.applyno                                             as applyno           --申请单编号
     , t1.deapplyname                                         as deapply_name      --调配申请名称
     , t1.deapplycoid                                         as deapply_coid      --调配申请单位coid
     , t1.deapplyconame                                       as deapply_coname    --调配申请单位名称
     , t1.deoutcoid                                           as deout_coid        --调出单位coid
     , t1.deoutconame                                         as deout_coname      --调出单位名称
     , t1.deincoid                                            as dein_coid         --调入单位coid
     , t1.deinconame                                          as dein_coname       --调入单位名称
     , t1.deployoutpro                                        as deployoutpro_name --调出项目名称
     , t1.deployinpro                                         as deployinpro_name  --调入项目名称
     , t1.zprojectout                                         as zprojectout       --调出项目主数据编码
     , t1.zprojectin                                          as zprojectin        --调入项目主数据编码
     , t1.equamount                                           as equamount         --装备数量
     , t1.deploycost                                          as deploycost        --调配费用
     , t1.range                                               as range             --距离
     , t1.curnode                                             as curnode_code      --当前节点编码
     , t2.diname                                              as curnode_name      --当前节点名称
     , t1.state                                               as state_code        --审批状态编码
     , t3.diname                                              as state_name        --审批状态名称(非直取,请注意查看文档进行调整)
     , t1.applydate                                           as apply_date        --申请日期
     , t1.applycuid                                           as applycu_id        --申请人id
     , t1.applycuname                                         as applycu_name      --申请人名称
     , t1.applyphone                                          as apply_phone       --申请人联系方式
     , t1.ctime                                               as ctime             --创建时间
     , t1.mtime                                               as mtime             --修改时间
     , t1.procinstid                                          as procinstid        --流程ID
     , t1.type                                                as type_code         --装备大类编码
     , case
           when t1.type = '1' then '船舶'
           when t1.type = '2' then '机械' end                 as type_name         --装备大类名称
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , t1.start_date                                          as start_date        --开始日期
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_deploy_apply_i_d,dim_erms_dictitem_d'   as source_table
from t1
         left join dict t2 on t1.curnode = t2.dicode and t2.dname = '调配审批节点'
         left join dict t3 on t1.state = t3.dicode and t3.dname = '审批进度'
;