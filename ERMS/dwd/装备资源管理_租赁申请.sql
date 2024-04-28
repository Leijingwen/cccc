--装备资源管理_租赁申请
create table dwd.dwd_erms_out_lease_apply_d
(
    leaseid           string comment '租赁申请id',
    lease_apply_code  string comment '租赁申请单编号',
    lease_apply_name  string comment '租赁申请名称',
    leasetype_code    string comment '租赁类型编码',
    leasetype_name    string comment '租赁类型名称',
    equ_num           Decimal(38, 0) comment '装备数量',
    pro_name          string comment '项目名称',
    pro_site_code     string comment '项目地点编码',
    pro_start_date    string comment '项目开始日期',
    pro_end_date      string comment '项目结束日期',
    exnode_code       string comment '审批节点编码',
    exnode_name       string comment '审批节点名称',
    exstate_code      string comment '审批状态编码',
    exstate_name      string comment '审批状态名称',
    apply_date        string comment '申请日期',
    apply_username    string comment '申请人名称',
    apply_orgid       string comment '申请单位ID',
    apply_orgname     string comment '申请单位名称',
    apply_orgusername string comment '申请单位负责人名称',
    procinstid        string comment '流程ID',
    amount            Decimal(19, 6) comment '总预算费用',
    promain           string comment '工程主要内容',
    prosurvey         string comment '项目概况',
    reason            string comment '租赁原因',
    projectid         string comment '项目ID',
    attachid          string comment '附件ID',
    lessorid          string comment '出租方ID',
    ctime             string comment '创建时间',
    mtime             string comment '修改时间',
    note              string comment '备注',
    accleases_coid    string comment '承租单位coid',
    accleases_name    string comment '承租单位名称',
    outmajor_code     string comment '是否是外租入重大装备(1:是)',
    groupapproval     string comment '集团批复附件',
    zproject          string comment '主数据合同id',
    start_date        string comment '开始日期',
    etl_time          string comment 'etl_时间',
    source_system     string comment '来源系统',
    source_table      string comment '来源表名'
) comment '装备资源管理_租赁申请'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_租赁申请
with dict as (select dcode, dname, dicode, diname
              from dwd.dim_erms_dictitem_d
              where dname in ('审批进度')),
     a as (select *
           from ods.ods_cccc_erms_out_lease_apply_i_d
           where end_date = '2999-12-31'
             and isdelete != '1')
--insert overwrite table dwd.dwd_erms_out_lease_apply_d partition(etl_date = '${etl_date}')
select a.leaseid                                              as leaseid           --租赁申请id
     , a.code                                                 as lease_apply_code  --租赁申请单编号
     , a.name                                                 as lease_apply_name  --租赁申请名称
     , a.type                                                 as leasetype_code    --租赁类型编码
     , case
           when a.type = '1' then '内租'
           when a.type = '2' then '外租'
    end                                                       as leasetype_name    --租赁类型名称
     , a.equnum                                               as equ_num           --装备数量
     , a.proname                                              as pro_name          --项目名称
     , a.prosite                                              as pro_site_code     --项目地点编码
     , a.prostartdate                                         as pro_start_date    --项目开始日期
     , a.proenddate                                           as pro_end_date      --项目结束日期
     , a.exnode                                               as exnode_code       --审批节点编码
     , case
           when a.exnode = '0' then '未发起'
           when a.exnode = '1' then '审批中'
           when a.exnode = '-1' then '退回'
           when a.exnode = '100' then '审批完成'
           when a.exnode = '101' then '已完成'
    end                                                       as exnode_name       --审批节点名称
     , a.exstate                                              as exstate_code      --审批状态编码
     , dict.diname                                            as exstate_name      --审批状态名称
     , a.applydate                                            as apply_date        --申请日期
     , a.applyuser                                            as apply_username    --申请人名称
     , a.applyorgid                                           as apply_orgid       --申请单位ID
     , a.applyorgname                                         as apply_orgname     --申请单位名称
     , a.applyorguser                                         as apply_orgusername --申请单位负责人名称
     , a.procinstid                                           as procinstid        --流程ID
     , a.amount                                               as amount            --总预算费用
     , a.promain                                              as promain           --工程主要内容
     , a.prosurvey                                            as prosurvey         --项目概况
     , a.reason                                               as reason            --租赁原因
     , a.projectid                                            as projectid         --项目ID
     , a.attachid                                             as attachid          --附件ID
     , a.lessorid                                             as lessorid          --出租方ID
     , a.ctime                                                as ctime             --创建时间
     , a.mtime                                                as mtime             --修改时间
     , a.note                                                 as note              --备注
     , a.accleasescoid                                        as accleases_coid    --承租单位coid
     , a.accleasesname                                        as accleases_name    --承租单位名称
     , a.outmajor                                             as outmajor_code     --是否是外租入重大装备(1:是)
     , a.groupapproval                                        as groupapproval     --集团批复附件
     , a.zproject                                             as zproject          --主数据合同id
     , a.start_date                                           as start_date        --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'dim_erms_dictitem_d,ods_cccc_erms_out_lease_apply_i_d'                    as source_table
from a
         left join dict on a.exstate = dict.dicode
;


