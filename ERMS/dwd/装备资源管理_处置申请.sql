--装备资源管理_处置申请
create table dwd.dwd_erms_handle_dis_apply_d
(
    hdaid            string comment '处置申请id',
    hdcode           string comment '处置单号',
    hdaname          string comment '处置申请单名称',
    hdtype_code      string comment '处置类型编码',
    hdtype_name      string comment '处置类型名称',
    disdate          string comment '处置日期',
    hdreason         string comment '处置原因',
    hdequnum         Decimal(38, 0) comment '处置装备数量',
    ovalue           Decimal(19, 6) comment '原值（万元）',
    nvalue           Decimal(19, 6) comment '净值（万元）',
    dvalue           Decimal(19, 6) comment '估计残值（万元）',
--     bussinesstype_code string comment '业务类型编码',
--     bussinesstype_name string comment '业务类型名称',
    ctime            string comment '创建时间',
    mtime            string comment '修改时间',
    note             string comment '备注',
    procinstid       string comment '流程id',
    exstate_code     string comment '审批状态编码',
    exstate_name     string comment '审批状态名称',
    exnode_code      string comment '当前节点编码',
    exnode_name      string comment '当前节点名称',
    applydate        string comment '申请日期',
    unit_oid         string comment '单位oid',
    unit_name        string comment '单位名称',
    second_unit_id   string comment '二级单位id',
    second_unit_name string comment '二级单位名称',
    third_unit_id    string comment '三级单位id',
    third_unit_name  string comment '三级单位名称',
    start_date       string comment '开始日期',
    etl_time         string comment 'etl_时间',
    source_system    string comment '来源系统',
    source_table     string comment '来源表名'
) comment '装备资源管理_处置申请'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_处置申请
with dis_app as (select *
                 from ods.ods_cccc_erms_handle_dis_apply_i_d
                 where end_date = '2999-12-31'
                   and isdelete != '1'),
     org_rule as (select oid
                       , oname
                       , second_unit_id
                       , second_unit_name
                       , third_unit_id
                       , third_unit_name
                  from dwd.dim_erms_orgext_d),
     dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname in ('处置审批节点'))
-- insert overwrite table dwd.dwd_erms_handle_dis_apply_d partition(etl_date = '${etl_date}')
select dis_app.hdaid                                          as hdaid            --处置申请id
     , dis_app.code                                           as hdcode           --处置单号
     , dis_app.name                                           as hdaname          --处置申请单名称
     , dis_app.type                                           as hdtype_code      --处置类型编码
     , case
           when dis_app.type = '1' then '让售'
           when dis_app.type = '2' then '报废' end            as hdtype_name      --处置类型名称
     , dis_app.disdate                                        as disdate          --处置日期
     , dis_app.reason                                         as hdreason         --处置原因
     , dis_app.amount                                         as hdequnum         --处置装备数量
     , dis_app.ovalue                                         as ovalue           --原值（万元）
     , dis_app.nvalue                                         as nvalue           --净值（万元）
     , dis_app.dvalue                                         as dvalue           --估计残值（万元）
--      , dis_app.bussinesstype                                  as bussinesstype_code --业务类型编码
--      , t2.null                                                as bussinesstype_name --业务类型名称(非直取,请注意查看文档进行调整)
     , dis_app.ctime                                          as ctime            --创建时间
     , dis_app.mtime                                          as mtime            --修改时间
     , dis_app.note                                           as note             --备注
     , dis_app.procinstid                                     as procinstid       --流程id
     , dis_app.exstate                                        as exstate_code     --审批状态编码
     , case
           when dis_app.exstate = '0' then '未发起'
           when dis_app.exstate = '1' then '审批中'
           when dis_app.exstate = '-1' then '退回'
           when dis_app.exstate = '101' then '审核完成' end   as exstate_name      --审批状态名称(非直取,请注意查看文档进行调整)
     , dis_app.exnode                                         as exnode_code      --当前节点编码
     , dict.diname                                            as exnode_name      --当前节点名称
     , dis_app.applydate                                      as applydate        --申请日期
     , dis_app.oid                                            as oid              --单位oid
     , org_rule.oname                                         as unit_name        --单位名称
     , org_rule.second_unit_id                                as second_unit_id   --二级单位id
     , org_rule.second_unit_name                              as second_unit_name --二级单位名称
     , org_rule.third_unit_id                                 as third_unit_id    --三级单位id
     , org_rule.third_unit_name                               as third_unit_name  --三级单位名称
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , dis_app.start_date                                     as start_date       --开始日期
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_handle_dis_apply_i_d,dim_erms_orgext_d,dim_erms_dictitem_d' as source_table
from dis_app
         left join org_rule on dis_app.oid = org_rule.oid
         left join dict on dis_app.exnode = dict.dicode and dict.dname = '处置审批节点'
;


