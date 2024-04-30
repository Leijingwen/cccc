drop table dwd.dwd_erms_invm_proapply_approval_d;
create table dwd.dwd_erms_invm_proapply_approval_d
(
    pfid          string comment '批复id',
    pftitle       string comment '批复标题',
    procinstid    string comment '流程实例id',
    pfstate_code  string comment '审批状态编码',
    pfstate_name  string comment '审批状态名称',
    pappid        string comment '投资立项id',
    ctime         string comment '创建时间',
    mtime         string comment '修改时间',
    own_unit_id   string comment '所属单位id',
    own_unit_name string comment '所属单位名称',
    start_date    string comment '开始日期',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_投资立项批复'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资立项批复
with t1 as (select *
            from ods.ods_cccc_erms_invm_proapply_approval_i_d
            where end_date = '2999-12-31'),
     org as (select oid
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
              WHERE dname in ('投资立项状态'))
-- insert overwrite table dwd.dwd_erms_invm_proapply_approval_d partition(etl_date = '${etl_date}')
select t1.pfid                                                                          as pfid          --批复id
     , t1.pftitle                                                                       as pftitle       --批复标题
     , t1.procinstid                                                                    as procinstid    --流程实例id
     , t1.pfstate                                                                       as pfstate_code  --审批状态编码
     , dict.diname                                                                      as pfstate_name  --审批状态名称
     , t1.pappid                                                                        as pappid        --投资立项id
     , t1.ctime                                                                         as ctime         --创建时间
     , t1.mtime                                                                         as mtime         --修改时间
     , t1.coid                                                                          as own_unit_id   --所属单位id
     , org.oname                                                                        as own_unit_name --所属单位名称
     , t1.start_date                                                                    as start_date    --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                           as etl_time
     , 'ERMS'                                                                           as source_system
     , 'ods_cccc_erms_invm_proapply_approval_i_d,dim_erms_orgext_d,dim_erms_dictitem_d' as source_table
from t1
         left join org on t1.coid = org.oid
         left join dict on t1.pfstate = dict.dicode
;