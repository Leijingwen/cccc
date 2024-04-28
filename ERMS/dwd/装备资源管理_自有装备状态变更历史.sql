--装备资源管理_自有装备状态变更历史
create table dwd.dwd_erms_own_state_change_his_d
(
    equ_mastercode     string comment '装备主数据编码',
    state_change_id    string comment '状态变更id',
    asid               string comment '资产id',
    status_value_code  string comment '状态值编码',
    status_value_name  string comment '状态值名称',
    state_start_time   string comment '状态开始时间',
    state_change_time  string comment '状态变更时间',
    change_person_name string comment '变更人名称',
    change_resource    string comment '变更来源',
    ctime              string comment '创建时间',
    mtime              string comment '修改时间',
    reason             string comment '变更原因',
    start_date         string comment '开始日期',
    etl_time           string comment 'etl_时间',
    source_system      string comment '来源系统',
    source_table       string comment '来源表名'
) comment '装备资源管理_自有装备状态变更历史'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_自有装备状态变更历史
with dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname = '外部装备状态'),
     a as (select *
           from ods.ods_cccc_erms_own_state_change_his_i_d
           where end_date = '2999-12-31'),
     c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31')
--insert overwrite table dwd.dwd_erms_own_state_change_his_d partition(etl_date = '${etl_date}')
select c.mastercode                                                                                       as equ_mastercode     --装备主数据编码
     , a.id                                                                                               as state_change_id    --状态变更id
     , a.asid                                                                                             as asid               --资产id
     , a.status_value                                                                                     as status_value_code  --状态值编码
     , dict.diname                                                                                        as status_value_name  --状态值名称
     , a.state_start_time                                                                                 as state_start_time   --状态开始时间
     , a.state_change_time                                                                                as state_change_time  --状态变更时间
     , a.change_person                                                                                    as change_person_name --变更人名称
     , a.change_resource                                                                                  as change_resource    --变更来源
     , a.ctime                                                                                            as ctime              --创建时间
     , a.mtime                                                                                            as mtime              --修改时间
     , a.reason                                                                                           as reason             --变更原因
     , a.start_date                                                                                       as start_date         --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                             as etl_time
     , 'ERMS'                                                                                             as source_system
     , 'dim_erms_dictitem_d,ods_cccc_erms_own_state_change_his_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d' as source_table
from a
         left join dict on a.status_value = dict.dicode
         left join c on a.asid = c.asid
;