create table dwd.dwd_erms_deploy_dispatch_d
(
    disid                  string comment '调配单id',
    daid                   string comment '调配申请id',
    dispatch_app_no        string comment '调遣申请单号',
    dispatch_name          string comment '调遣申请名称',
    tran_in_unit_id        string comment '调入单位id',
    tran_in_unit_name      string comment '调入单位名称',
    tran_out_unit_id       string comment '调出单位id',
    tran_out_unit_name     string comment '调出单位名称',
    zprojectin             string comment '调入项目主数据编码',
    zprojectout            string comment '调出项目主数据编码',
    tran_in_proname        string comment '调入项目名称',
    tran_out_proname       string comment '调出项目名称',
--     tran_in_project        string comment '调入工程',
--     tran_out_project       string comment '调出工程',
    equnum                 string comment '设备数量',
    application_name       string comment '申请人名称',
    application_time       string comment '申请时间',
    all_total_amount       Decimal(28, 6) comment '调配总金额',
--     dispatch_status_code   string comment '调遣状态编码',
--     dispatch_status_name   string comment '调遣状态名称',
    distance               Decimal(18, 6) comment '距离',
    ctime                  string comment '创建时间',
    mtime                  string comment '修改时间',
    type_code              string comment '调配装备大类编码',
    type_name              string comment '调配装备大类名称',
    tran_out_depart_date   string comment '调出地点出发日期',
    tran_in_location_code  string comment '调入地点编码',
    tran_in_location_name  string comment '调入地点名称',
    tran_out_location_code string comment '调出地点编码',
    tran_out_location_name string comment '调出地点名称',
    actual_time            string comment '实际进场时间',
    applyno                string comment '申请编号',
    start_date             string comment '开始日期',
    etl_time               string comment 'etl_时间',
    source_system          string comment '来源系统',
    source_table           string comment '来源表名'
) comment '装备资源管理_调配记录'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_调配记录
with t1 as (select *
            from ods.ods_cccc_erms_deploy_dispatch_i_d
            where end_date = '2999-12-31'
              and is_delete != '1'),
     org as (select oid
                  , oname
                  , second_unit_id
                  , second_unit_name
                  , third_unit_id
                  , third_unit_name
             from dwd.dim_erms_orgext_d),
     loc as (select trim(loc_code) as loc_code,
                         trim(loc_name) as loc_name
                  from (select zaddvscode as loc_code
                             , zaddvsname as loc_name
                        from ods.ods_cccc_erms_own_zaddvs_f_d --境内
                        union all
                        select zcountrycode as loc_code
                             , zcountryname as loc_name
                        from ods.ods_cccc_erms_own_zcountry_f_d --境外
                       ) t1)
-- insert overwrite table dwd.dwd_erms_deploy_dispatch_d partition(etl_date = '${etl_date}')
select t1.disid                                                                                                          as disid                  --调配单id
     , t1.daid                                                                                                           as daid                   --调配申请id
     , t1.dispatch_app_no                                                                                                as dispatch_app_no        --调遣申请单号
     , t1.dispatch_name                                                                                                  as dispatch_name          --调遣申请名称
     , t1.transfer_company                                                                                               as tran_in_unit_id        --调入单位id
     , t2.oname                                                                                                          as tran_in_unit_name      --调入单位名称
     , t1.transfer_unit                                                                                                  as tran_out_unit_id       --调出单位id
     , t3.oname                                                                                                          as tran_out_unit_name     --调出单位名称
     , t1.zprojectin                                                                                                     as zprojectin             --调入项目主数据编码
     , t1.zprojectout                                                                                                    as zprojectout            --调出项目主数据编码
     , t1.transfer_project                                                                                               as tran_in_proname        --调入项目名称
     , t1.call_out_item                                                                                                  as tran_out_proname       --调出项目名称
--      , t1.transfer_in_project                                 as tran_in_project        --调入工程
--      , t1.transfer_out_project                                as tran_out_project       --调出工程
     , t1.equ_number                                                                                                     as equnum                 --设备数量
     , t1.application                                                                                                    as application_name       --申请人名称
     , t1.application_time                                                                                               as application_time       --申请时间
     , t1.all_total_amount                                                                                               as all_total_amount       --调配总金额
--      , t1.dispatch_status                                     as dispatch_status_code   --调遣状态编码
--      , null                                                   as dispatch_status_name   --调遣状态名称(非直取,请注意查看文档进行调整)
     , t1.distance                                                                                                       as distance               --距离
     , t1.ctime                                                                                                          as ctime                  --创建时间
     , t1.mtime                                                                                                          as mtime                  --修改时间
     , t1.type                                                                                                           as type_code              --调配装备大类编码
     , case
           when t1.type = '1' then '船舶'
           when t1.type = '2'
               then '机械' end                                                                                           as type_name              --调配装备大类名称
     , t1.tran_out_depart_date                                                                                           as tran_out_depart_date   --调出地点出发日期
     , t1.tran_in_location                                                                                               as tran_in_location_code  --调入地点编码
     , t4.loc_name                                                                                                       as tran_in_location_name  --调入地点名称
     , t1.tran_out_location                                                                                              as tran_out_location_code --调出地点编码
     , t4.loc_name                                                                                                       as tran_out_location_name --调出地点名称
     , t1.actual_time                                                                                                    as actual_time            --实际进场时间
     , t1.applyno                                                                                                        as applyno                --申请编号
     , t1.start_date                                                                                                     as start_date             --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                                            as etl_time
     , 'ERMS'                                                                                                            as source_system
     , 'ods_cccc_erms_deploy_dispatch_i_d,dim_erms_orgext_d,ods_cccc_erms_own_zaddvs_f_d,ods_cccc_erms_own_zcountry_f_d' as source_table
from t1
         left join org t2 on t1.transfer_company = t2.oid
         left join org t3 on t1.transfer_unit = t3.oid
         left join loc t4 on t1.tran_in_location = t4.loc_code
         left join loc t5 on t1.tran_out_location = t5.loc_code
;



