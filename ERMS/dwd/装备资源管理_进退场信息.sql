create table dwd.dwd_erms_use_entry_exit_info_d
(
    eeid                   string comment '进退场id',
    asid                   string comment '资产id',
    equ_mastercode         string comment '装备主数据编码',
    disid                  string comment '调遣单id',
    zproject               string comment '项目主数据id',
    project_name           string comment '所属项目名称',
    equ_sour_code          string comment '装备来源编码',
    equ_sour_name          string comment '装备来源名称',
    equ_code               string comment '装备编码',
    equ_name               string comment '装备名称',
    equtype_code           string comment '装备类型编码',
    equtype_name           string comment '装备类型名称',
    equ_class              string comment '装备分类',
    license                string comment '车牌号',
    driver                 string comment '机驾人员',
    equ_tool               string comment '设备随机工具',
    tech_status            string comment '技术状况',
    equ_data               string comment '设备随机资料',
    entry_exit_status_code string comment '进退场状态编码',
    entry_exit_name_name   string comment '进退场状态名称',
    mob_basic              string comment '进场依据',
    acceptor_name          string comment '验收人名称',
    transfer_name          string comment '移交人名称',
    safety_name            string comment '安全员名称',
    prepared_name          string comment '编制人名称',
    accep_date             string comment '验收日期',
    accep_result           string comment '验收结果',
    is_req                 string comment '是否符合要求',
    equ_tire               string comment '设备轮胎破损情况',
    equ_main               string comment '设备各主要部件情况',
    dev_s_date             string comment '设备启用日期',
    equ_oupput             string comment '设备里程表/计时器/总产量初始值',
    equ_last_date          string comment '设备最后一次保养时间或日期',
    equ_tank               string comment '设备燃油箱油量初始值(L)',
    accep_com              string comment '验收意见',
    safe_content           string comment '安全教育内容',
    mach_manage            string comment '机械管理制度和相关要求的告知',
    exit_date              string comment '退场日期',
    exit_basis             string comment '退场依据',
    cum_time               string comment '累计使用时长',
    start_time             string comment '开始使用时间',
    end_time               string comment '结束使用时间',
    explain                string comment '说明',
    stop_att_date          string comment '停止考勤日期',
    ctime                  string comment '创建时间',
    mtime                  string comment '修改时间',
    allo_output_value      Decimal(20, 6) comment '调配产值',
    group_lease            string comment '集团内租赁进场',
    mana_unitid            string comment '管理单位id',
    mana_unitname          string comment '管理单位名称',
    rent_unitid            string comment '租用单位id',
    rent_unitname          string comment '租用单位名称',
    cz_unitid              string comment '出租单位id',
    cz_unitname            string comment '出租单位名称',
    opp_unit_id            string comment '对方单位id',
    opp_unit_name          string comment '对方单位名称',
    fill_unitid            string comment '填报单位id',
    fill_unitname          string comment '填报单位名称',
    lease_price            Decimal(20, 6) comment '租赁单价',
    manager_code           string comment '管理编号',
    start_date             string comment '开始日期',
    etl_time               string comment 'etl_时间',
    source_system          string comment '来源系统',
    source_table           string comment '来源表名'
) comment '装备资源管理_进退场信息'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_进退场信息
with t1 as (select *
            from ods.ods_cccc_erms_use_entry_exit_info_i_d
            where end_date = '2999-12-31'
              and isdelete = '0'),
     t2 as (select asid,
                   cnname,
                   mastercode
            from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
            where end_date = '2999-12-31'),
     dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname = '进退场装备来源'),
     mapping as (select equtype_code,
                        equtype_name
                 from dwd.dim_erms_equip_type_mapping_d),
     org as (select oid
                  , oname
                  , second_unit_id
                  , second_unit_name
                  , third_unit_id
                  , third_unit_name
             from dwd.dim_erms_orgext_d)
-- insert overwrite table dwd.dwd_erms_use_entry_exit_info_d partition(etl_date = '${etl_date}')
select t1.id                                                  as eeid                   --进退场id
     , t1.asid                                                as asid                   --资产id
     , t2.mastercode                                          as equ_mastercode         --装备主数据编码
     , t1.disid                                               as disid                  --调遣单id
     , t1.zproject                                            as zproject               --项目主数据id
     , t1.project                                             as project_name           --所属项目名称
     , t1.equ_sour                                            as equ_sour_code          --装备来源编码
     , t3.diname                                              as equ_sour_name          --装备来源名称
     , t1.equ_code                                            as equ_code               --装备编码
     , t1.equ_name                                            as equ_name               --装备名称
     , t1.equtype                                             as equtype_code           --装备类型编码
     , mapping.equtype_name                                   as equtype_name           --装备类型名称
     , t1.equ_class                                           as equ_class              --装备分类
     , t1.license                                             as license                --车牌号
     , t1.driver                                              as driver                 --机驾人员
     , t1.equ_tool                                            as equ_tool               --设备随机工具
     , t1.tech_status                                         as tech_status            --技术状况
     , t1.equ_data                                            as equ_data               --设备随机资料
     , t1.entry_exit_status                                   as entry_exit_status_code --进退场状态编码
     , case
           when t1.entry_exit_status = '01' then '未进场'
           when t1.entry_exit_status = '02' then '已进场'
           when t1.entry_exit_status = '03' then '已退场' end as entry_exit_status_name --进退场状态名称
     , t1.mob_basic                                           as mob_basic              --进场依据
     , t1.acceptor                                            as acceptor_name          --验收人名称
     , t1.transfer                                            as transfer_name          --移交人名称
     , t1.safety                                              as safety_name            --安全员名称
     , t1.prepared                                            as prepared_name          --编制人名称
     , t1.accep_date                                          as accep_date             --验收日期
     , t1.accep_result                                        as accep_result           --验收结果
     , t1.is_req                                              as is_req                 --是否符合要求
     , t1.equ_tire                                            as equ_tire               --设备轮胎破损情况
     , t1.equ_main                                            as equ_main               --设备各主要部件情况
     , t1.dev_s_date                                          as dev_s_date             --设备启用日期
     , t1.equ_oupput                                          as equ_oupput             --设备里程表/计时器/总产量初始值
     , t1.equ_last_date                                       as equ_last_date          --设备最后一次保养时间或日期
     , t1.equ_tank                                            as equ_tank               --设备燃油箱油量初始值(L)
     , t1.accep_com                                           as accep_com              --验收意见
     , t1.safe_content                                        as safe_content           --安全教育内容
     , t1.mach_manage                                         as mach_manage            --机械管理制度和相关要求的告知
     , t1.exit_date                                           as exit_date              --退场日期
     , t1.exit_basis                                          as exit_basis             --退场依据
     , t1.cum_time                                            as cum_time               --累计使用时长
     , t1.start_time                                          as start_time             --开始使用时间
     , t1.end_time                                            as end_time               --结束使用时间
     , t1.explain                                             as explain                --说明
     , t1.stop_att_date                                       as stop_att_date          --停止考勤日期
     , t1.ctime                                               as ctime                  --创建时间
     , t1.mtime                                               as mtime                  --修改时间
     , t1.allo_output_value                                   as allo_output_value      --调配产值
     , t1.group_lease                                         as group_lease            --集团内租赁进场
     , t1.mana_unit                                           as mana_unitid            --管理单位id
     , t5.oname                                               as mana_unitname          --管理单位名称
     , t1.rental_unit                                         as rent_unitid            --租用单位id
     , t6.oname                                               as rent_unitname          --租用单位名称
     , t1.cz_unit                                             as cz_unitid              --出租单位id
     , t7.oname                                               as cz_unitname            --出租单位名称
     , t1.opp_unit                                            as opp_unit_id            --对方单位id
     , t1.opp_unit_name                                       as opp_unit_name          --对方单位名称
     , t1.formcoid                                            as fill_unitid            --填报单位id
     , t8.oname                                               as fill_unitname          --填报单位名称
     , t1.lease_price                                         as lease_price            --租赁单价
     , t1.code                                                as manager_code           --管理编号
     , t1.start_date                                          as start_date             --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_use_entry_exit_info_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d,dim_erms_dictitem_d,dim_erms_equip_type_mapping_d,dim_erms_orgext_d'
                                                              as source_table
from t1
         left join t2 on t1.asid = t2.asid
         left join dict t3 on t1.equ_sour = t3.dicode and t3.dname = '进退场装备来源'
         left join mapping on t1.equtype = mapping.equtype_code
         left join org t5 on t1.mana_unit = t5.oid
         left join org t6 on t1.rental_unit = t6.oid
         left join org t7 on t1.cz_unit = t7.oid
         left join org t8 on t1.formcoid = t8.oid
;