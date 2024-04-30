--装备资源管理_采购计划
create table dwd.dwd_erms_invm_plan_year_plan_supply_d
(
    psid            string comment '采购id',
    year            string comment '年份',
    pcode           string comment '计划编号',
    pk_firstplan    Decimal(38, 0) comment '计划首版PK',
    pk_upplan       Decimal(38, 0) comment '计划上版PK',
    vernum          Decimal(38, 0) comment '版本号',
    isfirstver      string comment '是否第一版本',
    isnewver        string comment '是否最新版本',
    iscurtver       string comment '是否当前版本',
    pnote           string comment '推送内容',
    pmonth          string comment '计划年月(时间格式：YYYY-MM)',
    ptype_code      string comment '采购方式编码',
    ptype_name      string comment '采购方式名称',
    ps_4a_id        string comment '采购平台4A编码',
    ps_name         string comment '采购平台名称',
    pk_purplan      Decimal(38, 0) comment '采购计划PK，19位数字',
    pclass_code     string comment '计划分类编码',
    pclass_name     string comment '计划分类名称',
    pname           string comment '计划名称',
    totamt          Decimal(20, 6) comment '计划总金额',
    bz_code         string comment '币种编码',
    bz_name         string comment '币种名称',
    exchangerate    Decimal(38, 0) comment '汇率',
    adjust_reason   string comment '调整原因',
    ispushed        string comment '是否已推送',
    remark          string comment '备注',
    ptypeclass_code string comment '采购分类编码',
    ptypeclass_name string comment '采购分类名称',
    fill_unit_id    string comment '填报单位id(4A编码)',
    fill_unit_name  string comment '填报单位名称',
    creationtime    string comment '录入时间',
    modifiedtime    string comment '修改时间',
    psstatus_code   string comment '计划状态编码',
    psstatus_name   string comment '计划状态名称',
    apprresult_code string comment '审批结果编码',
    apprresult_name string comment '审批结果名称',
    start_date      string comment '开始日期',
    etl_time        string comment 'etl_时间',
    source_system   string comment '来源系统',
    source_table    string comment '来源表名'
) comment '装备资源管理_采购计划'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_采购计划
with dict as (select dcode, dname, dicode, diname
              from dwd.dim_erms_dictitem_d
              where dname in ('采购方式', '采购分类', '')),
     t1 as (select *
            from ods.ods_cccc_erms_invm_plan_year_plan_supply_i_d
            where end_date = '2999-12-31'
              and isdelete != '1')
--insert overwrite table dwd.dwd_erms_invm_plan_year_plan_supply_d partition(etl_date = '${etl_date}')
select t1.id                                                              as psid            --采购id
     , t1.year                                                            as year            --年份
     , t1.pcode                                                           as pcode           --计划编号
     , t1.pk_firstplan                                                    as pk_firstplan    --计划首版PK
     , t1.pk_upplan                                                       as pk_upplan       --计划上版PK
     , t1.vernum                                                          as vernum          --版本号
     , t1.isfirstver                                                      as isfirstver      --是否第一版本
     , t1.isnewver                                                        as isnewver        --是否最新版本
     , t1.iscurtver                                                       as iscurtver       --是否当前版本
     , t1.pnote                                                           as pnote           --推送内容
     , t1.ptime                                                           as pmonth          --计划年月(时间格式：YYYY-MM)
     , t1.ptype                                                           as ptype_code      --采购方式编码
     , t1.ptype                                                           as ptype_name      --采购方式名称
     , t1.pk_pspk                                                         as ps_4a_id        --采购平台4A编码
     , t1.psname                                                          as ps_name         --采购平台名称
     , t1.pk_purplan                                                      as pk_purplan      --采购计划PK，19位数字
     , t1.pclass                                                          as pclass_code     --计划分类编码
     , t1.pclass                                                          as pclass_name     --计划分类名称
     , t1.pname                                                           as pname           --计划名称
     , t1.totamt                                                          as totamt          --计划总金额
     , t1.moneytype                                                       as bz_code         --币种编码
     , t1.moneytype                                                       as bz_name         --币种名称
     , t1.exchangerate                                                    as exchangerate    --汇率
     , t1.adjust_reason                                                   as adjust_reason   --调整原因
     , t1.ispushed                                                        as ispushed        --是否已推送
     , t1.remark                                                          as remark          --备注
     , t1.ptypeclass                                                      as ptypeclass_code --采购分类编码
     , t1.ptypeclass                                                      as ptypeclass_name --采购分类名称
     , t1.pk_opunitpk                                                     as fill_unit_id    --填报单位id(4A编码)
     , t1.opunitname                                                      as fill_unit_name  --填报单位名称
     , t1.creationtime                                                    as creationtime    --录入时间
     , t1.modifiedtime                                                    as modifiedtime    --修改时间
     , t1.psstatus                                                        as psstatus_code   --计划状态编码
     , t1.psstatus                                                        as psstatus_name   --计划状态名称
     , t1.apprresult                                                      as apprresult_code --审批结果编码
     , t1.apprresult                                                      as apprresult_name --审批结果名称
     , t1.start_date                                                      as start_date      --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')             as etl_time
     , 'ERMS'                                                             as source_system
     , 'dim_erms_dictitem_d,ods_cccc_erms_invm_plan_year_plan_supply_i_d' as source_table
from t1
         left join dict
;