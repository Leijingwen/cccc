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
    fillunit_4a_id  string comment '填报单位编号(4A编码)',
    fillunit_name   string comment '填报单位名称',
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
              where dname in ('采购方式', '采购分类', '审批结果','币种')),
     a as (select *
           from ods.ods_cccc_erms_invm_plan_year_plan_supply_i_d
           where end_date = '2999-12-31'
             and isdelete != '1')
insert overwrite table dwd.dwd_erms_invm_plan_year_plan_supply_d partition(etl_date = '${etl_date}')
select a.id                                                               as psid            --采购id
     , a.year                                                             as year            --年份
     , a.pcode                                                            as pcode           --计划编号
     , a.pk_firstplan                                                     as pk_firstplan    --计划首版PK
     , a.pk_upplan                                                        as pk_upplan       --计划上版PK
     , a.vernum                                                           as vernum          --版本号
     , a.isfirstver                                                       as isfirstver      --是否第一版本
     , a.isnewver                                                         as isnewver        --是否最新版本
     , a.iscurtver                                                        as iscurtver       --是否当前版本
     , a.pnote                                                            as pnote           --推送内容
     , a.ptime                                                            as pmonth          --计划年月
     , a.ptype                                                            as ptype_code      --采购方式编码
     , dict_cgfs.diname                                                   as ptype_name      --采购方式名称(非直取,请注意查看文档进行调整)
     , a.pk_pspk                                                          as ps_4a_id        --采购平台4A编码
     , a.psname                                                           as ps_name         --采购平台名称
     , a.pk_purplan                                                       as pk_purplan      --采购计划PK，19位数字
     , a.pclass                                                           as pclass_code     --计划分类编码
     , a.pclass                                                           as pclass_name     --计划分类名称
     , a.pname                                                            as pname           --计划名称
     , a.totamt                                                           as totamt          --计划总金额
     , a.moneytype                                                        as bz_code         --币种编码
     , dict_bz.diname                                                     as bz_name         --币种名称(非直取,请注意查看文档进行调整)
     , a.exchangerate                                                     as exchangerate    --汇率
     , a.adjust_reason                                                    as adjust_reason   --调整原因
     , a.ispushed                                                         as ispushed        --是否已推送
     , a.remark                                                           as remark          --备注
     , a.ptypeclass                                                       as ptypeclass_code --采购分类编码
     , dict_cgfl.diname                                                   as ptypeclass_name --采购分类名称(非直取,请注意查看文档进行调整)
     , a.pk_opunitpk                                                      as fillunit_4a_id  --填报单位编号
     , a.opunitname                                                       as fillunit_name   --填报单位名称
     , a.creationtime                                                     as creationtime    --录入时间
     , a.modifiedtime                                                     as modifiedtime    --修改时间
     , a.psstatus                                                         as psstatus_code   --计划状态编码
     , case
           when a.psstatus = '0' then '编辑中'
           when a.psstatus = '1' then '已生效'
           when a.psstatus = '2' then '调整中'
           when a.psstatus = '3' then '审批中'
           when a.psstatus = '4' then '已完成'
           when a.psstatus = '5' then '不通过'
           when a.psstatus = '6' then '历史版'
    end                                                                   as psstatus_name   --计划状态名称(非直取,请注意查看文档进行调整)
     , a.apprresult                                                       as apprresult_code --审批结果编码
     , dict_spjg.diname                                                   as apprresult_name --审批结果名称(非直取,请注意查看文档进行调整)
     , a.start_date                                                       as start_date      --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')             as etl_time
     , 'ERMS'                                                             as source_system
     , 'dim_erms_dictitem_d,ods_cccc_erms_invm_plan_year_plan_supply_i_d' as source_table
from a
         left join dict dict_cgfs on a.ptype = dict_cgfs.dicode and dict_cgfs.dname = '采购方式'
         left join dict dict_bz on a.moneytype = dict_bz.dicode and dict_bz.dname = '币种'
         left join dict dict_cgfl on a.ptypeclass = dict_cgfl.dicode and dict_cgfl.dname = '采购分类'
         left join dict dict_spjg on a.apprresult = dict_spjg.dicode and dict_spjg.dname = '审批结果'
;