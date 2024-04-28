--装备资源管理_租赁装备清单
create table dwd.dwd_erms_out_lease_list_d
(
    llid             string comment '租赁装备清单id',
    asid             string comment '资产id',
    rsid             string comment '租赁装备id',
    equ_mastercode   string comment '装备主数据编码',
    equtype_code     string comment '装备类型编码',
    equtype_name     string comment '装备类型名称',
    spec             string comment '规格型号',
    pap              string comment '能力',
    equ_num          Decimal(38, 0) comment '数量',
    lease_start_date string comment '租赁开始日期',
    lease_end_date   string comment '租赁结束时间',
    amount           Decimal(19, 6) comment '预算费用',
    majorequ         string comment '装备分类',
    ctime            string comment '创建时间',
    mtime            string comment '修改时间',
    ovalue           Decimal(19, 6) comment '原值',
    selecttimes      Decimal(38, 15) comment '已关联数量',
    rentaletype_name string comment '租赁状态名称',
    code             string comment '管理编号',
    hadadd_code      string comment '是否已经被选中编码',
    pro_name         string comment '项目名称',
    prosite_code     string comment '项目地点编码',
    pro_start_date   string comment '项目开始日期',
    pro_end_date     string comment '项目结束日期',
    prosurvey        string comment '项目概况',
    reason           string comment '租赁原因',
    promain          string comment '工程主要内容',
    repstate_code    string comment '审批结果编码',
    repstate_name    string comment '审批结果名称',
    unitprice        Decimal(19, 6) comment '单价',
    unit             string comment '单位',
    zproject         string comment '主数据项目id',
    start_date       string comment '开始日期',
    etl_time         string comment 'etl_时间',
    source_system    string comment '来源系统',
    source_table     string comment '来源表名'
) comment '装备资源管理_租赁装备清单'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_租赁装备清单
with c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31'),
     a as (select *
           from ods.ods_cccc_erms_out_lease_list_i_d
           where end_date = '2999-12-31'
             and isdelete != '1')
--insert overwrite table dwd.dwd_erms_out_lease_list_d partition(etl_date = '${etl_date}')
select a.llid                                                                    as llid             --租赁装备清单id
     , a.asid                                                                    as asid             --资产id
     , a.rsid                                                                    as rsid             --租赁装备id
     , c.mastercode                                                              as equ_mastercode   --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.equtype                                                                 as equtype_code     --装备类型编码
     , a.equtypename                                                             as equtype_name     --装备类型名称
     , a.spec                                                                    as spec             --规格型号
     , a.pap                                                                     as pap              --能力
     , a.equnum                                                                  as equ_num          --数量
     , a.leasestartdate                                                          as lease_start_date --租赁开始日期
     , a.leaseenddate                                                            as lease_end_date   --租赁结束时间
     , a.amount                                                                  as amount           --预算费用
     , a.majorequ                                                                as majorequ         --装备分类
     , a.ctime                                                                   as ctime            --创建时间
     , a.mtime                                                                   as mtime            --修改时间
     , a.ovalue                                                                  as ovalue           --原值
     , a.selecttimes                                                             as selecttimes      --已关联数量
     , a.rentaletype                                                             as rentaletype_name --租赁状态名称
     , a.code                                                                    as code             --管理编号
     , a.hadadd                                                                  as hadadd_code      --是否已经被选中编码
     , a.proname                                                                 as pro_name         --项目名称
     , a.prosite                                                                 as prosite_code     --项目地点编码
     , a.prostartdate                                                            as pro_start_date   --项目开始日期
     , a.proenddate                                                              as pro_end_date     --项目结束日期
     , a.prosurvey                                                               as prosurvey        --项目概况
     , a.reason                                                                  as reason           --租赁原因
     , a.promain                                                                 as promain          --工程主要内容
     , a.repstate                                                                as repstate_code    --审批结果编码
     , case
           when a.repstate = '0' then '未审批'
           when a.repstate = '100' then '同意'
           when a.repstate = '-100' then '不同意'
    end                                                                          as repstate_name    --审批结果名称
     , a.unitprice                                                               as unitprice        --单价
     , a.unit                                                                    as unit             --单位
     , a.zproject                                                                as zproject         --主数据项目id
     , a.start_date                                                              as start_date       --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                    as etl_time
     , 'ERMS'                                                                    as source_system
     , ',ods_cccc_erms_base_pub_assetsinfo_i_d,ods_cccc_erms_out_lease_list_i_d' as source_table
from a
         left join c on a.asid = c.asid
;


