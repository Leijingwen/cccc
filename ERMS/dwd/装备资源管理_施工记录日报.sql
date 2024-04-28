create table dwd.dwd_erms_use_construction_day_info_d
(
    cdiid            string comment '施工记录日报id',
    ciid             string comment '施工记录id',
    precount         Decimal(10, 2) comment '额定台班总数',
    count            Decimal(10, 2) comment '台班总数',
    oilwear          Decimal(10, 2) comment '油耗(升)',
    fuelmoney        Decimal(19, 2) comment '燃润料消耗(元)',
    ctime            string comment '创建时间',
    mtime            string comment '修改时间',
    constructiondate string comment '施工日期',
    workcont         string comment '工作内容',
    desquestion      string comment '问题描述',
    precostmoney     Decimal(19, 2) comment '成本金额（不含税）',
    costmoney        Decimal(19, 2) comment '成本金额（含税）',
    dddc             Decimal(19, 2) comment '消耗金额（含税）',
    predddc          Decimal(19, 2) comment '消耗金额（不含税）',
    deploymoney      Decimal(19, 2) comment '调配产值',
    start_date       string comment '开始日期',
    etl_time         string comment 'etl_时间',
    source_system    string comment '来源系统',
    source_table     string comment '来源表名'
) comment '装备资源管理_施工记录日报'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_施工记录日报
with t1 as (select *
            from ods.ods_cccc_erms_use_construction_day_info_i_d
            where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_use_construction_day_info_d partition(etl_date = '${etl_date}')
select t1.cdiid                                               as cdiid            --施工记录日报id
     , t1.ciid                                                as ciid             --施工记录id
     , t1.precount                                            as precount         --额定台班总数
     , t1.count                                               as count            --台班总数
     , t1.oilwear                                             as oilwear          --油耗(升)
     , t1.fuelmoney                                           as fuelmoney        --燃润料消耗(元)
     , t1.ctime                                               as ctime            --创建时间
     , t1.mtime                                               as mtime            --修改时间
     , t1.constructiondate                                    as constructiondate --施工日期
     , t1.workcont                                            as workcont         --工作内容
     , t1.desquestion                                         as desquestion      --问题描述
     , t1.precostmoney                                        as precostmoney     --成本金额（不含税）
     , t1.costmoney                                           as costmoney        --成本金额（含税）
     , t1.dddc                                                as dddc             --消耗金额（含税）
     , t1.predddc                                             as predddc          --消耗金额（不含税）
     , t1.deploymoney                                         as deploymoney      --调配产值
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , t1.start_date                                          as start_date       --开始日期
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_use_construction_day_info_i_d'          as source_table
from t1
;