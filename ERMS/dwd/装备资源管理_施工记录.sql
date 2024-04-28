create table dwd.dwd_erms_use_construction_info_d
(
    ciid          string comment '施工记录id',
    eeid          string comment '进退场id',
    period        string comment '统计期次(YYYY-MM)',
    oilwearnum    Decimal(10, 2) comment '油耗小计(升)',
    fuelmoneynum  Decimal(19, 2) comment '燃润料消耗小计(元)',
    countnum      Decimal(10, 2) comment '台班总数',
    precountnum   Decimal(10, 2) comment '额定台班总数',
    availability  Decimal(5, 2) comment '利用率',
    ctime         string comment '创建时间',
    mtime         string comment '修改时间',
    start_date    string comment '开始日期',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_施工记录'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_施工记录
with t1 as (select *
            from ods.ods_cccc_erms_use_construction_info_i_d
            where end_date = '2999-12-31')
-- insert overwrite table dwd.dwd_erms_use_construction_info_d partition(etl_date = '${etl_date}')
select t1.ciid                                                as ciid         --施工记录id
     , t1.eeid                                                as eeid         --进退场id
     , t1.period                                              as period       --统计期次(YYYY-MM)
     , t1.oilwearnum                                          as oilwearnum   --油耗小计(升)
     , t1.fuelmoneynum                                        as fuelmoneynum --燃润料消耗小计(元)
     , t1.countnum                                            as countnum     --台班总数
     , t1.precountnum                                         as precountnum  --额定台班总数
     , t1.availability                                        as availability --利用率
     , t1.ctime                                               as ctime        --创建时间
     , t1.mtime                                               as mtime        --修改时间
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time
     , t1.start_date                                          as start_date   --开始日期
     , 'ERMS'                                                 as source_system
     , 'ods_cccc_erms_use_construction_info_i_d'              as source_table
from t1
;