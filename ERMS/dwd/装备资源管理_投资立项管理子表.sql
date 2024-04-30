drop table dwd.dwd_erms_invm_plan_proapply_chil_d;
create table dwd.dwd_erms_invm_plan_proapply_chil_d
(
    ppcid                      string comment '立项子表id',
    equtype_code               string comment '装备类型编码',
    equtype_name               string comment '装备类型名称',
    invtype_code               string comment '投资方式编码',
    invtype_name               string comment '投资方式名称',
    equnum                     Decimal(38, 0) comment '投资装备数量',
    invamount                  Decimal(20, 6) comment '总体投资金额(万元)',
    ctime                      string comment '创建时间',
    mtime                      string comment '修改时间',
    plid                       string comment '投资计划id',
    majorequ                   string comment '装备分类',
    approve_state_code         string comment '审批状态编码',
    approve_state_name         string comment '审批状态名称',
    ispass                     string comment '是否通过',
    isimport                   string comment '状态位是否被引用',
    isjtimport                 string comment '集团是否引用',
    endfinishwork              Decimal(8, 4) comment '累计工程量计划完成比(%)',
    endfinishamount            Decimal(19, 6) comment '累计完成投资金额(万元)',
    new_fill_cycle             string comment '最新填报周期',
    planprogress_code          string comment '投资进展状态编码',
    planprogress_name          string comment '投资进展状态名称',
--     glppcid              string comment '关联其他系统的立项申报子表id',
--     receivetime          string comment '接收时间',
    fill_unit_id               string comment '填报单位id',
    fill_unit_name             string comment '填报单位名称',
--     dj_unitid            string comment '对接单位id',
--     dj_unitname          string comment '对接单位名称',
    fill_second_unit_id        string comment '填报二级单位id',
    fill_second_unit_shortname string comment '填报二级单位简称',
    start_date                 string comment '开始日期',
    etl_time                   string comment 'etl_时间',
    source_system              string comment '来源系统',
    source_table               string comment '来源表名'
) comment '装备资源管理_投资立项管理子表'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_投资立项管理子表
with t1 as (select *
            from ods.ods_cccc_erms_invm_plan_proapply_chil_i_d
            where end_date = '2999-12-31'),
     mapping as (select equtype_code, equtype_name
                 from dwd.dim_erms_equip_type_mapping_d),
     dict as (select dcode,
                     dname,
                     dicode,
                     diname
              from dwd.dim_erms_dictitem_d
              where dname in ('投资方式', '投资立项状态')),
     org as (select oid
                  , oname
                  , second_unit_id
                  , second_unit_name
                  , third_unit_id
                  , third_unit_name
             from dwd.dim_erms_orgext_d)
--insert overwrite table dwd.dwd_erms_invm_plan_proapply_chil_d partition ( etl_date = '${etl_date}' )
select t1.ppcid                                                                                      as ppcid                      --立项子表id
     , t1.equtype                                                                                    as equtype_code               --装备类型编码
     , mapping.equtype_name                                                                          as equtype_name               --装备类型名称                                                                               as equtype_name         --装备类型名称
     , t1.invtype                                                                                    as invtype_code               --投资方式编码
     , t2.diname                                                                                     as invtype_name               --投资方式名称
     , t1.equamount                                                                                  as equnum                     --投资装备数量
     , t1.invamount                                                                                  as invamount                  --总体投资金额(万元)
     , t1.ctime                                                                                      as ctime                      --创建时间
     , t1.mtime                                                                                      as mtime                      --修改时间
     , t1.plid                                                                                       as plid                       --投资计划id
     , t1.majorequ                                                                                   as majorequ                   --装备分类
     , t1.state                                                                                      as approve_state_code         --审批状态编码
     , t3.diname                                                                                     as approve_state_name         --审批状态名称
     , t1.ispass                                                                                     as ispass                     --是否通过
     , t1.isimport                                                                                   as isimport                   --状态位是否被引用
     , t1.isjtimport                                                                                 as isjtimport                 --集团是否引用
     , t1.endfinishwork                                                                              as endfinishwork              --累计工程量计划完成比(%)
     , t1.endfinishamount                                                                            as endfinishamount            --累计完成投资金额(万元)
     , t1.newdata                                                                                    as new_fill_cycle             --最新填报周期
     , t1.planprogress                                                                               as planprogress_code          --投资进展状态编码
     , case
           when t1.planprogress = '0' then '未完成'
           when t1.planprogress = '1' then '已完成'
    end                                                                                              as planprogress_name          --投资进展状态名称
--      , t1.glppcid                                                                     as glppcid              --关联其他系统的立项申报子表id
--      , t1.receivetime                                                                 as receivetime          --接收时间
     , t1.coid                                                                                       as fill_unit_id               --填报单位id
     , t1.coidname                                                                                   as fill_unit_name             --填报单位名称
--      , t1.djcoid                                                                      as dj_unitid            --对接单位id
--      , org.oname                                                                      as dj_unitname          --对接单位名称
     , t1.secondcoid                                                                                 as fill_second_unit_id        --填报二级单位id
     , t1.secondname                                                                                 as fill_second_unit_shortname --填报二级单位简称
     , t1.start_date                                                                                 as start_date                 --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                                        as etl_time
     , 'ERMS'                                                                                        as source_system
     , 'ods_cccc_erms_invm_plan_proapply_chil_i_d,dim_erms_equip_type_mapping_d,dim_erms_dictitem_d' as source_table
from t1
         left join mapping on t1.equtype = mapping.equtype_code
         left join dict t2 on t1.invtype = t2.dicode and t2.dname = '投资方式'
         left join dict t3 on t1.state = t3.dicode and t3.dname = '投资立项状态'
--  left join org on t1.djcoid = org.oid
;
