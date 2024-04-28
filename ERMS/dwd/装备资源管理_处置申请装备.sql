create table dwd.dwd_erms_handle_dis_apply_equ_d
(
    hdeid             string comment '处置申请装备id',
    hdaid             string comment '处置申请id',
    asid              string comment '装备asid',
    equ_mastercode    string comment '装备主数据编码',
    equcode           string comment '装备编码',
    equname           string comment '装备名称',
    equtype_code      string comment '装备类型编码',
    equtype_name      string comment '装备类型名称',
    model             string comment '规格型号',
    equnum            Decimal(38, 0) comment '数量（台/套）',
    ovalue            Decimal(19, 6) comment '原值（元）',
    nvalue            Decimal(19, 6) comment '净值（元）',
    dvalue            Decimal(19, 6) comment '估计残值/评估价',
    depr              Decimal(38, 0) comment '装备年限',
    manufacturer_name string comment '建造/生产厂家名称',
    allyear           Decimal(38, 0) comment '全部耐用年限',
    ctime             string comment '创建时间',
    mtime             string comment '修改时间',
    depamount         Decimal(19, 6) comment '折旧金额',
    manager_code      string comment '管理编号',
    srorgname         string comment '受让单位名称',
    start_date        string comment '开始日期',
    etl_time          string comment 'etl_时间',
    source_system     string comment '来源系统',
    source_table      string comment '来源表名'
) comment '装备资源管理_处置申请装备'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;

--装备资源管理_处置申请装备
with dis_app_equ as (select *
                     from ods.ods_cccc_erms_handle_dis_apply_equ_i_d
                     where end_date = '2999-12-31' and isdelete != '1'),
     pub as (select asid,
                    cnname,
                    mastercode
             from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
             where end_date = '2999-12-31'),
     etype as (select *
               from dwd.dim_erms_equip_type_mapping_d)
insert overwrite table dwd.dwd_erms_handle_dis_apply_equ_d partition(etl_date = '${etl_date}')
select dis_app_equ.hdeid                                                              as hdeid             --处置申请装备id
     , dis_app_equ.hdaid                                                              as hdaid             --处置申请id
     , dis_app_equ.asid                                                               as asid              --装备asid
     , pub.mastercode                                                                 as equ_mastercode    --装备主数据编码
     , dis_app_equ.equcode                                                            as equcode           --装备编码
     , dis_app_equ.equname                                                            as equname           --装备名称
     , dis_app_equ.equtype                                                            as equtype_code      --装备类型编码
     , etype.equtype_name                                                             as equtype_name      --装备类型名称
     , dis_app_equ.model                                                              as model             --规格型号
     , dis_app_equ.equamount                                                          as equnum            --数量（台/套）
     , dis_app_equ.ovalue                                                             as ovalue            --原值（元）
     , dis_app_equ.nvalue                                                             as nvalue            --净值（元）
     , dis_app_equ.dvalue                                                             as dvalue            --估计残值/评估价
     , dis_app_equ.depr                                                               as depr              --装备年限
     , dis_app_equ.manufacturer                                                       as manufacturer_name --建造/生产厂家名称
     , dis_app_equ.allyear                                                            as allyear           --全部耐用年限
     , dis_app_equ.ctime                                                              as ctime             --创建时间
     , dis_app_equ.mtime                                                              as mtime             --修改时间
     , dis_app_equ.depamount                                                          as depamount         --折旧金额
     , dis_app_equ.code                                                               as manager_code      --管理编号
     , dis_app_equ.srorgname                                                          as srorgname         --受让单位名称
     , dis_app_equ.start_date                                                         as start_date        --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                         as etl_time
     , 'ERMS'                                                                         as source_system
     , 'ods_cccc_erms_handle_dis_apply_equ_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d,dim_erms_equip_type_mapping_d' as source_table
from dis_app_equ
         left join pub on dis_app_equ.asid = pub.asid
         left join etype on dis_app_equ.equtype = etype.equtype_code;
