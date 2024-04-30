--装备资源管理_装备附件表
create table dwd.dwd_erms_waf_core_attach_info_d
(
    attach_id           string comment '附件id',
    asid                string comment '资产id',
    equ_mastercode      string comment '装备主数据编码',
    bizid               string comment '业务ID',
    fieldname           string comment '附件标识，具体功能模块',
    filename            string comment '附件上传后的名称',
    filesize            string comment '文件大小',
    filepath            string comment '附件服务器路径',
    localfilename       string comment '附件原名称',
    uploadtime          string comment '上传时间',
    flag                string comment '标识',
    creatorname         string comment '上传人姓名',
    base64              string comment 'base64码',
    version             Decimal(38, 0) comment '版本号',
    downloadurl         string comment '下载地址',
    modifytime          string comment '修改时间',
    sno                 Decimal(38, 0) comment '序号(用于首页附件排序)',
    djcoid              string comment '对接单位的coid',
    modelname           string comment '功能模块名称',
    glid                string comment '关联业务id',
    dj_second_unit_id   string comment '对接二级单位id',
    dj_second_unit_name string comment '对接二级单位名称',
    dj_third_unit_id    string comment '对接三级单位id',
    dj_third_unit_name  string comment '对接三级单位名称',
    start_date          string comment '开始日期',
    etl_time            string comment 'etl_时间',
    source_system       string comment '来源系统',
    source_table        string comment '来源表名'
) comment '装备资源管理_装备附件表'
    PARTITIONED BY ( etl_date string COMMENT '分区字段')
    STORED AS ORC;


--装备资源管理_装备附件表
with org_rule as (select *
                  from dwd.dim_erms_orgext_d),
     a as (select *
           from ods.ods_cccc_erms_waf_core_attach_info_i_d
           where end_date = '2999-12-31'
             and isdelete != '1'),
     c as (select *
           from ods.ods_cccc_erms_base_pub_assetsinfo_i_d
           where end_date = '2999-12-31')
--insert overwrite table dwd.dwd_erms_waf_core_attach_info_d partition(etl_date = '${etl_date}')
select a.id                                                                            as attach_id           --附件id
     , regexp_replace(a.bizid, '_photo', '')                                           as asid                --资产id(非直取,请注意查看文档进行调整)
     , c.mastercode                                                                    as equ_mastercode      --装备主数据编码(非直取,请注意查看文档进行调整)
     , a.bizid                                                                         as bizid               --业务ID
     , a.fieldname                                                                     as fieldname           --附件标识，具体功能模块
     , a.filename                                                                      as filename            --附件上传后的名称
     , a.filesize                                                                      as filesize            --文件大小
     , a.filepath                                                                      as filepath            --附件服务器路径
     , a.localfilename                                                                 as localfilename       --附件原名称
     , a.uploadtime                                                                    as uploadtime          --上传时间
     , a.flag                                                                          as flag                --标识
     , a.creatorname                                                                   as creatorname         --上传人姓名
     , a.base64                                                                        as base64              --base64码
     , a.version                                                                       as version             --版本号
     , a.downloadurl                                                                   as downloadurl         --下载地址
     , a.modifytime                                                                    as modifytime          --修改时间
     , a.sno                                                                           as sno                 --序号(用于首页附件排序
     , a.djcoid                                                                        as djcoid              --对接单位的coid
     , a.modelname                                                                     as modelname           --功能模块名称
     , a.glid                                                                          as glid                --关联业务id
     , org_rule.second_unit_id                                                         as dj_second_unit_id   --对接二级单位id(非直取,请注意查看文档进行调整)
     , org_rule.second_unit_name                                                       as dj_second_unit_name --对接二级单位名称(非直取,请注意查看文档进行调整)
     , org_rule.third_unit_id                                                          as dj_third_unit_id    --对接三级单位id(非直取,请注意查看文档进行调整)
     , org_rule.third_unit_name                                                        as dj_third_unit_name  --对接三级单位名称(非直取,请注意查看文档进行调整)
     , a.start_date                                                                    as start_date          --开始日期
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                          as etl_time
     , 'ERMS'                                                                          as source_system
     , 'dim_erms_orgext_d,ods_cccc_erms_waf_core_attach_info_i_d,ods_cccc_erms_base_pub_assetsinfo_i_d' as source_table
from a
         left join c on a.id = c.asid
         left join org_rule on a.glid = org_rule.oid
;