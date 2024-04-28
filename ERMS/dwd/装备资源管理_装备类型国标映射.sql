-- 装备资源管理_装备类型国标映射
create table dwd.dim_erms_equip_type_mapping_d
(
    equtype_code       STRING comment '装备类型编码',
    equtype_name       STRING comment '装备类型名称',
    efinancetype_name  STRING comment '财务分类名称',
    efinancetypeid     STRING comment '财务分类编码',
    enationaltypeid    STRING comment '国标分类编码',
    enationaltype_name STRING comment '国标分类名称',
    etl_time           STRING comment 'etl_时间',
    source_system      STRING comment '来源系统',
    source_table       STRING comment '来源表名'
) comment '装备资源管理_装备类型国标映射'
    stored as orc;


insert overwrite table dwd.dim_erms_equip_type_mapping_d
select a.etypeid                                              as etypeid,         --装备类型编码
       a.etypename                                            as etypename,       --装备类型名称
       a.efinancetype                                         as efinancetype,    --财务分类名称
       a.efinancetypeid                                       as efinancetypeid,  --财务分类编码
       a.enationaltypeid                                      as enationaltypeid, --国标分类编码
       a.enationaltype                                        as enationaltype,   --国标分类名称
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time,
       'ERMS'                                                 as source_system,
       'ODS.ODS_CCCC_ERMS_EQUIP_TYPE_MAPPING_F_D'             as source_table
from ODS.ODS_CCCC_ERMS_EQUIP_TYPE_MAPPING_F_D a;


