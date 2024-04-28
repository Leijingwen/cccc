--装备资源管理_字典详情
drop table dwd.dim_erms_dictitem_d;

create table dwd.dim_erms_dictitem_d
(
    did           string comment '字典目录id',
    pdid          string comment '上级字典目录id',
    drule         string comment '字典目录规则码',
    dcode         string comment '字典目录码',
    dname         string comment '字典目录名称',
    dtype         string comment '字典目录分类',
    dgrade        string comment '字典目录等级',
    dsno          string comment '字典目录序号',
    diid          string comment '字典详情id',
    pdiid         string comment '上级字典详情id',
    dirule        string comment '字典详情规则码',
    dicode        string comment '字典详情编码',
    diname        string comment '字典详情名称',
    digrade       decimal(38, 0) comment '字典详情等级',
    disno         decimal(38, 0) comment '字典详情序号',
    dinote        string comment '字典详情备注',
    etl_time      string comment 'etl_时间',
    source_system string comment '来源系统',
    source_table  string comment '来源表名'
) comment '装备资源管理_字典详情'
    stored as orc;

with a as (select * from ods.ods_cccc_erms_waf_core_dictitem_f_d where status = '1'),
     b as (select * from ods.ods_cccc_erms_waf_core_dict_f_d)
-- insert overwrite table dwd.dim_erms_dictitem_d
select a.did                                                                 as did,     --字典目录id
       b.pdid                                                                as pdid,    --上级字典目录id
       b.drule                                                               as drule,   --字典目录规则码
       b.code                                                                as dcode,   --字典目录码
       b.name                                                                as dname,   --字典目录名称
       b.type                                                                as dtype,   --字典目录分类
       b.grade                                                               as dgrade,  --字典目录等级
       b.sno                                                                 as dsno,    --字典目录序号
       a.diid                                                                as diid,    --字典详情id
       a.pdiid                                                               as pdiid,   --上级字典详情id
       a.dirule                                                              as dirule,  --字典详情规则码
       a.code                                                                as dicode,  --字典详情编码
       a.name                                                                as diname,  --字典详情名称
       a.grade                                                               as digrade, --字典详情等级
       a.sno                                                                 as disno,   --字典详情序号
       a.note                                                                as dinote,  --字典详情备注
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                as etl_time,
       'ERMS'                                                                as source_system,
       'ods_cccc_erms_waf_core_dictitem_f_d,ods_cccc_erms_waf_core_dict_f_d' as source_table
from a
         left join b on a.did = b.did;

select * from dwd.dim_erms_dictitem_d where dname like '%币种%';