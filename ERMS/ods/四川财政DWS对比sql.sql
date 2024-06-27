set parallelism.default = 10;
set 'taskmanager.memory.process.size' = '4g';
set taskmanager.numberOfTaskSlots=2;
use catalog my_paimon_catalog;
set 'table.exec.sink.upsert-materialize' = 'NONE';


-- 1.解决join 中的数据倾斜, 而不是group by 中的数据倾斜
-- 2.group by 造成的数据倾斜可以通过随机前缀,然后再去掉随机前缀的方法
-- 3.flink-sql 可以开启 miniBatch 和 LocalGlobal 功能,DataStreamAPI 需要自己写代码实现:在 keyBy 上游算子数据发送之前，
--      首先在上游算子的本地对数据进行聚合后，再发送到下游，使下游接收到的数据量大大减少，从而使得 keyBy 之后的聚合操作不再是任务的瓶颈。
--      类似MapReduce 中 Combiner 的思想，但是这要求聚合操作必须是多条数据或者一批数据才能聚合，单条数据没有办法通过聚合来减少数据量。
--      从 Flink LocalKeyBy 实现原理来讲，必然会存在一个积攒批次的过程，在上游算子中必须攒够一定的数据量，对这些数据聚合后再发送到下游。
-- 4.join 中需要对两边的数据都进行随机前缀, 因为维表中的数据只有1条, 需要进行扩展,随机出很多条 才能做join


create TEMPORARY view DWD_MV as
select *
from `default`.DWD_MV_ELEUNION
where FELEMENTCODE IN ('AGENCY_NO',
                       'MOF_DIV_CODE',
                       'DWD_PAIMON_FADMDIVCODE',
                       'PERSON_IDENTITY');

create TEMPORARY view T as
select t1.MOF_DIV_CODE
     ,t1.NEW_MOF_DIV_CODE
     ,t1.X_PER_ID
     ,t1.BIZ_KEY
     ,t1.IDEN_TYPE_CODE
     ,cast(RAND_INTEGER(100) as String) || '|'|| IDEN_TYPE_CODE NEW_IDEN_TYPE_CODE
     ,t1.IDEN_NO
     ,t1.PER_NAME
     ,t1.SEX_CODE1
     ,t1.NAT_CODE
     ,t1.SCH_REC_CODE1
     ,t1.PER_SOU_CODE1
     ,t1.PER_STA_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.NAT_CODE NEW_NAT_CODE
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.SCH_REC_CODE1 NEW_SCH_REC_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.PER_SOU_CODE1 NEW_PER_SOU_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.PER_STA_CODE1 NEW_PER_STA_CODE1
     ,t1.AGENCY_CODE
     ,t1.WORK_INIT_DATE1
     ,t1.ENTER_AGENCY_DATE
     ,t1.IS_AUTH
     ,t1.PER_IDE_CODE1
     ,t1.SERV_LEN
     ,t1.IS_UNI_SALA
     ,t1.START_DATE
     ,t1.END_DATE
     ,t1.STATE
     ,t1.REDUCE_REASON
     ,t1.UPDATE_TIME
     ,t1.IS_DELETED
     ,t1.CREATE_TIME
     ,t1.FISCAL_YEAR
     ,cast(RAND_INTEGER(100) as String) || '|'|| FISCAL_YEAR NEW_FISCAL_YEAR
     ,t1.POS_CODE1
     ,t1.GR_CODE1
     ,t1.TEC_WORKER_GR_CODE
     ,cast(RAND_INTEGER(100) as String) || '|'|| t1.POS_CODE1  NEW_POS_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'|| t1.GR_CODE1  NEW_GR_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'|| t1.TEC_WORKER_GR_CODE  NEW_TEC_WORKER_GR_CODE
     ,t1.AGENCY_ID
     ,if (LEFT(t1.PER_IDE_CODE1, 1) = '8' or t1.PER_IDE_CODE1 = 'A', '9', coalesce(t1.PER_IDE_CODE1, '9')) as PER_IDE_CODE
     ,if (LEFT(t1.PER_IDE_CODE1, 1) = '8' or t1.PER_IDE_CODE1 = 'A', '其他人员', coalesce(E06.FNAME, '其他人员')) as PER_IDE_NAME
from (select  MOF_DIV_CODE
           ,cast(RAND_INTEGER(100) as String) || '|'|| MOF_DIV_CODE   as NEW_MOF_DIV_CODE
           ,X_PER_ID
           ,BIZ_KEY
           ,IDEN_TYPE_CODE
           ,IDEN_NO
           ,PER_NAME
           ,SEX_CODE1
           ,NAT_CODE
           ,SCH_REC_CODE1
           ,PER_SOU_CODE1
           ,PER_STA_CODE1
           ,AGENCY_CODE
           ,if(INSTR(TRIM(PER_STA_CODE1), '2') <> NULL,ENTER_AGENCY_DATE,WORK_INIT_DATE1) as WORK_INIT_DATE1
           ,ENTER_AGENCY_DATE
           ,IS_AUTH
           ,PER_IDE_CODE1
           ,SERV_LEN
           ,IS_UNI_SALA
           ,START_DATE
           ,END_DATE
           ,STATE
           ,REDUCE_REASON
           ,UPDATE_TIME
           ,IS_DELETED
           ,CREATE_TIME
           ,FISCAL_YEAR
           ,POS_CODE1
           ,GR_CODE1
           ,TEC_WORKER_GR_CODE
           ,AGENCY_ID
      from `default`.DWD_X_BAS_PERSON_INFO
      where SUBSTR(CAST(MOF_DIV_CODE AS STRING), 1, 4) NOT IN ('4603')
        AND CAST(MOF_DIV_CODE AS STRING) NOT IN ('460110000', '469091000')
     ) t1
         left JOIN (
    select FELEMENTCODE,
           FNAME,
           MOF_DIV_CODE,
           MOF_DIV_CODE NEW_MOF_DIV_CODE,
           FCODE
    from DWD_MV
    where FELEMENTCODE = 'PERSON_IDENTITY' and MOF_DIV_CODE <> '510100000'
    union all
    --数据倾斜 MOF_DIV_CODE = '510100000' 复制出8份,+随机前缀 0~7
    select FELEMENTCODE,
           FNAME,
           MOF_DIV_CODE,
           cast(num as string) || '|'|| MOF_DIV_CODE  as NEW_MOF_DIV_CODE,
           FCODE
    from (
             select FELEMENTCODE,
                    FNAME,
                    MOF_DIV_CODE ,
                    array[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99] nums,
                                FCODE
             from DWD_MV
             where FELEMENTCODE = 'PERSON_IDENTITY' and MOF_DIV_CODE = '510100000'
         ) ext , UNNEST(nums) as T ( num )
) E06
                   ON  t1.NEW_MOF_DIV_CODE = E06.NEW_MOF_DIV_CODE
                       AND t1.PER_IDE_CODE1 = E06.FCODE
union all
select    t1.MOF_DIV_CODE
     ,t1.NEW_MOF_DIV_CODE
     ,t1.X_PER_ID
     ,t1.BIZ_KEY
     ,t1.IDEN_TYPE_CODE
     ,cast(RAND_INTEGER(100) as String) || '|'|| IDEN_TYPE_CODE NEW_IDEN_TYPE_CODE
     ,t1.IDEN_NO
     ,t1.PER_NAME
     ,t1.SEX_CODE1
     ,t1.NAT_CODE
     ,t1.SCH_REC_CODE1
     ,t1.PER_SOU_CODE1
     ,t1.PER_STA_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.NAT_CODE NEW_NAT_CODE
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.SCH_REC_CODE1 NEW_SCH_REC_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.PER_SOU_CODE1 NEW_PER_SOU_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'||  t1.PER_STA_CODE1 NEW_PER_STA_CODE1
     ,t1.AGENCY_CODE
     ,t1.WORK_INIT_DATE1
     ,t1.ENTER_AGENCY_DATE
     ,t1.IS_AUTH
     ,t1.PER_IDE_CODE1
     ,t1.SERV_LEN
     ,t1.IS_UNI_SALA
     ,t1.START_DATE
     ,t1.END_DATE
     ,t1.STATE
     ,t1.REDUCE_REASON
     ,t1.UPDATE_TIME
     ,t1.IS_DELETED
     ,t1.CREATE_TIME
     ,t1.FISCAL_YEAR
     ,cast(RAND_INTEGER(100) as String) || '|'|| FISCAL_YEAR NEW_FISCAL_YEAR
     ,t1.POS_CODE1
     ,t1.GR_CODE1
     ,t1.TEC_WORKER_GR_CODE
     ,cast(RAND_INTEGER(100) as String) || '|'|| t1.POS_CODE1  NEW_POS_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'|| t1.GR_CODE1  NEW_GR_CODE1
     ,cast(RAND_INTEGER(100) as String) || '|'|| t1.TEC_WORKER_GR_CODE  NEW_TEC_WORKER_GR_CODE
     ,t1.AGENCY_ID
     ,COALESCE(t1.PER_IDE_CODE1, '9')                                                PER_IDE_CODE
     ,COALESCE(E06.FNAME, '其他人员')                                                     PER_IDE_NAME
from (select  MOF_DIV_CODE
           ,cast(RAND_INTEGER(100) as String) || '|'|| MOF_DIV_CODE   as NEW_MOF_DIV_CODE
           ,X_PER_ID
           ,BIZ_KEY
           ,IDEN_TYPE_CODE
           ,IDEN_NO
           ,PER_NAME
           ,SEX_CODE1
           ,NAT_CODE
           ,SCH_REC_CODE1
           ,PER_SOU_CODE1
           ,PER_STA_CODE1
           ,AGENCY_CODE
           ,WORK_INIT_DATE1
           ,ENTER_AGENCY_DATE
           ,IS_AUTH
           ,PER_IDE_CODE1
           ,cast(RAND_INTEGER(100) as String) || '|'|| PER_IDE_CODE1 as NEW_PER_IDE_CODE1
           ,SERV_LEN
           ,IS_UNI_SALA
           ,START_DATE
           ,END_DATE
           ,STATE
           ,REDUCE_REASON
           ,UPDATE_TIME
           ,IS_DELETED
           ,CREATE_TIME
           ,FISCAL_YEAR
           ,POS_CODE1
           ,GR_CODE1
           ,TEC_WORKER_GR_CODE
           ,AGENCY_ID
      from `default`.DWD_X_BAS_PERSON_INFO_HIS
      WHERE SUBSTR(MOF_DIV_CODE, 1, 4) NOT IN ('4603')
        AND MOF_DIV_CODE NOT IN ('460110000', '469091000') ) t1
         left JOIN (
    select FELEMENTCODE,
           FNAME,
           MOF_DIV_CODE,
           MOF_DIV_CODE NEW_MOF_DIV_CODE,
           FCODE
    from DWD_MV
    where FELEMENTCODE = 'PERSON_IDENTITY' and MOF_DIV_CODE <> '510100000'
    union all
    --数据倾斜 MOF_DIV_CODE = '510100000' 复制出四份,+随机前缀 0~7
    select FELEMENTCODE,
           FNAME,
           MOF_DIV_CODE,
           cast(num as string) || '|'|| MOF_DIV_CODE  as NEW_MOF_DIV_CODE,
           FCODE
    from (
             select FELEMENTCODE,
                    FNAME,
                    MOF_DIV_CODE ,
                    array[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99] nums,
                                    FCODE
             from DWD_MV
             where FELEMENTCODE = 'PERSON_IDENTITY' and MOF_DIV_CODE = '510100000'
         ) ext ,UNNEST(nums) as T ( num )
) E06
                   ON  t1.NEW_MOF_DIV_CODE = E06.NEW_MOF_DIV_CODE
                       AND t1.NEW_PER_IDE_CODE1 = E06.FCODE
;



create TEMPORARY view EEE as
select *
from `default`.ODS_T_OTHERITEM
where FELEMENTCODE in (
                       'E_PERSON_NATION',
                       'E_PERSON_EDUCATION',
                       'E_PERSON_SOURCE',
                       'E_PERSON_PERSTATUS',
                       'E_PERSON_POST',
                       'E_PERSON_RANK',
                       'E_PERSON_TECHLEVEL',
                       'E_PERSON_NATION',
                       'E_PERSON_CARDTYPE');

INSERT overwrite
    `my_paimon_catalog`.`default`.DWS_BAS_PERSON_INFO_V23
SELECT T.X_PER_ID || coalesce(SUBSTR(DS.VERSION, 1, 2),'')                                  PER_ID,
       T.BIZ_KEY                                                                           BIZ_KEY,
       LPAD(T.IDEN_TYPE_CODE, 2, '0')                                                      IDEN_TYPE_CODE,
       E11.FNAME                                                                           IDEN_TYPE_NAME,
       LPAD(T.IDEN_NO, 18, '0')                                                            IDEN_NO,
       -- T.IDEN_NO IDEN_NO,
       COALESCE(T.MOF_DIV_CODE, '当前没有MOF_DIV_CODE')                                    MOF_DIV_CODE,
       COALESCE(E00.FNAME, '当前没有MOF_DIV_NAME')                                         MOF_DIV_NAME,
       T.PER_NAME                                                                          PER_NAME,
       T.SEX_CODE1                                                                         SEX_CODE,
       '中国'                                                                              NATION,
       if(T.SEX_CODE1 = '1', '男', '女')                                                   SEX_NAME,
       E10.FNAME                                                                           NAT_NAME,
       T.NAT_CODE                                                                          NAT_CODE,
       COALESCE(T.SCH_REC_CODE1, '9')                                                      SCH_REC_CODE,
       COALESCE(E02.FNAME, '其他')                                                         SCH_REC_NAME,
       T.PER_SOU_CODE1                                                                     PER_SOU_CODE,
       E03.FNAME                                                                           PER_SOU_NAME,
       COALESCE(T.PER_STA_CODE1, '9')                                                      PER_STA_CODE,
       COALESCE(E04.FNAME, '其他')                                                         PER_STA_NAME,
       COALESCE(E05.FCODE, T.AGENCY_CODE)                                                  AGENCY_CODE,
       COALESCE(TRIM(E05.FNAME), '暂未设定单位')                                           AGENCY_NAME,
       T.WORK_INIT_DATE1                                                                   WORK_INIT_DATE,
       if(T.WORK_INIT_DATE1 > T.ENTER_AGENCY_DATE, T.WORK_INIT_DATE1, T.ENTER_AGENCY_DATE) ENTER_AGENCY_DATE,
       T.IS_AUTH                                                                           IS_AUTH,
       T.PER_IDE_CODE                                                                       PER_IDE_CODE,
       T.PER_IDE_NAME                                                                      PER_IDE_NAME,
       T.SERV_LEN                                                                          SERV_LEN,
       T.POS_CODE1                                                                          POS_CODE,
       E07.FNAME                                                                           POS_NAME,
       T.GR_CODE1                                                                          GR_CODE,
       E08.FNAME                                                                           GR_NAME,
       T.TEC_WORKER_GR_CODE                                                                TEC_WORKER_GR_CODE,
       E09.FNAME                                                                           TEC_WORKER_GR_NAME,
       T.IS_UNI_SALA                                                                       IS_UNI_SALA,
       T.START_DATE                                                                        START_DATE,
       T.END_DATE                                                                          END_DATE,
       T.STATE                                                                             STATE,
       T.REDUCE_REASON                                                                     REDUCE_REASON,
       T.UPDATE_TIME                                                                       UPDATE_TIME,
       if(E05.FCODE IS NULL, '1', T.IS_DELETED)                        as                  IS_DELETED,
       cast(NOW() as string)                                           as                  SEND_DATE,
       if(T.CREATE_TIME > T.UPDATE_TIME, T.UPDATE_TIME, T.CREATE_TIME) as                  CREATE_TIME,
       T.FISCAL_YEAR                                                   as                  FISCAL_YEAR,
       cast(null as string )                                           as                  IS_ENABLED,
       cast(null as string )                                           as                  SERV_TEC_TYPE_CODE,
       cast(null as string )                                           as                  SERV_PER_ATR_CODE,
       cast(null as string )                                           as                  PENSION_PAY_MODE_CODE,
       cast(null as string )                                           as                  MANAGE_MOF_DEP_CODE,
       cast(null as string )                                           as                  BIRTH_MD,
       cast(null as string )                                           as                  IS_FIN_SUPPLY,
       DS.FISCAL_YEAR || DS.VERSION                                    as                  VERSION,
       DS.VERSION_NAME                                                 as                  VERSION_NAME,
       'CHN'                                                           as                  COUNTRY_CODE,
       cast(null as string )                                           as                  EMPLOYEE_CODE,
       '0'                                                             as                  IS_LAST_INST
FROM  T
          left JOIN EEE E02
                    ON E02.FELEMENTCODE = 'E_PERSON_EDUCATION' AND T.NEW_SCH_REC_CODE1 = E02.FCODE
          left JOIN EEE E03
                    ON E03.FELEMENTCODE = 'E_PERSON_SOURCE' AND T.NEW_PER_SOU_CODE1 = E03.FCODE
          left JOIN EEE E04
                    ON E04.FELEMENTCODE = 'E_PERSON_PERSTATUS' AND T.NEW_PER_STA_CODE1 = E04.FCODE
          left JOIN EEE E07
                    ON E07.FELEMENTCODE = 'E_PERSON_POST' AND T.NEW_POS_CODE1 = E07.FCODE
          left JOIN EEE E08
                    ON E08.FELEMENTCODE = 'E_PERSON_RANK' AND T.NEW_GR_CODE1 = E08.FCODE
          left JOIN EEE E09
                    ON E09.FELEMENTCODE = 'E_PERSON_TECHLEVEL' AND T.NEW_TEC_WORKER_GR_CODE = E09.FCODE
          left JOIN EEE E10
                    ON E10.FELEMENTCODE = 'E_PERSON_NATION' AND T.NEW_NAT_CODE = E10.FCODE
          left JOIN EEE E11
                    ON E11.FELEMENTCODE = 'E_PERSON_CARDTYPE' AND T.NEW_IDEN_TYPE_CODE = E11.FCODE
          left JOIN DWD_MV E05
                    ON E05.FELEMENTCODE IN ('AGENCY_NO', 'MOF_DIV_CODE')
                        AND T.NEW_MOF_DIV_CODE = E05.MOF_DIV_CODE
                        AND T.AGENCY_ID = E05.ELE_ID
          left JOIN `default`.DWD_T_DATA_SQL DS
                    ON T.NEW_FISCAL_YEAR = DS.FISCAL_YEAR
          left JOIN (select * from DWD_MV
                     where FELEMENTCODE = 'MOF_DIV_CODE'
                     and MOF_DIV_CODE =  FCODE)  E00
                    ON T.NEW_MOF_DIV_CODE = E00.FCODE
;

