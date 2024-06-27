SELECT T.X_PER_ID || SUBSTR(CAST(DS.VERSION AS STRING), 1, 2)                   as PER_ID,
       T.BIZ_KEY                                                                as BIZ_KEY,
       LPAD(T.IDEN_TYPE_CODE, 2, '0')                                           as IDEN_TYPE_CODE,
       -- E11.FCODE IDEN_TYPE_CODE,
       --- 写一个自定义UDF
           E11.FNAME                                                            as IDEN_TYPE_NAME,
       LPAD(T.IDEN_NO, 18, '0')                                                 as IDEN_NO,
       -- T.IDEN_NO IDEN_NO,
       COALESCE(E00.FCODE, E0001.FCODE, T.MOF_DIV_CODE, '当前没有MOF_DIV_CODE') as MOF_DIV_CODE,
       COALESCE(E00.FNAME, E0001.FNAME, '当前没有MOF_DIV_NAME')                 as MOF_DIV_NAME,
       T.PER_NAME                                                               as PER_NAME,
       T.SEX_CODE1                                                              as SEX_CODE,
       '中国'                                                                   as NATION,
       if(T.SEX_CODE1 = '1', '男', '女')                                        as SEX_NAME,
       E10.FNAME                                                                as NAT_NAME,
       E10.FCODE                                                                as NAT_CODE,
       COALESCE(E02.FCODE, '9')                                                 as SCH_REC_CODE,
       COALESCE(E02.FNAME, '其他')                                              as SCH_REC_NAME,
       E03.FCODE                                                                as PER_SOU_CODE,
       E03.FNAME                                                                as PER_SOU_NAME,
       COALESCE(E04.FCODE, '9')                                                 as PER_STA_CODE,
       COALESCE(E04.FNAME, '其他')                                              as PER_STA_NAME,
       COALESCE(E05.FCODE, E0501.FCODE, T.AGENCY_CODE)                          as AGENCY_CODE,
       COALESCE(TRIM(E05.FNAME), TRIM(E0501.FNAME), '暂未设定单位')             as AGENCY_NAME,
       CASE
           WHEN INSTR(TRIM(PER_STA_CODE1), '2') is not NULL THEN T.ENTER_AGENCY_DATE
           ELSE T.WORK_INIT_DATE1
           END                                                                     WORK_INIT_DATE,
       CASE
           WHEN T.WORK_INIT_DATE1 > T.ENTER_AGENCY_DATE THEN T.WORK_INIT_DATE1
           ELSE T.ENTER_AGENCY_DATE
           END                                                                     ENTER_AGENCY_DATE,
       T.IS_AUTH                                                                as IS_AUTH,
       CASE
           WHEN LEFT(COALESCE(E06.FCODE, E0601.FCODE), 1) = '8'
               OR COALESCE(E06.FCODE, E0601.FCODE) = 'A' THEN '9'
           ELSE COALESCE(E06.FCODE, E0601.FCODE, '9')
           END                                                                  as PER_IDE_CODE,
       CASE
           WHEN LEFT(COALESCE(E06.FCODE, E0601.FCODE), 1) = '8'
               OR COALESCE(E06.FCODE, E0601.FCODE) = 'A' THEN '其他人员'
           ELSE COALESCE(E06.FNAME, E0601.FCODE, '其他人员')
           END                                                                  as PER_IDE_NAME,
       T.SERV_LEN                                                               as SERV_LEN,
       E07.FCODE                                                                as POS_CODE,
       E07.FNAME                                                                as POS_NAME,
       E08.FCODE                                                                as GR_CODE,
       E08.FNAME                                                                as GR_NAME,
       T.TEC_WORKER_GR_CODE                                                     as TEC_WORKER_GR_CODE,
       E09.FNAME                                                                as TEC_WORKER_GR_NAME,
       T.IS_UNI_SALA                                                            as IS_UNI_SALA,
       T.START_DATE                                                             as START_DATE,
       T.END_DATE                                                               as END_DATE,
       T.STATE                                                                  as STATE,
       T.REDUCE_REASON                                                          as REDUCE_REASON,
       T.NEW_UPDATE_TIME                                                        as UPDATE_TIME,
       CASE
           WHEN E05.FCODE IS NULL and E0501.FCODE IS NULL THEN '1'
           ELSE T.X_IS_DELETED
           END                                                                     IS_DELETED,
       cast(NOW() as string)                                                       SEND_DATE,
       CASE
           WHEN T.CREATE_TIME > T.UPDATE_TIME THEN T.UPDATE_TIME
           ELSE T.CREATE_TIME
           END                                                                     CREATE_TIME,
       T.X_FISCAL_YEAR                                                             FISCAL_YEAR,
       'CHN'                                                                       COUNTRY_CODE,
       '0'                                                                         IS_LAST_INST,
       DS.FISCAL_YEAR || DS.VERSION                                                VERSION,
    /*按不同版本区划,是整合系统的财政供养  、一体化的人员信息*/
       DS.VERSION_NAME
FROM `default`.DWD_X_BAS_PERSON_INFO T
         left JOIN `default`.DWD_T_DATA_SQL DS ON T.X_FISCAL_YEAR = DS.FISCAL_YEAR
         left JOIN `default`.DWD_MV_ELEUNION E00 ON E00.FELEMENTCODE = 'MOF_DIV_CODE'
    AND E00.MOF_DIV_CODE = T.MOF_DIV_CODE
    -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
    AND T.MOF_DIV_CODE = E00.FCODE
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.DWD_MV_ELEUNION E0001 ON E0001.FELEMENTCODE = 'MOF_DIV_CODE'
    AND E0001.MOF_DIV_CODE = 'DWD_PAIMON_FADMDIVCODE'
    -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
    AND T.MOF_DIV_CODE = E0001.FCODE
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.ODS_T_OTHERITEM E01 ON E01.FELEMENTCODE = 'E_PERSON_NATION'
    AND T.NAT_CODE = E01.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E02 ON E02.FELEMENTCODE = 'E_PERSON_EDUCATION'
    AND T.SCH_REC_CODE1 = E02.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E03 ON E03.FELEMENTCODE = 'E_PERSON_SOURCE'
    AND T.PER_SOU_CODE1 = E03.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E04 ON E04.FELEMENTCODE = 'E_PERSON_PERSTATUS'
    AND T.PER_STA_CODE1 = E04.FCODE
         left JOIN `default`.DWD_MV_ELEUNION E05
                   ON E05.FELEMENTCODE IN ('AGENCY_NO', 'MOF_DIV_CODE')
                       -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
                       AND E05.MOF_DIV_CODE = T.MOF_DIV_CODE
                       AND T.AGENCY_ID = E05.ELE_ID
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.DWD_MV_ELEUNION E0501
                   ON E0501.FELEMENTCODE IN ('AGENCY_NO', 'MOF_DIV_CODE')
                       -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
                       AND E0501.MOF_DIV_CODE = 'DWD_PAIMON_FADMDIVCODE'
                       AND T.AGENCY_ID = E0501.ELE_ID
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.DWD_MV_ELEUNION E06 ON E06.FELEMENTCODE = 'PERSON_IDENTITY'
    -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
    AND E06.MOF_DIV_CODE = T.MOF_DIV_CODE
    AND T.PER_IDE_CODE1 = E06.FCODE
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.DWD_MV_ELEUNION E0601 ON E0601.FELEMENTCODE = 'PERSON_IDENTITY'
    -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
    AND E0601.MOF_DIV_CODE = 'DWD_PAIMON_FADMDIVCODE'
    AND T.PER_IDE_CODE1 = E0601.FCODE
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.ODS_T_OTHERITEM E07 ON E07.FELEMENTCODE = 'E_PERSON_POST'
    AND T.POS_CODE1 = E07.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E08 ON E08.FELEMENTCODE = 'E_PERSON_RANK'
    AND T.GR_CODE1 = E08.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E09 ON E09.FELEMENTCODE = 'E_PERSON_TECHLEVEL'
    AND T.TEC_WORKER_GR_CODE = E09.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E10 ON E10.FELEMENTCODE = 'E_PERSON_NATION'
    AND T.NAT_CODE = E10.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E11 ON E11.FELEMENTCODE = 'E_PERSON_CARDTYPE'
    AND T.IDEN_TYPE_CODE = E11.FCODE
WHERE SUBSTR(CAST(T.MOF_DIV_CODE AS STRING), 1, 4) NOT IN ('4603')
  AND CAST(T.MOF_DIV_CODE AS STRING) NOT IN ('460110000', '469091000')

UNION ALL
SELECT T.X_PER_ID || SUBSTR(DS.VERSION, 1, 2)                                   PER_ID,
       - -X_PER_IDD                                                             前段组装已拼接年度后两位数,
       切记                                                                     与BAS_PERSON_EXT_V1 同时修改主键一致，技术检查表间关系
       T.BIZ_KEY                                                                BIZ_KEY, --- 写一个自定义UDF
    LPAD(E11.FCODE, 2, '0') IDEN_TYPE_CODE,
       -- E11.FCODE IDEN_TYPE_CODE,
       E11.FNAME                                                                IDEN_TYPE_NAME,
       LPAD(T.IDEN_NO, 18, '0')                                                 IDEN_NO,
       -- T.IDEN_NO IDEN_NO,
       COALESCE(E00.FCODE, E0001.FCODE, T.MOF_DIV_CODE, '当前没有MOF_DIV_CODE') MOF_DIV_CODE,
       COALESCE(E00.FNAME, E0001.FNAME, '当前没有MOF_DIV_NAME')                 MOF_DIV_NAME,
       T.PER_NAME                                                               PER_NAME,
       T.SEX_CODE1                                                              SEX_CODE,
       '中国'                                                                   NATION,
       CASE
           T.SEX_CODE1
           WHEN '1' THEN '男'
           ELSE '女'
           END AS                                                               SEX_NAME,
       E10.FNAME                                                                NAT_NAME,
       E10.FCODE                                                                NAT_CODE,
       COALESCE(E02.FCODE, '9')                                                 SCH_REC_CODE,
       COALESCE(E02.FNAME, '其他')                                              SCH_REC_NAME,
       E03.FCODE                                                                PER_SOU_CODE,
       E03.FNAME                                                                PER_SOU_NAME,
       COALESCE(E04.FCODE, '9')                                                 PER_STA_CODE,
       COALESCE(E04.FNAME, '其他')                                              PER_STA_NAME,
       COALESCE(E05.FCODE, E0501.FCODE, T.AGENCY_CODE)                          AGENCY_CODE,
       COALESCE(TRIM(E05.FNAME), TRIM(E0501.FNAME), '暂未设定单位')             AGENCY_NAME,
       WORK_INIT_DATE1                                                          WORK_INIT_DATE,
    /*参加工作时间*/
       CASE
           WHEN WORK_INIT_DATE1 > T.ENTER_AGENCY_DATE THEN WORK_INIT_DATE1
           ELSE T.ENTER_AGENCY_DATE
           END                                                                  ENTER_AGENCY_DATE,
    /*进入本单位时间*/
       T.IS_AUTH                                                                IS_AUTH,
       COALESCE(E06.FCODE, E0601.FCODE, '9')                                    PER_IDE_CODE,
       COALESCE(E06.FNAME, E0601.FNAME, '其他人员')                             PER_IDE_NAME,
       T.SERV_LEN                                                               SERV_LEN,
       E07.FCODE                                                                POS_CODE,
       E07.FNAME                                                                POS_NAME,
       E08.FCODE                                                                GR_CODE,
       E08.FNAME                                                                GR_NAME,
       E09.FCODE                                                                TEC_WORKER_GR_CODE,
       E09.FNAME                                                                TEC_WORKER_GR_NAME,
       T.IS_UNI_SALA                                                            IS_UNI_SALA,
       T.START_DATE                                                             START_DATE,
       T.END_DATE                                                               END_DATE,
       T.STATE                                                                  STATE,
       T.REDUCE_REASON                                                          REDUCE_REASON,
       T.UPDATE_TIME                                                            UPDATE_TIME,
       CASE
           WHEN E05.FCODE IS NULL and E0501.FCODE IS NULL THEN '1'
           ELSE T.IS_DELETED
           END                                                                  IS_DELETED,
       -- SYSDATE + (1/(24*60*60)*(TRUNC(ROWNUM/5000)+1)) SEND_DATE,
       cast(NOW() as string)                                                    SEND_DATE,
       CASE
           WHEN T.CREATE_TIME > T.UPDATE_TIME THEN T.UPDATE_TIME
           ELSE T.CREATE_TIME
           END                                                                  CREATE_TIME,
       T.FISCAL_YEAR,
       'CHN'                                                                    COUNTRY_CODE,
       - -V23新增
                                                                                '0' IS_LAST_INST, DS.FISCAL_YEAR || DS.VERSION VERSION,
       DS.VERSION_NAME
FROM (select *
      from `default`.DWD_X_BAS_PERSON_INFO_HIS
      WHERE SUBSTR(MOF_DIV_CODE, 1, 4) NOT IN ('4603')
        AND MOF_DIV_CODE NOT IN ('460110000', '469091000')) T
         left JOIN `default`.DWD_T_DATA_SQL DS ON T.FISCAL_YEAR = DS.FISCAL_YEAR
         left JOIN `default`.DWD_MV_ELEUNION E00 ON E00.FELEMENTCODE = 'MOF_DIV_CODE'
    AND E00.MOF_DIV_CODE = T.MOF_DIV_CODE
    -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
    AND T.MOF_DIV_CODE = E00.FCODE
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.DWD_MV_ELEUNION E0001 ON E0001.FELEMENTCODE = 'MOF_DIV_CODE'
    AND E0001.MOF_DIV_CODE = 'DWD_PAIMON_FADMDIVCODE'
    -- AND T.MOF_DIV_CODE IN (SELECT FADMDIVCODE FROM `default`.DWD_PAIMON_FADMDIVCODE)
    AND T.MOF_DIV_CODE = E0001.FCODE
    -- AND T.X_FISCAL_YEAR IN (SELECT fiscal_year FROM  `default`.DWD_PAIMON_FISCAL_YEAR)
         left JOIN `default`.ODS_T_OTHERITEM E01 ON E01.FELEMENTCODE = 'E_PERSON_NATION'
    AND T.NAT_CODE = E01.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E02 ON E02.FELEMENTCODE = 'E_PERSON_EDUCATION'
    AND T.SCH_REC_CODE1 = E02.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E03 ON E03.FELEMENTCODE = 'E_PERSON_SOURCE'
    AND T.PER_SOU_CODE1 = E03.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E04 ON E04.FELEMENTCODE = 'E_PERSON_PERSTATUS'
    AND T.PER_STA_CODE1 = E04.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E07 ON E07.FELEMENTCODE = 'E_PERSON_POST'
    AND T.POS_CODE1 = E07.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E08 ON E08.FELEMENTCODE = 'E_PERSON_RANK'
    AND T.GR_CODE1 = E08.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E09 ON E09.FELEMENTCODE = 'E_PERSON_TECHLEVEL'
    AND T.TEC_WORKER_GR_CODE = E09.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E10 ON E10.FELEMENTCODE = 'E_PERSON_NATION'
    AND T.NAT_CODE = E10.FCODE
         left JOIN `default`.ODS_T_OTHERITEM E11 ON E11.FELEMENTCODE = 'E_PERSON_CARDTYPE'
    AND T.IDEN_TYPE_CODE = E11.FCODE
         left JOIN `default`.DWD_MV_ELEUNION E05
                   ON E05.FELEMENTCODE IN ('AGENCY_NO', 'MOF_DIV_CODE')
                       AND E05.MOF_DIV_CODE = T.MOF_DIV_CODE
                       AND T.AGENCY_ID = E05.ELE_ID
         left JOIN `default`.DWD_MV_ELEUNION E0501
                   ON E0501.FELEMENTCODE IN ('AGENCY_NO', 'MOF_DIV_CODE')
                       AND E0501.MOF_DIV_CODE = 'DWD_PAIMON_FADMDIVCODE'
                       AND T.AGENCY_ID = E0501.ELE_ID
         left JOIN `default`.DWD_MV_ELEUNION E06 ON E06.FELEMENTCODE = 'PERSON_IDENTITY'
    AND E06.MOF_DIV_CODE = T.MOF_DIV_CODE
    AND T.PER_IDE_CODE1 = E06.FCODE
         left JOIN `default`.DWD_MV_ELEUNION E0601 ON E0601.FELEMENTCODE = 'PERSON_IDENTITY'
    AND E0601.MOF_DIV_CODE = 'DWD_PAIMON_FADMDIVCODE'
    AND T.PER_IDE_CODE1 = E0601.FCODE;