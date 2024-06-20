INSERT INTO my_paimon_catalog.`default`.DWD_X_BAS_PERSON_INFO_HIS
SELECT T.*,
       T.AGENCY_ID                                            as AGENCY_ID1,
       -- '' as WORK_INIT_DATE1,
       CASE
           WHEN T.IS_DELETED = '2' AND T.SERV_LEN IS NOT NULL AND T.SERV_LEN <> '1'
               /*工龄*/
               AND DATEADDSUBTRACT(cast(T.WORK_INIT_DATE as DATE), COALESCE(CAST(T.SERV_LEN AS INT) - 1, 0), 0, 0) >=
                   CURRENT_DATE
               THEN cast(DATEADDSUBTRACT(CURRENT_DATE, COALESCE(-CAST(T.SERV_LEN AS INT) - 1, 0), 0, 0) as string)
           ELSE T.WORK_INIT_DATE
           END                                                as WORK_INIT_DATE1,
    /*参加工作时间*/
       CASE
           WHEN COALESCE(TRIM(REPLACE(T.SCH_REC_CODE, '　', ' ')), '20') in ('0', '01', '1') THEN '20'
           ELSE COALESCE(TRIM(REPLACE(T.SCH_REC_CODE, '　', ' ')), '20')
           END                                                as SCH_REC_CODE1,
       COALESCE(TRIM(REPLACE(T.PER_SOU_CODE, '　', ' ')), '9') as PER_SOU_CODE1,
       TRIM(REPLACE(T.PER_STA_CODE, '　', ' '))                as PER_STA_CODE1,
       COALESCE(TRIM(REPLACE(T.PER_IDE_CODE, '　', ' ')), '9') as PER_IDE_CODE1,
       COALESCE(TRIM(REPLACE(T.POS_CODE, '　', ' ')), '13')    as POS_CODE1,
       COALESCE(TRIM(REPLACE(T.GR_CODE, '　', ' ')), '12')     as GR_CODE1,
       -- 通过身份证最后一位判断性别
       CASE
           WHEN CHAR_LENGTH(T.IDEN_NO) NOT IN (15, 18) AND T.SEX_CODE IS NOT NULL THEN T.SEX_CODE
           WHEN T.SEX_CODE IS NULL AND MOD(cast(right (T.IDEN_NO, 1) AS INT), 2) = 0 THEN '2'
           WHEN T.SEX_CODE IS NULL AND MOD(cast(right (T.IDEN_NO, 1) AS INT), 2) <> 0 THEN '1'
           ELSE COALESCE(T.SEX_CODE, '1') END                 AS SEX_CODE1,
       T.PER_ID || SUBSTR(T.FISCAL_YEAR, 3, 2)                   X_PER_ID
FROM my_paimon_catalog.`default`.ODS_BAS_PERSON_INFO_HIS_KH T -- /*排除当年切换到的预算年度*/
         JOIN (select FPARAMID, FPARAMDATA
               from my_paimon_catalog.`default`.ODS_T_SYSSET
               where FPARAMID = 'G_BudgetYear') S
              on CAST(TRIM(S.FPARAMDATA) AS DECIMAL(4, 0)) > T.FISCAL_YEAR;