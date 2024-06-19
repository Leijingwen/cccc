--- 将主表写入PAIMON，并落库
-- SYYANG
-- 20231226
-----------任务1 同步 XXX
INSERT INTO
    `my_paimon_catalog`.`default`.DWD_X_BAS_PERSON_INFO
SELECT
    T.*,
    T.AGENCY_ID AGENCY_ID1,
    -- '' as WORK_INIT_DATE1,
    CASE
        WHEN T.IS_DELETED = '2'
            AND T.SERV_LEN IS NOT NULL
            AND T.SERV_LEN <> '1'
            /*工龄*/
            AND DATEADDSUBTRACT(cast(T.WORK_INIT_DATE as DATE),COALESCE(CAST(T.SERV_LEN AS INT) -1, 0),0,0) >= CURRENT_DATE
            THEN Cast(DATEADDSUBTRACT(CURRENT_DATE,COALESCE(-CAST(T.SERV_LEN AS INT) -1 , 0),0,0) as string)
        ELSE T.WORK_INIT_DATE
        END AS WORK_INIT_DATE1,
    /*参加工作时间*/
    CASE
        COALESCE(TRIM(REPLACE(T.SCH_REC_CODE, '　', ' ')), '20')
        WHEN '0' THEN '20'
        WHEN '01' THEN '20'
        WHEN '1' THEN '20'
        ELSE COALESCE(TRIM(REPLACE(T.SCH_REC_CODE, '　', ' ')), '20')
        END AS SCH_REC_CODE1,
    COALESCE(TRIM(REPLACE(T.PER_SOU_CODE, '　', ' ')), '9') PER_SOU_CODE1,
    TRIM(REPLACE(T.PER_STA_CODE, '　', ' ')) PER_STA_CODE1,
    COALESCE(TRIM(REPLACE(T.PER_IDE_CODE, '　', ' ')), '9') PER_IDE_CODE1,
    COALESCE(TRIM(REPLACE(T.POS_CODE, '　', ' ')), '13') POS_CODE1,
    COALESCE(TRIM(REPLACE(T.GR_CODE, '　', ' ')), '12') GR_CODE1,
    -- 通过身份证最后一位判断性别
    CASE
        WHEN CHAR_LENGTH(T.IDEN_NO) NOT IN (15, 18)
            AND T.SEX_CODE IS NOT NULL THEN T.SEX_CODE
        WHEN T.SEX_CODE IS NULL
            AND MOD(
                        CAST(
                                SUBSTR(
                                        CAST(T.IDEN_NO AS STRING),
                                        CHAR_LENGTH(T.IDEN_NO) -1,
                                        1
                                ) AS INT
                        ),
                        2
                ) = 0 THEN '2'
        WHEN T.SEX_CODE IS NULL
            AND MOD(
                        CAST(
                                SUBSTR(
                                        T.IDEN_NO,
                                        CHAR_LENGTH(T.IDEN_NO) -1,
                                        1
                                ) AS INT
                        ),
                        2
                ) <> 0 THEN '1'
        ELSE COALESCE(T.SEX_CODE, '1')
        END AS SEX_CODE1,
    T.PER_ID || SUBSTR(S.FPARAMDATA, 3, 2) X_PER_ID,
    S.FPARAMDATA X_FISCAL_YEAR,
    T.IS_DELETED X_IS_DELETED,
    T.UPDATE_TIME NEW_UPDATE_TIME
FROM
    `my_paimon_catalog`.`default`.ODS_BAS_PERSON_INFO_KH T
        JOIN `my_paimon_catalog`.`default`.ODS_T_SYSSET S ON S.FPARAMID = 'G_BudgetYear'
UNION ALL
--退休
SELECT
    T.*,
    BS1.FGUID AGENCY_ID1,
    -- '' as WORK_INIT_DATE1,
    CASE
        WHEN T.IS_DELETED = '2'
            AND T.SERV_LEN IS NOT NULL
            AND T.SERV_LEN <> '1'
            /*工龄*/
            AND DATEADDSUBTRACT(cast(T.WORK_INIT_DATE as DATE),COALESCE(CAST(T.SERV_LEN AS INT) -1, 0),0,0) >= CURRENT_DATE
            THEN Cast(DATEADDSUBTRACT(CURRENT_DATE,COALESCE(-CAST(T.SERV_LEN AS INT) -1 , 0),0,0) as string)
        ELSE T.WORK_INIT_DATE
        END AS WORK_INIT_DATE1,
    /*参加工作时间*/
    CASE
        COALESCE(TRIM(REPLACE(T.SCH_REC_CODE, '　', ' ')), '20')
        WHEN '0' THEN '20'
        WHEN '01' THEN '20'
        WHEN '1' THEN '20'
        ELSE COALESCE(TRIM(REPLACE(T.SCH_REC_CODE, '　', ' ')), '20')
        END AS SCH_REC_CODE1,
    COALESCE(TRIM(REPLACE(T.PER_SOU_CODE, '　', ' ')), '9') PER_SOU_CODE1,
    /*TRIM(REPLACE(T.PER_STA_CODE, '　', ' '))*/
    '2' PER_STA_CODE1,
    COALESCE(TRIM(REPLACE(T.PER_IDE_CODE, '　', ' ')), '9') PER_IDE_CODE1,
    COALESCE(TRIM(REPLACE(T.POS_CODE, '　', ' ')), '13') POS_CODE1,
    COALESCE(TRIM(REPLACE(T.GR_CODE, '　', ' ')), '12') GR_CODE1,
    -- 通过身份证最后一位判断性别
    CASE
        WHEN CHAR_LENGTH(T.IDEN_NO) NOT IN (15, 18)
            AND T.SEX_CODE IS NOT NULL THEN T.SEX_CODE
        WHEN T.SEX_CODE IS NULL
            AND MOD(
                        CAST(
                                SUBSTR(
                                        T.IDEN_NO,
                                        CHAR_LENGTH(T.IDEN_NO) -1,
                                        1
                                ) AS INT
                        ),
                        2
                ) = 0 THEN '2'
        WHEN T.SEX_CODE IS NULL
            AND MOD(
                        CAST(
                                SUBSTR(
                                        T.IDEN_NO,
                                        CHAR_LENGTH(T.IDEN_NO) -1,
                                        1
                                ) AS INT
                        ),
                        2
                ) <> 0 THEN '1'
        ELSE COALESCE(T.SEX_CODE, '1')
        END AS SEX_CODE1,
    -- X.PER_ID || SUBSTR(S.fiscal_year, 3, 2) X_PER_ID,
    X.PER_ID || 'DWD_PAIMON_FISCAL_YEAR_3_2' X_PER_ID,
    'DWD_PAIMON_FISCAL_YEAR' X_FISCAL_YEAR,
    X.IS_DELETED X_IS_DELETED,
    X.UPDATE_TIME NEW_UPDATE_TIME
FROM
    `my_paimon_catalog`.`default`.ODS_BAS_PERSON_INFO_KH T
        JOIN `my_paimon_catalog`.`default`.ODS_BAS_PERSON_INFO_TX X ON X.MOF_DIV_CODE = T.MOF_DIV_CODE -- 实现2020年到2023年的单个字段的展示
        JOIN `my_paimon_catalog`.`default`.DWD_V_BASEITEM_ALL BS1 ON BS1.FELEMENTCODE = 'AGENCY_NO'
        AND BS1.FADMDIVCODE = T.MOF_DIV_CODE
        AND BS1.FCODE = X.AGENCY_CODE
        AND BS1.START_YEAR <= BS1.END_YEAR
-- AND BS1.START_YEAR <= S.fiscal_year
-- AND BS1.END_YEAR > S.fiscal_year
WHERE
    T.IS_DELETED = '2';