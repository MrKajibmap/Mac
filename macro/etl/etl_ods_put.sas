/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 5d277b357d6209ca9b2dda1720fb3859500746f3 $: etl_ods_put.sas  $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Размещает выгруженный набор в ODS, регистрирует выгрузку в реестре ODS.
*     Выгружает только выбранные поля
*
*  ПАРАМЕТРЫ:
*     mpInData         +  имя входного набора, выгруженной таблицы
*     mpInRegistry     +  имя входного набора, с записью из реестра о выгруженной таблице
*     mpOutData        +  имя выходного набора, ODS таблицы
*     mpOutRegistry    +  имя выходного набора, реестра ODS
*     mpOutVars        +  список полей выходного набора (все поля ODS таблицы, за исключением виртуальных)
*
******************************************************************
*  Использует:
*     %ETL_DBMS_table_name
*     %ETL_DBMS_connect
*     %etl_transaction_start
*     %etl_transaction_finish
*     %error_check
*     %list_expand
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     в трансформации transform_ods_put.sas
*
******************************************************************
*  17-11-2014  Кузенков       Начальное кодирование
******************************************************************/

%macro etl_ods_put (
   mpInData        = ,
   mpInRegistry    = ,
   mpOutData       = ,
   mpOutRegistry   = ,
   mpOutVars       =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем available_dttm размещаемой таблицы */
   %LOCAL lmvAvailableDt;
   PROC SQL NOPRINT;
     SELECT
       datepart(available_dttm) FORMAT=32. INTO :lmvAvailableDt TRIMMED
     FROM
       &mpInRegistry
     ;
   QUIT;

   %local lmvOutTableName lmvOutTableFullName;
   %&ETL_DBMS._table_name (mpSASTable=&mpOutData, mpOutFullNameKey=lmvOutTableFullName, mpOutNameKey=lmvOutTableName);


   %local lmvOutRegistryTable;
   %&ETL_DBMS._table_name (mpSASTable=&mpOutRegistry, mpOutFullNameKey=lmvOutRegistryTable);

   %local lmvTmpName;
   %let lmvTmpName = etl_upload_&lmvUID.;

   /* Нетранзакционно: */
   proc sql;
      %&ETL_DBMS._connect (mpLoginSet=DWH_ODS);

      /* обновляем статус в реестре ODS */
      execute (
         MERGE INTO &lmvOutRegistryTable M
            USING (SELECT %STR(%')&lmvOutTableName%STR(%') AS table_nm FROM DUAL) U
            ON (M.table_nm = U.table_nm)
         WHEN MATCHED THEN
            UPDATE SET status_cd = 'C', actual_dt = NULL, processed_ts = Current_timestamp
         WHEN NOT MATCHED THEN
            INSERT (table_nm, status_cd, processed_ts)
               VALUES (%STR(%')&lmvOutTableName%STR(%'), 'C', Current_timestamp)
      ) by &ETL_DBMS
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* очищаем целевую таблицу */
      execute (
         truncate table &lmvOutTableFullName
      ) by &ETL_DBMS
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* создаем временную таблицу и заливаем в нее данные */
      /* воссоздаем структуру целевой таблицы */
      execute (
         create table &lmvTmpName as
           select
             %list_expand(&mpOutVars, "{}", mpOutDlm=%STR(,))
           from &lmvOutTableFullName
           where 1=0
      ) by &ETL_DBMS
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

   quit;

   /* заливаем данные во временную таблицу */
   proc append
     base = DWH_ODS.&lmvTmpName (&ETL_BULKLOAD_OPTIONS)
     data = &mpInData(KEEP=%list_expand(&mpOutVars, {}))
   ;
   run;
   %error_check;
   %if &STEP_RC ne 0 %then
     %return;

   /* Транзакционно: */
   %etl_transaction_start (mpLoginSet=DWH_ODS);
      /* переносим записи в ODS ... */
      execute (
         insert into &lmvOutTableFullName (%list_expand(&mpOutVars, "{}", mpOutDlm=%STR(,)))
         select * from &lmvTmpName
      ) by &ETL_DBMS
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* ... и обновляем записи в реестре ODS */
      execute (
         MERGE INTO &lmvOutRegistryTable M
            USING (SELECT %STR(%')&lmvOutTableName%STR(%') AS table_nm FROM DUAL) U
            ON (M.table_nm = U.table_nm)
         WHEN MATCHED THEN
            UPDATE SET status_cd = 'R', actual_dt = %&ETL_DBMS._date(&lmvAvailableDt), processed_ts = Current_timestamp
         WHEN NOT MATCHED THEN
            INSERT (table_nm, status_cd, actual_dt, processed_ts)
               VALUES (%STR(%')&lmvOutTableName%STR(%'), 'R', %&ETL_DBMS._date(&lmvAvailableDt), Current_timestamp)
      ) by &ETL_DBMS
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

   %etl_transaction_finish;

   /* Очистка */
   %member_drop(DWH_ODS.&lmvTmpName);

%mend etl_ods_put;
