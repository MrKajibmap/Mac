/*****************************************************************
*  ВЕРСИЯ:
*     $Id: f46239b14a4640df2a5f0d30b4f1931b074c6f56 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет таблицу BK-RK (BK-CD) новыми парами соответствий.
*
*  ПАРАМЕТРЫ:
*     mpIn                 +  имя входного набора, соответствия новый BK - существующий SK
*     mpInFieldBK          +  поле бизнес-ключа во входном наборе
*     mpInFieldSK          +  поле суррогатного ключа во входном наборе
*     mpLookup             +  имя таблицы соответствий BK-RK (BK-CD)
*     mpLkupFieldBK        -  поле бизнес-ключа в таблице соответствий
*                             по умолчанию равно mpInFieldBK
*     mpLkupFieldSK        -  поле суррогатного ключа в таблице соответствий
*                             по умолчанию равно mpInFieldSK
*     mpConflict1Level     -  поле типа ошибки в случае конфликта 1 типа
*                             возможные значения: E, W, I, D, отсутствует
*                             при W+ и конфликте постоянная таблица соответствий не будет обновлена
*                             по умолчанию отсутствует, т.е. игнор
*     mpConflict2Level     -  поле типа ошибки в случае конфликта 2 типа
*     mpStatusKey          +  имя макропеременной, в которую возвращается статус
*
******************************************************************
*  Использует:
*     %error_check
*
*  Устанавливает макропеременные:
*     mpStatusKey =  0           успех, таблица соответствия обновлена
*     mpStatusKey = -1           ошибка, конфликт 1 типа
*     mpStatusKey = -2           ошибка, конфликт 2 типа
*     mpStatusKey = -99          ошибка обновления таблицы соответствия
*     mpStatusKey =  1           успех, конфликт 1 типа
*     mpStatusKey =  2           успех, конфликт 2 типа

*
******************************************************************
*  Пример использования:
*     В макро etl_generate_sk_2.sas
*
******************************************************************
*  02-06-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_generate_sk_merge_add (
   mpIn                 =  ,
   mpInFieldBK          =  ,
   mpInFieldSK          =  ,
   mpLookup             =  ,
   mpLkupFieldBK        =  &mpInFieldBK,
   mpLkupFieldSK        =  &mpOutFieldSK,
   mpConflict1Level     =  ,
   mpConflict2Level     =  ,
   mpStatusKey          =
);
   /* Если входная таблица пуста, то выход */
   %local lmvInObs;
   %let lmvInObs = %member_obs(mpData=&mpIn);
   %if &lmvInObs le 0 %then %do;
      %let &mpStatusKey = 0;
      %return;
   %end;

   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Нетранзакционно: */
   /* создаем временную таблицу и заливаем в нее данные */
   %local lmvLookupLibref lmvLookupDbmsTable lmvLookupDbmsLoginSet;
   %member_names (mpTable=&mpLookup, mpLibrefNameKey=lmvLookupLibref);
   %&ETL_DBMS._table_name (mpSASTable=&mpLookup, mpOutFullNameKey=lmvLookupDbmsTable, mpOutLoginSetKey=lmvLookupDbmsLoginSet);

   %local lmvTmpName lmvTmpSasTable;
   %let lmvTmpName            =  etl_sk_add_&lmvUID.;
   %let lmvTmpSasTable        =  &lmvLookupLibref..&lmvTmpName;
   %let lmvTmpFieldConflict   =  etl_sk_conflict_&lmvUID.;

   /* Транзакционно: */
   %etl_transaction_start (mpLoginSet=&lmvLookupDbmsLoginSet);
      /* Переносим временную таблицу */
      create table &lmvTmpSasTable as select
         &mpInFieldBK,
         &mpInFieldSK,
         "0" as &lmvTmpFieldConflict
      from &mpIn
      ;
      %error_check (mpStepType=SQL);

      %let &mpStatusKey = 0;

      /* Проверка конфликтов 1 типа:  BK уже существует */
      /* В 2 запроса, поскольку (Note 15896) pass-through не возвращает кол-во обновленных записей */

      /* 1) обновляем временную таблицу соответствия уже существующими ключами */
      execute (
         update &lmvTmpName target set
            &lmvTmpFieldConflict = '1'
         where exists (
            select 1
            from &lmvLookupDbmsTable
            where target.&mpInFieldBK = &mpLkupFieldBK
         )
      ) by &ETL_DBMS;

      /* 2) получаем кол-во обновленных записей */
      %local lmvConflict1Obs;
      select conflict1_obs into :lmvConflict1Obs
      from connection to &ETL_DBMS (
         select count(*) as conflict1_obs
         from
            &lmvTmpName bk
         where
            &lmvTmpFieldConflict = '1'
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      %if &lmvConflict1Obs gt 0 %then %do;
         %if not %is_blank(mpConflict1Level) %then %do;
            %job_event_reg (mpEventTypeCode  =  DATA_CONFLICT,
                            mpLevel          =  &mpConflict1Level,
                            mpEventDesc      =  %bquote(Конфликт 1 типа в таблицах &mpIn, &mpLookup),
                            mpEventValues    =  %bquote(&lmvConflict1Obs общих ключей) );
            %let &mpStatusKey = 1;
         %end;

         %if %etl_level_ge (mpLevel1=&mpConflict1Level, mpLevel2=W) %then %do;
            %let &mpStatusKey = -1;
            %goto txn_finish;
         %end;
      %end;

      /* Проверка конфликтов 2 типа:  SK не существует */
      /* тоже в 2 запроса */

      /* 1) обновляем временную таблицу соответствия несуществующими ключами */
      execute (
         update &lmvTmpName target set
            &lmvTmpFieldConflict = '2'
         where not exists (
            select 1
            from &lmvLookupDbmsTable
            where target.&mpInFieldSK = &mpLkupFieldSK
         )
      ) by &ETL_DBMS;

      /* 2) получаем кол-во обновленных записей */
      %local lmvConflict2Obs;
      select conflict2_obs into :lmvConflict2Obs
      from connection to &ETL_DBMS (
         select count(*) as conflict2_obs
         from
            &lmvTmpName bk
         where
            &lmvTmpFieldConflict = '2'
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      %if &lmvConflict2Obs gt 0 %then %do;
         %if not %is_blank(mpConflict2Level) %then %do;
            %job_event_reg (mpEventTypeCode  =  DATA_CONFLICT,
                            mpLevel          =  &mpConflict2Level,
                            mpEventDesc      =  %bquote(Конфликт 2 типа в таблицах &mpIn, &mpLookup),
                            mpEventValues    =  %bquote(&lmvConflict2Obs общих ключей) );
            %let &mpStatusKey = 2;
         %end;

         %if %etl_level_ge (mpLevel1=&mpConflict2Level, mpLevel2=W) %then %do;
            %let &mpStatusKey = -2;
            %goto txn_finish;
         %end;
      %end;

      /* Обновляем постоянную таблицу соответствия новыми ключами */
      execute (
         insert into &lmvLookupDbmsTable (&mpLkupFieldBK, &mpLkupFieldSK)
         select &mpInFieldBK, &mpInFieldSK
         from &lmvTmpName
         where &lmvTmpFieldConflict = '0'
      ) by &ETL_DBMS;
      %error_check (mpStepType=SQL_PASS_THROUGH);

%txn_finish:
   %etl_transaction_finish;

   %if (&ETL_MODULE_RC ne 0) %then
      %let &mpStatusKey = -99;;

   /* Очистка */
   %member_drop(&lmvTmpSasTable);
%mend etl_generate_sk_merge_add;
