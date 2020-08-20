/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 7b897eb60efa5deff924188e43025393b4dd20a4 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет таблицу BK-RK (BK-CD) новыми парами соответствий.
*     В выходной набор выводится актуальная таблица соответствий для ключей входного набора.
*
*  ПАРАМЕТРЫ:
*     mpIn                 +  имя входного набора, соответствия новый BK - неизвестный SK
*     mpInFieldBK          +  поле бизнес-ключа во входном наборе
*     mpInFieldSK          +  поле суррогатного ключа во входном наборе
*     mpOut                +  имя выходного набора
*     mpSKMethod           -  метод генерации суррогатных ключей (RK, ASCII, HEX)
*                             По умолчанию RK
*     mpLookup             +  имя таблицы соответствий BK-RK (BK-CD)
*     mpLkupFieldBK        -  поле бизнес-ключа в таблице соответствий
*                             по умолчанию равно mpInFieldBK
*     mpLkupFieldSK        -  поле суррогатного ключа в таблице соответствий
*                             по умолчанию равно mpInFieldSK
*     mpGridFieldBy        -  при исполнении на гриде - поле во входном наборе и таблице соответствий, используемое для разбиения
*     mpGridValues         -  при исполнении на гриде - значения mpGridFieldBy, позволяющие отобрать нужную часть
*     mpConflict1Level     -  поле типа ошибки в случае конфликта 1 типа
*                             возможные значения: E, W, I, D, отсутствует
*                             при W+ и конфликте постоянная таблица соответствий не будет обновлена
*                             по умолчанию отсутствует, т.е. игнор
*     mpStatusKey          +  имя макропеременной, в которую возвращается статус
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_level_ge
*     %etl_transaction_start/finish
*     %job_event_reg
*     %member_drop
*     %member_names
*     %member_var_attr
*     %util_list
*
*  Устанавливает макропеременные:
*     mpStatusKey =  0           успех, таблица соответствия обновлена
*     mpStatusKey = -1           ошибка, конфликт 1 типа
*     mpStatusKey = -99          ошибка обновления таблицы соответствия
*     mpStatusKey =  1           успех, конфликт 1 типа
*
******************************************************************
*  Пример использования:
*     В макро etl_generate_sk.sas, etl_generate_sk_2.sas
*
******************************************************************
*  28-05-2014  Нестерёнок     Начальное кодирование
*  24-07-2014  Нестерёнок     Поддержка исполнения на гриде (mpGridFieldBy, mpGridValues)
*  09-02-2015  Сазонов        Обновление временной таблицы переписано на merge для db2
******************************************************************/

%macro etl_generate_sk_merge_new (
   mpIn                 =  ,
   mpInFieldBK          =  ,
   mpInFieldSK          =  ,
   mpOut                =  ,
   mpSKMethod           =  RK,
   mpLookup             =  ,
   mpLkupFieldBK        =  &mpInFieldBK,
   mpLkupFieldSK        =  &mpOutFieldSK,
   mpGridFieldBy        =  ,
   mpGridValues         =  ,
   mpConflict1Level     =  ,
   mpStatusKey          =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем режимы работы */
   %local lmvGridMode;
   %let lmvGridMode     =  %eval( (not %is_blank(mpGridFieldBy)) and (not %is_blank(mpGridValues)) );

   /* Нетранзакционно: */
   /* создаем временную таблицу и заливаем в нее данные */
   %local lmvLookupLibref lmvLookupDbmsTable lmvLookupDbmsLoginSet;
   %member_names (mpTable=&mpLookup, mpLibrefNameKey=lmvLookupLibref);
   %&ETL_DBMS._table_name (mpSASTable=&mpLookup, mpOutFullNameKey=lmvLookupDbmsTable, mpOutLoginSetKey=lmvLookupDbmsLoginSet);

   %local lmvTmpName lmvTmpSasTable;
   %let lmvTmpName            =  etl_sk_merge_&lmvUID.;
   %let lmvTmpSasTable        =  &lmvLookupLibref..&lmvTmpName;
   %let lmvTmpFieldConflict   =  etl_sk_conflict_&lmvUID.;

   /* Транзакционно: */
   %etl_transaction_start (mpLoginSet=&lmvLookupDbmsLoginSet);
      reset noprint;

      /* Блокируем таблицу */
      %&ETL_DBMS._table_lock (mpTable=&lmvLookupDbmsTable, mpLockMode=EXCLUSIVE, mpWait=INF);

      /* Получаем макс. существующий суррогатный ключ */
      %local lmvMaxSK;
%if &mpSKMethod=RK %then %do;
      select
         &mpLkupFieldSK format=best20.
      into :lmvMaxSK
      from connection to &ETL_DBMS(
         select
            max(&mpLkupFieldSK) as &mpLkupFieldSK
         from
            &lmvLookupDbmsTable
      );
%end;
%else %do;
      %local lmvSKLength;
      %let lmvSKLength = %member_var_attr (mpData=&lmvLookupDbmsTable, mpVar=&mpLkupFieldSK, mpAttr=VARLEN);

      select
%if &mpSKMethod=ASCII %then %do;
         max(ascii_canonical(&mpLkupFieldSK))
%end;
%if &mpSKMethod=HEX %then %do;
         max(hex_canonical(&mpLkupFieldSK))
%end;
      length=&lmvSKLength
      into :lmvMaxSK
      from connection to &ETL_DBMS(
         select
            length(&mpLkupFieldSK),
            max(&mpLkupFieldSK) as &mpLkupFieldSK
         from
            &lmvLookupDbmsTable
         group by
            length(&mpLkupFieldSK)
      );
%end;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Создаем новые ключи */
      create table &lmvTmpSasTable (&ETL_BULKLOAD_OPTIONS) as select
         &mpInFieldBK,
%if &mpSKMethod=RK %then %do;
         case(not missing(&mpInFieldSK))
            when(1) then &mpInFieldSK
            else sum(&lmvMaxSK, monotonic())
         end
         as &mpInFieldSK,
%end;
%else %do;
%if &mpSKMethod=ASCII %then %do;
         ascii_next_n("&lmvMaxSK", monotonic())
%end;
%if &mpSKMethod=HEX %then %do;
         hex_next_n("&lmvMaxSK", monotonic())
%end;
         as &mpInFieldSK
         length=&lmvSKLength,
%end;
%if &lmvGridMode %then %do;
         &mpGridFieldBy,
%end;
         "0" as &lmvTmpFieldConflict
      from &mpIn
      ;
      %error_check (mpStepType=SQL);

      %let &mpStatusKey = 0;

      /* Проверка конфликтов 1 типа:  BK уже существует */
      /* В 2 запроса, поскольку (Note 15896) pass-through не возвращает кол-во обновленных записей */

      /* 1) обновляем временную таблицу соответствия уже существующими ключами */
%if &ETL_DBMS = db2 %then %do;
     execute (
     merge into
         &lmvTmpName target
      using
         &lmvLookupDbmsTable lkup
      on target.&mpInFieldBK = lkup.&mpLkupFieldBK
%if &lmvGridMode %then %do;
            and lkup.&mpGridFieldBy in (%util_list(&mpGridValues))
%end;
      when matched then update
         set
         &mpInFieldSK = &mpLkupFieldSK,
            &lmvTmpFieldConflict = '1'
     ) by &ETL_DBMS;
%end;
%else %if &ETL_DBMS = postgres %then %do;
     execute (
         update &lmvTmpName target
           set &mpInFieldSK = lkup.&mpLkupFieldSK, 
               &lmvTmpFieldConflict = 1
            from
               &lmvLookupDbmsTable lkup
           where target.&mpInFieldBK = lkup.&mpLkupFieldBK
%if &lmvGridMode %then %do;
            and lkup.&mpGridFieldBy in (%util_list(&mpGridValues))
%end;
     ) by &ETL_DBMS;
%end;
%else %do;
      execute (
         update (
            select
               &mpInFieldSK as tmp_sk,
               &mpLkupFieldSK as actual_sk,
               &lmvTmpFieldConflict
            from
               &lmvTmpName target
            inner join
               &lmvLookupDbmsTable lkup
            on target.&mpInFieldBK = lkup.&mpLkupFieldBK
%if &lmvGridMode %then %do;
            and lkup.&mpGridFieldBy in (%util_list(&mpGridValues))
%end;
         )
         set
            tmp_sk = actual_sk,
            &lmvTmpFieldConflict = '1'
      ) by &ETL_DBMS;
%end;

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

      /* Обновляем постоянную таблицу соответствия новыми ключами */
      execute (
         insert into &lmvLookupDbmsTable (
            %util_list(&mpLkupFieldBK &mpLkupFieldSK &mpGridFieldBy)
         )
         select
            %util_list(&mpInFieldBK   &mpInFieldSK   &mpGridFieldBy)
         from &lmvTmpName
         where &lmvTmpFieldConflict = '0'
      ) by &ETL_DBMS;
      %error_check (mpStepType=SQL_PASS_THROUGH);

%txn_finish:
   %etl_transaction_finish;

   /* Создаем выход */
   %if (&ETL_MODULE_RC ne 0) %then
      %let &mpStatusKey = -99;;

   %if (&mpStatusKey ge 0) %then %do;
      %local lmvTmpSkTable;
      %let lmvTmpSkTable   = etl_sk_final_&lmvUID.;
      proc sort
         data=&lmvTmpSasTable (keep= &mpInFieldBK &mpInFieldSK)
         out=&lmvTmpSkTable (index= (&mpInFieldBK /unique))
         ;
         by &mpInFieldBK;
      run;
      %error_check;

      data &mpOut (index= (&mpInFieldBK /unique));
         set &mpIn;
         set &lmvTmpSkTable key=&mpInFieldBK /unique;
      run;
      %error_check;

      %member_drop(&lmvTmpSkTable);
   %end;

   /* Очистка */
   %member_drop(&lmvTmpSasTable);
%mend etl_generate_sk_merge_new;
