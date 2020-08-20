/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 3e5be322bac7febada2a24968f56c282fa225f3f $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Размещает выгруженный набор в архиве, регистрирует выгрузку в реестре.
*
*  ПАРАМЕТРЫ:
*     mpInData             +  имя входного набора, выгруженной таблицы
*     mpInRegistry         -  имя входного набора, списка обновляемых записей в реестре
*                             Если не задан, то будет определен по архивной таблице
*     mpNoOpenAction       -  Действие при отсутствии открытых ресурсов в реестре,
*                             ERR - вызывать ошибку, NOP - ничего не делать,
*                             по умолчанию ERR
*     mpOut                +  имя выходного набора, архивной таблицы
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_arch_delete
*     %etl_extract_common
*     %etl_extract_registry_*
*     %member_attr
*     %member_obs
*     %resource_update
*     %resource_attr_update
*     %unique_id
*     %util_loop_data
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     в трансформации transform_archive_put.sas
*
******************************************************************
*  15-11-2013  Нестерёнок     Начальное кодирование
*  15-11-2013  Нестерёнок     Добавлен сбор кол-ва записей
*  02-12-2013  Нестерёнок     Добавлен сбор размера записей
*  16-05-2014  Нестерёнок     Введена политика обновления при перезапуске
*  11-08-2014  Кузенков       Добавлен mpNoOpenAction
*  09-02-2015  Сазонов        Создание структуры переписано для db2
******************************************************************/

%macro etl_archive_put (
      mpInData                =  ,
      mpInRegistry            =  ,
      mpNoOpenAction          =  ERR,
      mpOut                   =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем состояние записей по выгрузке */
   %local lmvExtractStatus;

%if %is_blank(mpInRegistry) %then %do;
   /* Если список реестра не задан, то получаем записи так же, как и для Extract */
   %let mpInRegistry   = work.tr_arch_put_&lmvUID._reg;

   %etl_extract_common (
      mpData            =  &mpOut,
      mpResourceId      =  BY_ARCH,
      mpVersion         =  MIN,
      mpNoOpenAction    =  &mpNoOpenAction,
      mpOutRegistry     =  &mpInRegistry,
      mpOutStatusKey    =  lmvExtractStatus
   );

%end;
%else %do;
   /* Иначе только получаем состояние открытых записей */
   %etl_extract_common (
      mpData            =  &mpOut,
      mpInRegistry      =  &mpInRegistry,
      mpNoOpenAction    =  &mpNoOpenAction,
      mpOutStatusKey    =  lmvExtractStatus
   );

%end;

   /* Если ошибка, то выходим */
   %if &lmvExtractStatus lt 0 %then %return;

   /* Если требуется (пере)выгрузка */
   %if (&lmvExtractStatus = 0) or (&lmvExtractStatus = 2) %then %do;
      /* Очищаем архив, если нужно */
      %if &lmvExtractStatus ne 0 %then %do;
         %etl_arch_delete (mpIn=&mpInRegistry, mpOut=&mpOut);
      %end;

      /* Нетранзакционно: */
      /* создаем временную таблицу и заливаем в нее данные */
      %local lmvTmpName lmvArchiveTable;
      %let lmvTmpName  =  etl_arch_upload_&lmvUID.;
      %&ETL_DBMS._table_name (mpSASTable=&mpOut, mpOutFullNameKey=lmvArchiveTable);

      /* воссоздаем структуру архивной таблицы */
      proc sql;
         %&ETL_DBMS._connect (mpLoginSet=ETL_STG);
%if &ETL_DBMS = db2 %then %do;
         execute (
            create table &lmvTmpName as (select * from &lmvArchiveTable) with no data
         ) by &ETL_DBMS
         ;
%end;
%else %do;
		 execute (
            create table &lmvTmpName as select * from &lmvArchiveTable
            where 1=0
         ) by &ETL_DBMS
         ;
%end;
         %error_check (mpStepType=SQL_PASS_THROUGH);
      quit;

      /* заливаем данные */
      proc append base=ETL_STG.&lmvTmpName (&ETL_BULKLOAD_OPTIONS) data=&mpInData;
      run;
      %error_check;
      %if &STEP_RC ne 0 %then %return;

      /* Макро для обновления */
%macro _tr_arch_put_loop;
         %resource_update (
            mpResourceId=ALL, mpVersion=&version_id,
            mpDate=NOCHG, mpProcessedBy=&JOB_ID, mpStatus=P,
            mpNotFound=ERR,
            mpConnection=&ETL_DBMS
         );
         %resource_attr_update (
            mpResourceId=ALL, mpVersion=&version_id,
            mpArchRows=%member_obs(mpData=&mpInData),
            mpArchRowLen=%member_attr(mpData=&mpInData, mpAttr=LRECL),
            mpNotFound=ADD,
            mpConnection=&ETL_DBMS
         );
%mend _tr_arch_put_loop;

      /* Транзакционно: */
      %etl_transaction_start (mpLoginSet=ETL_STG);
         /* переносим записи в архив... */
         execute (
            insert into &lmvArchiveTable
            select * from &lmvTmpName
         ) by &ETL_DBMS
         ;
         %error_check (mpStepType=SQL_PASS_THROUGH);

         /* ... и обновляем записи в реестре */
         %util_loop_data (mpLoopMacro=_tr_arch_put_loop, mpData=&mpInRegistry);
      %etl_transaction_finish;

      /* Очистка */
      %member_drop(ETL_STG.&lmvTmpName);

   %end;

   /* Переводим группу в N, если все оригиналы выгружены */
   %etl_extract_registry_set (
      mpInRegistry      =  &mpInRegistry,
      mpNoOpenAction    =  &mpNoOpenAction,
      mpLogTable        =  &mpOut
   );
%mend etl_archive_put;
