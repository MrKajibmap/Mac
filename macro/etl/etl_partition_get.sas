/*****************************************************************
*  ВЕРСИЯ:
*     $Id: ae558ed6a86a31513d907490cce1da000b9d7af4 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает набор из партиции хранилища.
*     Работает в глобальном режиме или внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     tpIn                    +  имя входного набора, таблицы хранилища
*     tpShardId               +  версия, которой маркирована партиция и выходной набор
*                                Как правило, &STREAM_ID
*     tpOut                   +  имя выходного набора, локальной таблицы
*
******************************************************************
*  Использует:
*     %error_check
*     %ETL_DBMS_*
*     %member_names
*     %util_loop_data
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     В трансформе transform_partition_get.sas
*
******************************************************************
*  10-10-2013  Нестерёнок     Начальное кодирование
*  18-10-2013  Морозов        Добавлено условие для обработки со значением AUTO
******************************************************************/

%macro etl_partition_get(
   mpIn=,
   mpShardId=,
   mpOut=
);
   /* Если целевая таблица существует и маркирована этой версией, то ничего делать не надо */
   %if %sysfunc(exist(&mpOut)) %then %do;
      %if &mpShardId eq %member_attr(mpData=&mpOut, mpAttr=LABEL) %then %do;
         %log4sas_info (dwf.macro.etl_partition_get, Table &mpOut already exists with version &mpVersion);
         %return;
      %end;
   %end;

   %if &mpShardId = %unquote(AUTO) %then %do;

   %let mpVersion = %unquote(&STREAM_ID);

   /* Получаем имя партиции */
   %local lmvTableFull lmvTable lmvLoginSet lmvPartitionName;
   %&ETL_DBMS._table_name (mpSASTable=&mpIn,  mpOutFullNameKey=lmvTableFull,  mpOutNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   %let lmvPartitionName = ;



   proc sql noprint;
      %&ETL_DBMS._connect (mpLoginSet=&lmvLoginSet)
      ;
      select
         partition_name into :lmvPartitionName
      from
         connection to &ETL_DBMS (
            select partition_name from user_tab_partitions
            where table_name = %&ETL_DBMS._string(&lmvTable)
            and partition_name like %&ETL_DBMS._string(%\_&mpVersion) escape %&ETL_DBMS._string(\)
      );
   quit;
   %error_check (mpStepType=SQL_PASS_THROUGH);

   %let SHARD_ID =  &lmvPartitionName;

   /* Если партиция не найдена, то ошибка */
   %if %is_blank(lmvPartitionName) %then %do;
      %job_event_reg (mpEventTypeCode  =  PARTITION_NOT_FOUND,
                      mpEventValues    =  %bquote(В таблице &mpIn нет партиции с версией &mpVersion) );
      %return;
   %end;

   /* Иначе создаем локальную таблицу и маркируем */
   data &mpOut
         (label= &mpVersion)
   ;
      set &mpIn (or_partition= &lmvPartitionName);
   run;
   %end;
   %error_check (mpStepType=DATA);
%mend etl_partition_get;