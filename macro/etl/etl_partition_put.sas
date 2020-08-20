/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 7ddc28b68c88b3c174be642f88ee089a6d07d782 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Размещает набор в партиции хранилища, маркирует версию.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpPartitionName         +  имя партиции (все другие партиции с тем же именем будут удалены)
*     mpVersion               +  версия, которой будет маркирована партиция
*     mpOut                   +  имя выходного набора, таблицы хранилища
*     mpOutOptions            -  дополнительные опции выходного набора
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
*     В трансформе transform_partition_put.sas
*
******************************************************************
*  10-10-2013  Нестерёнок     Начальное кодирование
*  18-10-2013  Морозов        Добавил условие для значения параметра auto
*  22-10-2013  Морозов        Добавлен поиск партиции по substr при условии AUTO
******************************************************************/

%macro etl_partition_put (
   mpIn                    =  ,
   mpPartitionName         =  ,
   mpVersion               =  ,
   mpOut                   =  ,
   mpOutOptions            =
);
   /* Получаем список партиций с тем же именем */
   %local lmvTableFull lmvTable lmvLoginSet;
   %&ETL_DBMS._table_name (mpSASTable=&mpOut,  mpOutFullNameKey=lmvTableFull,  mpOutNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   proc sql;
      %&ETL_DBMS._connect (mpLoginSet=&lmvLoginSet)
      ;
      create table drop_partitions as select
         "&mpOut" as mpTableNm,
         partition_name as mpPartitionNm
      from
         connection to &ETL_DBMS (
            select partition_name from user_tab_partitions
            where table_name = %&ETL_DBMS._string(&lmvTable)
            /* Если не задано значение, получаем существующую партицию */
%if &mpPartitionName eq %unquote(AUTO) %then %do;
               %let SHARD = %sysfunc(substr(&SHARD_ID,1,%sysfunc(index(&SHARD_ID, _))));
               and partition_name like %&ETL_DBMS._string(&SHARD.%)
%end;
%else %do;
               and partition_name like %&ETL_DBMS._string(&mpPartitionName.\_%) escape %&ETL_DBMS._string(\)
%end;
         );
      quit;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      %macro _single_partition_drop;
         %&ETL_DBMS._partition_drop(mpTable=&mpTableNm, mpPartitionName=&mpPartitionNm);
      %mend _single_partition_drop;

      /* Очищаем все партиции с тем же именем */
      %util_loop_data (mpData=drop_partitions, mpLoopMacro=_single_partition_drop);

      /* Добавляем записи в хранилище */
      %local lmvPartitionName;
%if &mpPartitionName eq %unquote(AUTO) %then %do;
      %let lmvPartitionName = &SHARD_ID;
%end;
%else %do;
      %let lmvPartitionName = &mpPartitionName._&mpVersion;
%end;
      %&ETL_DBMS._partition_create (mpTable=&mpOut, mpPartitionName=&lmvPartitionName);

      proc append
         base=&mpOut (
            OR_PARTITION=&lmvPartitionName
            &ETL_BULKLOAD_OPTIONS  BL_DIRECT_PATH=NO
            &mpOutOptions
         )
         data=&mpIn
      ;
      run;
      %error_check;
%mend etl_partition_put;