/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 5e42621926cccb4adff4793d7ac1f5afe1c2d0b7 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обнуляет партицию в таблице.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS, в которой находится партиция
*     mpPartitionName         +  имя партиции
*
******************************************************************
*  Использует:
*     %error_check
*     %oracle_connect
*     %oracle_table_name
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %oracle_partition_truncate (mpTable=ETL_STG.ACCNTAB, mpPartitionName=ACCNTAB_P1);
*
******************************************************************
*  02-10-2013  Нестерёнок     Начальное кодирование
******************************************************************/

%macro oracle_partition_truncate (mpTable=, mpPartitionName=);
   %local lmvTable lmvLoginSet;
   %oracle_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   proc sql;
      %oracle_connect (mpLoginSet=&lmvLoginSet);
	  execute (
         alter session set ddl_lock_timeout = %rand_between(200,300)
      ) by oracle;
	  
      execute (
         alter table &lmvTable truncate partition &mpPartitionName
      ) by oracle;
   quit;
   %error_check (mpStepType=SQL_PASS_THROUGH);
%mend oracle_partition_truncate;
