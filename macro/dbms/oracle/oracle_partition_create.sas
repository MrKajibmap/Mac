/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 233fed311bf7179e3ac894d28f70054ab38421b1 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создает партицию в таблице.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS, в которую добавляется партиция
*     mpPartitionName         +  имя партиции
*     mpType                  -  тип партиции (LIST)
*	  mpValues                -  значения для LIST партиции через запятую
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
*     %oracle_partition_create (mpTable=ETL_STG.ACCNTAB, mpPartitionName=ACCNTAB_P1);
*
******************************************************************
*  02-10-2013  Нестерёнок     Начальное кодирование
******************************************************************/

%macro oracle_partition_create (mpTable=, mpPartitionName=, mpType=, mpValues=);
   %local lmvTable lmvLoginSet;
   %oracle_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   proc sql;
      %oracle_connect (mpLoginSet=&lmvLoginSet);
	  execute (
         alter session set ddl_lock_timeout = %rand_between(200,300)
      ) by oracle;
	  
      execute (
         alter table &lmvTable add partition &mpPartitionName
		 %if &mpType = LIST %then
			values (%unquote(&mpValues));
      ) by oracle;
   quit;
   %error_check (mpStepType=SQL_PASS_THROUGH);
%mend oracle_partition_create;
