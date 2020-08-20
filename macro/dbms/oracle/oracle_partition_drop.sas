/*****************************************************************
*  ВЕРСИЯ:
*     $Id: acd6f209601283442d20c3348f9ebec0e483218b $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Удаляет партицию в таблице.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS, из которой удаляется партиция
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
*     %oracle_partition_drop (mpTable=ETL_STG.ACCNTAB, mpPartitionName=ACCNTAB_P1);
*
******************************************************************
*  02-10-2013  Нестерёнок     Начальное кодирование
******************************************************************/

%macro oracle_partition_drop (mpTable=, mpPartitionName=, mpLoginSet=);
   %local lmvTable lmvLoginSet;
   %if %is_blank(mpLoginSet) %then %do;
	 %oracle_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);
   %end;
   %else %do;
	%let lmvTable=&mpTable;
	%let lmvLoginSet=&mpLoginSet;
   %end;
   
   %local lmvIsNotSQL;
   %let lmvIsNotSQL   = %eval (&SYSPROCNAME ne SQL);

   /* Открываем proc sql, если он еще не открыт */
   %if &lmvIsNotSQL %then %do;
	 proc sql;
	  %oracle_connect (mpLoginSet=&lmvLoginSet);
   %end; 
      execute (
         alter session set ddl_lock_timeout = %rand_between(200,300)
      ) by oracle;
	  
      execute (
         alter table &lmvTable drop partition &mpPartitionName update indexes
      ) by oracle;
   %if &lmvIsNotSQL %then %do;
	  %error_check (mpStepType=SQL_PASS_THROUGH);
	  disconnect from oracle;
	 quit;
   %end;   
%mend oracle_partition_drop;
