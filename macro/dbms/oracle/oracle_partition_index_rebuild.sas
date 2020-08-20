/*****************************************************************
*  ВЕРСИЯ:
*     $Id: a3448a2910b4685b83db7101ac0c01133a0e27b1 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Перестраивает индексы в партиции.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS, в которой перестраиваются индексы
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
*     %oracle_partition_index_rebuild (mpTable=ETL_STG.ACCNTAB, mpPartitionName=ACCNTAB_P1);
*
******************************************************************
*  09-03-2017  Сазонов     Начальное кодирование
******************************************************************/

%macro oracle_partition_index_rebuild (mpTable=, mpPartitionName=, mpLoginSet=);
   %local lmvTable lmvLoginSet;
   %if %is_blank(mpLoginSet) %then %do;
	 %oracle_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);
   %end;
   %else %do;
	%let lmvTable=&mpTable;
	%let lmvLoginSet=&mpLoginSet;
   %end;
   
   %local lmvIsNotSQL lmvIndexList;
   %let lmvIsNotSQL   = %eval (&SYSPROCNAME ne SQL);

   /* Открываем proc sql, если он еще не открыт */
   %if &lmvIsNotSQL %then %do;
	 proc sql;
	  %oracle_connect (mpLoginSet=&lmvLoginSet);
   %end;      
      %oracle_get_table_indexes (mpTable=&lmvTable, mpOutIndexList=lmvIndexList); 
	  %macro _alter(mpIndex);
		  execute (
			 alter index &mpIndex rebuild partition &mpPartitionName
		  ) by oracle;
	  %mend _alter;
	  %util_loop(mpLoopMacro=_alter, mpWith=&lmvIndexList);
   %if &lmvIsNotSQL %then %do;
	  %error_check (mpStepType=SQL_PASS_THROUGH);
	  disconnect from oracle;
	 quit;
   %end;   
%mend oracle_partition_index_rebuild;
