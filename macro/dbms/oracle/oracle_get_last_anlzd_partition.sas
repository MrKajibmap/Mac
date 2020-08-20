/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 694be3bd80eeef99a8def299d0e0e086d404a394 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает последнюю партицию по которой собиралась статистика (dbms_stats)
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к БД (ETL_SYS, ETL_STG и т.д.)
*     mpIn                    +  имя таблицы
*	  mpOutPartition		  +  максимальный номер выходной партиции
*
******************************************************************
*  Использует:
*     %error_check
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %macro tst;
* 	 	%local lmvPOut;
* 		%oracle_get_last_anlzd_partition(mpLoginSet = RDB_BASE,mpIn = A01_ACCOUNT, mpOutPartition = lmvPOut);
* 		%put &lmvPOut;
* 	 %mend tst;
* 
* %tst;
*
******************************************************************
*  12-09-2017  Городничев     Начальное кодирование
******************************************************************/

%macro oracle_get_last_anlzd_partition (
   mpLoginSet                 =  ,
   mpIn                       =  ,  
   mpOutPartition			  =
);


	%local lmvSchema;
	%if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
        %let lmvSchema = &&&mpLoginSet._CONNECT_SCHEMA;
    %end;
	%else %do;
	    %let lmvSchema = &mpLoginSet;
	%end;


proc sql noprint;
	%&ETL_DBMS._connect(mpLoginSet=&mpLoginSet);

	/* регулярка отбирает строки где встречаются 5 и более подряд идущих чисел */
	select
	 LAST_ANALYZED_PARTITION into :&mpOutPartition
	from connection to &ETL_DBMS
	(
		select max(partition_name) keep(dense_rank first order by last_analyzed desc nulls last) as  LAST_ANALYZED_PARTITION
		from all_tab_partitions
		where table_name = upper(%&ETL_DBMS._string(&mpIn))
		  and partition_name not like 'P_DUM%'
		  and table_owner = upper(%&ETL_DBMS._string(&lmvSchema))
		  and last_analyzed is not null	

	); 
	
	disconnect from &ETL_DBMS;
	%error_check (mpStepType=SQL_PASS_THROUGH);
quit;

%mend oracle_get_last_anlzd_partition;