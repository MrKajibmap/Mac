/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 59e1df7afcafb7ab315c755b21cc44eebc4cae0c $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос копирует Oracle статистику из одной партиции в другую
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к БД (ETL_SYS, ETL_STG и т.д.)
*     mpIn                    +  имя таблицы
*	  mpPartitionFrom		  +  имя партиции из которой происходит копирование статистики
*	  mpPartitionTo			  +  имя партиции в которую копируются статистика из mpPartitionFrom
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
*     %macro oracle_copy_partition_statistics(mpLoginSet=RDB_BASE, mpIn=A01_ACCOUNT, mpPartitionFrom=P_11111, mpPartitionTo=P_22222)
*
******************************************************************
*  19-01-2017  Городничев     Начальное кодирование
******************************************************************/

%macro oracle_copy_partition_statistics (
   mpLoginSet                 =  ,
   mpIn                       =  ,
   mpPartitionFrom			  =  ,
   mpPartitionTo			  =  
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
		
		 execute (
			 BEGIN
				DBMS_STATS.COPY_TABLE_STATS( %&ETL_DBMS._string(&lmvSchema),
											 %&ETL_DBMS._string(&mpIn),
											 %&ETL_DBMS._string(&mpPartitionFrom),
											 %&ETL_DBMS._string(&mpPartitionTo));
			END;
		 ) by &ETL_DBMS;
		
		%error_check (mpStepType=SQL_PASS_THROUGH);
	quit;


%mend oracle_copy_partition_statistics;
