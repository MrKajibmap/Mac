/*****************************************************************
* ВЕРСИЯ:
*     $Id: 5e7aff0c937106df389c3fd442d9a57948de34dd $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*     Возвращяет первичный ключ таблицы ORACLE в виде списка 
*	  (разделитель пробел) в макропеременную.
*
* ПАРАМЕТРЫ:
*     mpTable         +  Имя таблицы
*     mpLib           +  Библиотека Oracle
*     mpOutVar        +  Имя макропременной куда передать PK
*
******************************************************************
* ИСПОЛЬЗУЕТ:
*	  DWF macros
*    
* УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     mpOutVar
*
******************************************************************
* ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*	%oracle_get_pkey(mpTable=RESULT_TRANCHE, mpLib=TEST_OUT, mpOutVar=mvPKey);
*
******************************************************************
* 17-01-2019   Колосов  Начальное кодирование
******************************************************************/

%macro oracle_get_pkey(mpTable=, mpLib=, mpOutVar=);
	proc sql noprint feedback;
		  %oracle_connect(mpLoginSet=&mpLib);
			   select column_name into :lmvPKey separated by " " from 
				connection to &ETL_DBMS. ( select * from (
					select column_name 
					from all_cons_columns 
					where constraint_name = ( 
							select constraint_name from all_constraints 
							where upper(table_name) = upper(%oracle_string(&mpTable)) 
								and owner = %oracle_string(&&&mpLib._CONNECT_SCHEMA) 
								and constraint_type = 'P' 
					)
				) 
				); 
	            %error_check (mpStepType=SQL_PASS_THROUGH);
				disconnect from &ETL_DBMS;
	quit;

	%log4sas_debug (dwf.macro.oracle_get_pkey, %bquote(lmvPKey=&lmvPKey) );

	%let &&mpOutVar=&lmvPKey;
%mend oracle_get_pkey;