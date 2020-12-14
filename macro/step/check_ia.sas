/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Проверяет статус ресурса в IA.ETL_PROCESS_LOG. 
*	  Если ресурс за сегодня в статусе(IA слоя IA_STATUS_CD) L, заводит ресурс  в etl_sys.etl_resource_registry, 
*	  в IA.ETL_PROCESS_LOG производит апдейт, переводя статус (статус SAS SAS_STATUS_CD) в A
*
*  ПАРАМЕТРЫ:
*     mpResource                -  имя ресурса 
*									
*	  mpResourceId 				- id ресурса  
*									
*
******************************************************************
*  Использует: 
*				%resource_add
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. должен быть обёрнут в %etl_job_start; %etl_job_finish;
*
******************************************************************
*  Пример использования:
*	в джобе m_001_010_check_ia.sas
*
****************************************************************************
*  17-04-2020  Зотиков     Начальное кодирование
*  20-08-2020  Борзунов	   Замена переменной ETL_CURRENT_DT на today в первом запросе к версии ресурса
****************************************************************************/
%macro check_ia;

	%local lmvVersion;
	%let mpResourceIa = &mpResourceIa;
	%let lmvVersion = ;

	proc sql noprint;
		select ETL_PROCESS_ID into :lmvVersion
		from IA.ETL_PROCESS_LOG
		where IA_STATUS_CD = "L"
			and datepart(IA_FINISH_DTTM) = /*&ETL_CURRENT_DT.*/ today()
			and (SAS_START_DTTM is null or SAS_STATUS_CD="E")
			and RESOURCE_NAME = upcase("&mpResourceIa.")
		;
	quit;

	%if %length(&lmvVersion.) %then %do;
	
		%resource_add (mpResourceId=&mpResourceId., /*mpVersion=&lmvVersion.,*/ mpDate=&JOB_START_DTTM., mpStatus=A);

		proc sql;
			update IA.ETL_PROCESS_LOG
			set SAS_STATUS_CD = 'A',
				SAS_START_DTTM = &JOB_START_DTTM.
			where IA_STATUS_CD = "L"
				and datepart(IA_FINISH_DTTM) = today()
				and SAS_START_DTTM is null
				and RESOURCE_NAME = upcase("&mpResourceIa.")
				and ETL_PROCESS_ID = &lmvVersion.
			;
		quit;
	
	%end;
/* test */
%mend check_ia;