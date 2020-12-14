/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки csv в DP
*	
*
*  ПАРАМЕТРЫ:
*     Нет
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %rtp011_load_to_dp;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp011_load_to_dp;

	%let etls_jobName=rtp011_load_to_dp;
	%etl_job_start;
	
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_KOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_KOMP);


	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_KOMP);


	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_NONKOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_NONKOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_NONKOMP);

	%etl_job_finish;
	
%mend rtp011_load_to_dp;