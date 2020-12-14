/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Поток закрытия текущих ресурсов
*
******************************************************************
*  05-10-2020  Борзунов     Начальное кодирование
******************************************************************/
*%etl_stream_start;
%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
%macro tech_001_Correct_Sched;
	%let etls_jobName = tech_001_Correct_Sched;
	%etl_job_start;

	%tech_correct_sched;
		
	%etl_job_finish;

%mend tech_001_Correct_Sched;
%tech_001_Correct_Sched;
*%etl_stream_finish;

