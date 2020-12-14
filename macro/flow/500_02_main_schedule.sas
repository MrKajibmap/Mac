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
%etl_stream_start;
%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
%macro tech_002_main_schedule;
	%let etls_jobName = tech_002_main_schedule;
	%etl_job_start;
	%put _ALL_;
	%tech_main_schedule;
		
	%etl_job_finish;

%mend tech_002_main_schedule;

%tech_002_main_schedule;
%etl_stream_finish;

