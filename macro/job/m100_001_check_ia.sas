/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Чекалка статусов ресурсов. 
*
******************************************************************
*  09-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro m100_001_check_ia;

	%let etls_jobName=m100_001_check_ia;
	%etl_job_start;
	
	proc sql noprint;
		create table WORK.IA_RESOURCES as
		select *
		from IA.ETL_PROCESS_LOG
		where IA_STATUS_CD = "L"
			and datepart(IA_FINISH_DTTM) = &ETL_CURRENT_DT.
			and (SAS_START_DTTM is null or SAS_STATUS_CD="E")
		;
	quit;

	proc sql;
		create table WORK.OPEN_RESOURCES as
		select ir.ETL_PROCESS_ID, rxs.resource_id
		from IA_RESOURCES ir
		inner join ETL_SYS.ETL_RESOURCE_X_SOURCE rxs
			on rxs.table_nm=ir.resource_name
		;
	quit;
	
	%macro add_resources;
		%resource_add (mpResourceId=&resource_id., mpDate=&JOB_START_DTTM., mpStatus=A);
	%mend add_resources;
	
	%if %member_obs(mpData=WORK.OPEN_RESOURCES) eq 24 %then %do;
	
		%util_loop_data (mpData=WORK.OPEN_RESOURCES, mpLoopMacro=add_resources);
		
		proc sql;
			update IA.ETL_PROCESS_LOG
			set SAS_STATUS_CD = 'A',
				SAS_START_DTTM = &JOB_START_DTTM.
			where ETL_PROCESS_ID in
				(select ETL_PROCESS_ID
				from WORK.OPEN_RESOURCES)
			;
		quit;
	%end;
	
	%etl_job_finish;

%mend m100_001_check_ia;



