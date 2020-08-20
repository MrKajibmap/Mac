/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загрузка ресурсов, со статусом N, из ETL_STG в ETL_IA 
*
******************************************************************
*  14-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro m300_001_load_etl_ia;

	%let etls_jobName=m300_001_load_etl_ia;
	%etl_job_start;

		
	proc sql;
		create view open_res_tmp as
		select  put(resource_id,res_id_cd.) as tpResource
				,datepart(available_dttm) as tpAvailableDt
				,version_id as tpVersionId
		from etl_sys.etl_resource_registry
		where version_id in(select MIN(version_id) 
							from etl_sys.etl_resource_registry 
							where status_cd = 'N'
							group by resource_id)
		;
	quit;
	
	data open_res;
	set open_res_tmp;
	seq = _N_;
	run;
	
	%macro m_load_etl_ia_reg;
	
		%let seq=&seq;
		%let tpResource = &tpResource.;
		%let tpAvailableDt = &tpAvailableDt.;
		%let tpVersionId = &tpVersionId.;
		
		filename par_&seq temp;
			
		data _null_;
			file par_&seq;                   
			put "%nrstr(%%)load_etl_ia(mpResource=&tpResource,mpAvailableDt=&tpAvailableDt,mpVersionId=&tpVersionId)";
		run;
		
		systask command 
		"""/opt/sas/mcd_config/bin/start_sas.sh"" ""%sysfunc(pathname(par_&seq))"" 300_load_data_regular_etl_ia load_etl_ia_&tpResource"
		taskname=task_&seq status=rc_&seq;
		
		%if &SYSRC ne 0 %then %do;
			%put WARNING: сессия не была запущена успешно;
		%end;
	
	%mend m_load_etl_ia_reg;
	
	%util_loop_data( 
	 mpLoopMacro       =  m_load_etl_ia_reg,
	 mpData            =  open_res);
	
	
	
	%macro m_load_etl_ia_reg_waitfor;
		task_&seq
	%mend m_load_etl_ia_reg_waitfor;

	waitfor _all_
	%util_loop_data( 
	 mpLoopMacro       =  m_load_etl_ia_reg_waitfor,
	 mpData            =  open_res);
	
	%etl_job_finish;
	
		

%mend m300_001_load_etl_ia;