/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загрузка ресурсов, со статусом A, из IA в ETL_STG 
*
******************************************************************
*  09-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro m200_001_load_etl_stg;

	%let etls_jobName=m200_001_load_etl_stg;
	%etl_job_start;
		
	proc sql;
		create view open_res_tmp as
		select put(resource_id,res_id_cd.) as tpResource, version_id as tpVersion
		from etl_sys.etl_resource_registry
		where version_id in(select MIN(version_id) 
							from etl_sys.etl_resource_registry 
							where status_cd = 'A' 
							group by resource_id)
		;
	quit;
	
	data open_res;
		set open_res_tmp;
		seq = _N_;
	run;
	
	%macro m_load_etl_stg_reg;

		%let seq=&seq;
		%let tpResource = %trim(&tpResource.);
		
		filename par_&seq temp;
			
		data _null_;
			file par_&seq;                   
			put "%nrstr(%%)load_etl_stg(mpResource=&tpResource, mpVersion=&tpVersion)";
		run;
		
		systask command 
		"""/opt/sas/mcd_config/bin/start_sas.sh"" ""%sysfunc(pathname(par_&seq))"" 200_load_data_regular_etl_stg load_etl_stg_&tpResource"
		taskname=task_&seq status=rc_&seq;
		
		%if &SYSRC ne 0 %then %do;
			%put WARNING: сессия не была запущена успешно;
		%end;

	%mend m_load_etl_stg_reg;


	%util_loop_data( 
	 mpLoopMacro       =  m_load_etl_stg_reg,
	 mpData            =  open_res);	
	
	%macro m_load_etl_stg_reg_waitfor;
		task_&seq
	%mend m_load_etl_stg_reg_waitfor;

	waitfor _all_
	%util_loop_data( 
	 mpLoopMacro       =  m_load_etl_stg_reg_waitfor,
	 mpData            =  open_res);
		
	%etl_job_finish;

%mend m200_001_load_etl_stg;
