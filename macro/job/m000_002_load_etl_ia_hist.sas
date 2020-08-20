/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загрузка ресурсов из ETL_STG в ETL_IA 
*
******************************************************************
*  14-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro m000_002_load_etl_ia_hist;

	%let etls_jobName=m_000_002_load_etl_ia_hist;
	%etl_job_start;
		
		proc sql;
		create view open_res_tmp as
		select  put(resource_id,res_id_cd.) as tpResource
		from etl_sys.etl_resource
		where put(resource_id,res_id_cd.) in (select strip(tranwrd(tranwrd(memname, "IA_", ""), "_HISTORY", "")) as nameee
											from sashelp.vtable 
											where libname = 'IA'
											and memname contains 'HISTORY')
		;
	quit;
	
	data open_res;
	set open_res_tmp;
	seq = _N_;
	run;
	
	%macro m_load_etl_ia_hist;

		%let seq=&seq;
		%let tpResource = %trim(&tpResource.);
		
		filename par_&seq temp;
			
		data _null_;
			file par_&seq;                   
			put "%nrstr(%%)load_etl_ia_hist(mpResource=&tpResource)";
		run;
		
		systask command 
		"""/opt/sas/mcd_config/bin/start_sas.sh"" ""%sysfunc(pathname(par_&seq))"" 000_load_data_history load_etl_ia_&tpResource"
		taskname=task_&seq status=rc_&seq;
		
		%if &SYSRC ne 0 %then %do;
			%put WARNING: сессия не была запущена успешно;
		%end;

	%mend m_load_etl_ia_hist;


	%util_loop_data( 
	 mpLoopMacro       =  m_load_etl_ia_hist,
	 mpData            =  open_res);
	
	
	
	%macro m_load_etl_ia_hist_waitfor;
		task_&seq
	%mend m_load_etl_ia_hist_waitfor;

	waitfor _all_
	%util_loop_data( 
	 mpLoopMacro       =  m_load_etl_ia_hist_waitfor,
	 mpData            =  open_res);

		
	%etl_job_finish;	
			

%mend m000_002_load_etl_ia_hist;