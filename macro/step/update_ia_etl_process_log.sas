/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Update ia.etl_process_log
*		переводит в статус L загруженные ресурсы
*
*  ПАРАМЕТРЫ:
*
******************************************************************
*  Использует:
*			
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %update_ia_etl_process_log;
*
****************************************************************************
*  27-04-2020  Зотиков     Начальное кодирование
****************************************************************************/
%macro update_ia_etl_process_log;
	
	proc sql;
		create table upd_res as
		select put(resource_id,res_id_cd.) as ttpResource
		from etl_sys.etl_resource_registry
		where version_id in(select MAX(version_id) 
							from etl_sys.etl_resource_registry 
							where datepart(available_dttm) = today() 
							group by resource_id)
		;
	quit;
	/*
	%let lmvFinishDttm = %sysfunc(datetime());

	proc sql;
		update IA.ETL_PROCESS_LOG
		set SAS_STATUS_CD = 'L',
			SAS_FINISH_DTTM = &lmvFinishDttm.
		where IA_STATUS_CD = "L"
			and SAS_STATUS_CD = "A"
			and datepart(IA_FINISH_DTTM) = today()
			and datepart(SAS_START_DTTM) = today()
			and RESOURCE_NAME in (select tpResource from upd_res )
		;
	quit;
	*/
	
	proc sql;
		create table tmp_process_log
		(RESOURCE_NAME char(800), 
		IA_FINISH_DTTM num format=DATETIME25., 
		IA_STATUS_CD char(16), 
		SAS_STATUS_CD char(160),
		SAS_START_DTTM num format=DATETIME25., 
		SAS_FINISH_DTTM num format=DATETIME25.,
		SAS_ROW_CNT num, 
		SAS_ERROR_CD char(160),
		SAS_ERROR_DESC char(1024));
	quit;
	
	%macro ins_tmp_process_log;
	
		%let mvStatusTbl = %trim(%upcase(STATUS_&ttpResource.));
		
		%if %member_exists (etl_stg.&mvStatusTbl.) %then %do;
		
			proc append base=tmp_process_log data=etl_stg.&mvStatusTbl. force;
			run;
			/*
			proc sql;
				%postgres_connect (mpLoginSet=ETL_STG);
					execute      
					(drop table etl_stg.&mvStatusTbl.;
					) 
					by postgres;  
				disconnect from postgres;
			quit;
			*/
		%end;
	
	%mend ins_tmp_process_log;
	
	proc sql;
		create table etl_stg.all_tmp_process_log as 
		select * 
		from tmp_process_log ;
	quit;
	
	%util_loop_data( 
	 mpLoopMacro       =  ins_tmp_process_log,
	 mpData            =  upd_res);
	 
	 proc sql;
		update IA.ETL_PROCESS_LOG as t1 
		set SAS_STATUS_CD = (select SAS_STATUS_CD 
							 from tmp_process_log 
							 where t1.RESOURCE_NAME = RESOURCE_NAME
							 and t1.IA_STATUS_CD = IA_STATUS_CD
							 and t1.SAS_STATUS_CD = SAS_STATUS_CD
							 and datepart(t1.IA_FINISH_DTTM) = datepart(IA_FINISH_DTTM)
							 and datepart(t1.SAS_START_DTTM) = datepart(SAS_START_DTTM))
			,SAS_FINISH_DTTM = (select SAS_FINISH_DTTM 
							 from tmp_process_log 
							 where t1.RESOURCE_NAME = RESOURCE_NAME
							 and t1.IA_STATUS_CD = IA_STATUS_CD
							 and t1.SAS_STATUS_CD = SAS_STATUS_CD
							 and datepart(t1.IA_FINISH_DTTM) = datepart(IA_FINISH_DTTM)
							 and datepart(t1.SAS_START_DTTM) = datepart(SAS_START_DTTM))
			,SAS_ROW_CNT = (select SAS_ROW_CNT 
							 from tmp_process_log 
							 where t1.RESOURCE_NAME = RESOURCE_NAME
							 and t1.IA_STATUS_CD = IA_STATUS_CD
							 and t1.SAS_STATUS_CD = SAS_STATUS_CD
							 and datepart(t1.IA_FINISH_DTTM) = datepart(IA_FINISH_DTTM)
							 and datepart(t1.SAS_START_DTTM) = datepart(SAS_START_DTTM))
			,SAS_ERROR_CD = (select SAS_ERROR_CD 
							 from tmp_process_log 
							 where t1.RESOURCE_NAME = RESOURCE_NAME
							 and t1.IA_STATUS_CD = IA_STATUS_CD
							 and t1.SAS_STATUS_CD = SAS_STATUS_CD
							 and datepart(t1.IA_FINISH_DTTM) = datepart(IA_FINISH_DTTM)
							 and datepart(t1.SAS_START_DTTM) = datepart(SAS_START_DTTM))
			,SAS_ERROR_DESC = (select SAS_ERROR_DESC 
							 from tmp_process_log 
							 where t1.RESOURCE_NAME = RESOURCE_NAME
							 and t1.IA_STATUS_CD = IA_STATUS_CD
							 and t1.SAS_STATUS_CD = SAS_STATUS_CD
							 and datepart(t1.IA_FINISH_DTTM) = datepart(IA_FINISH_DTTM)
							 and datepart(t1.SAS_START_DTTM) = datepart(SAS_START_DTTM))
			  where t1.RESOURCE_NAME in (select RESOURCE_NAME 
										 from tmp_process_log)
			  and t1.IA_STATUS_CD = "L"
			  and t1.SAS_STATUS_CD = "A"
			  and datepart(t1.IA_FINISH_DTTM) = today()
			  and datepart(t1.SAS_START_DTTM) = today()
		;
	quit;
	
	/*
	proc sql;
		%postgres_connect (mpLoginSet=ETL_STG);
			execute      
			(drop table etl_stg.all_tmp_process_log;
			) 
			by postgres;  
		disconnect from postgres;
	quit;
	*/
	
%mend update_ia_etl_process_log;