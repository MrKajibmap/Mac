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
%macro m_001_001_check_ia;

	%let etls_jobName=m_001_001_check_ia;
	%etl_job_start;
	
	%local lmvObs;

	proc sql;
		create table RESOURCES as
		select resource_id as mpResourceId,
			resource_cd as mpResource
		from ETL_SYS.ETL_RESOURCE
		where resource_cd in ("COST_PRICE",
							"PBO_LOC_ATTRIBUTES",
							"PBO_CLOSE_PERIOD",
							"SEGMENT",
							"CHANNEL",
							"MACRO_FACTOR",
							"PRODUCT_CHAIN",
							"PMIX_SALES",
							"PBO_SALES",
							"RECEIPT",
							"EVENTS",
							"PRODUCT",
							"PRODUCT_HIERARCHY",
							"PRODUCT_ATTRIBUTES",
							"PBO_LOCATION",
							"PBO_LOС_HIERARCHY",
							"PBO_LOС_ATTRIBUTES",
							"PROMO",
							"PROMO_X_PBO",
							"PROMO_X_PRODUCT",
							"MEDIA",
							"PRICE",
							"ASSORT_MATRIX",
							"MACRO",
							"COMPETITOR",
							"COMP_MEDIA",
							"WEATHER"/*,
							"TEST"*/
							)
		;
	quit;
	
	%let lmvObs = %member_obs(mpData=WORK.RESOURCES);
	
	%if &lmvObs. gt 0 %then %do;
	
		%util_loop_data (mpData=work.RESOURCES, mpLoopMacro=check_ia);
	
	%end;
	
	%etl_job_finish;

%mend m_001_001_check_ia;



