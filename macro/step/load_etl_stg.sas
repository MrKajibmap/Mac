/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Размещает выгруженный набор из Oracle в архиве (Postgres), регистрирует выгрузку в реестре.
*
*  ПАРАМЕТРЫ:
*   	mpResource       + ресурс
*   	mpVersion        + версия
*
******************************************************************
*  Использует:
*				etl_archive_put
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %load_etl_stg;
*
****************************************************************************
*  20-04-2020  Зотиков     Начальное кодирование
****************************************************************************/
%macro load_etl_stg(
					mpResource=, 
					mpVersion=);
					
	%let etls_jobName=load_etl_stg;
	%etl_job_start;
	%M_LOG_EVENT(START, load_etl_stg_&mpResource.);
	%local lmvIn lmvOut lmvKeep lmvKeepComma;

	proc sql noprint;
		select table_nm into :lmvIn
		from ETL_SYS.ETL_RESOURCE_X_SOURCE
		where put(resource_id,res_id_cd.)="&mpResource."
		;
		select arch_nm into :lmvOut
		from ETL_SYS.ETL_RESOURCE_X_ARCH
		where put(resource_id,res_id_cd.)="&mpResource."
		;
	quit;
	
	%let lmvKeep = %member_vars (etl_stg.&lmvOut.);
	%let lmvKeepComma = %member_vars (etl_stg.&lmvOut., mpDlm=%str(, ));

	proc sql;
		create table clms as
		select *
		from sashelp.vcolumn 
		where libname = 'ETL_STG' and memname = "&lmvOut." and format = 'DATE9.'
		;
	quit;

	%let lmvOutDtVarsCnt = %member_obs (mpData=work.clms);

	%if &lmvOutDtVarsCnt. gt 0 %then %do;
		proc sql;
			select name into :lmvOutDtVarsKeep separated by ' '
			from work.clms
			;
		quit;
	
		
		proc sql;
			select name into :lmvOutDtVarsNm1 %if &lmvOutDtVarsCnt. gt 1 %then %do; - :lmvOutDtVarsNm&lmvOutDtVarsCnt. %end; 
			from work.clms
			;
		quit;

	%end;

	data work.&mpResource.(keep=&lmvKeep.);
		%if &lmvOutDtVarsCnt. gt 0 %then %do;
			format &lmvOutDtVarsKeep. date9.;
		%end;
		set IA.&lmvIn.;
		%if &lmvOutDtVarsCnt. gt 0 %then %do;
			%do i=1 %to &lmvOutDtVarsCnt.;
				if &&lmvOutDtVarsNm&i..=. then do;
					&&lmvOutDtVarsNm&i..=.;
				end;
				else do;
					&&lmvOutDtVarsNm&i.. = datepart(&&lmvOutDtVarsNm&i..);
				end;
			%end;
		%end;
		etl_extract_id = &mpVersion.;
	run; 
	

	%etl_archive_put(mpInData=work.&mpResource.,mpOut=etl_stg.&lmvOut.);
	%let STEP_RC_N      = &STEP_RC;
    %let STEP_MESSAGE_N = &STEP_MESSAGE;
	%let ETL_MODULE_RC_N = &ETL_MODULE_RC;
	
	%let lmvFinishDttm = %sysfunc(datetime());
	
	proc sql;
		select count(*) into :lmvObsRes 
		from work.&mpResource.
		;
	quit;
	
	%error_check;
	
	%if &ETL_MODULE_RC ne 0 %then %do;
		
		proc sql;
			update IA.ETL_PROCESS_LOG
			set SAS_STATUS_CD = "E",
				SAS_FINISH_DTTM = &lmvFinishDttm.,
				SAS_ROW_CNT = 0,
				SAS_ERROR_CD = "&ETL_MODULE_RC.",
				SAS_ERROR_DESC = "&STEP_MESSAGE."
			where IA_STATUS_CD = "L"
				and SAS_STATUS_CD = "A"
				and datepart(IA_FINISH_DTTM) = &ETL_CURRENT_DT.
				and datepart(SAS_START_DTTM) = &ETL_CURRENT_DT.
				and RESOURCE_NAME = "&lmvIn."
			;
		quit;
		
	%end;
	%else %do;
		
		proc sql;
			update IA.ETL_PROCESS_LOG
			set SAS_STATUS_CD = "L",
				SAS_FINISH_DTTM = &lmvFinishDttm.,
				SAS_ROW_CNT = &lmvObsRes.,
				SAS_ERROR_CD = "",
				SAS_ERROR_DESC = ""
			where IA_STATUS_CD = "L"
				and SAS_STATUS_CD = "A"
				and datepart(IA_FINISH_DTTM) = &ETL_CURRENT_DT.
				and datepart(SAS_START_DTTM) = &ETL_CURRENT_DT.
				and RESOURCE_NAME = "&lmvIn."
			;
		quit;
		
	%end;
	
	%M_LOG_EVENT(END, load_etl_stg_&mpResource.);
	%etl_job_finish;

%mend load_etl_stg;