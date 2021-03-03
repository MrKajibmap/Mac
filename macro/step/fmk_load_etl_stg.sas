%macro fmk_load_etl_stg(mpResource=);
	
	%local 
			lmvResource
			lmvResId
			lmvResNm
			lmvResTypeLoad
			lmvFieldTmFrame
			lmvTmFrameVal
			lmvOutDtVarsCnt
			lmvOutDtVarsKeep
			lmvVersionId
			lmvEventId
			;
	%let lmvResource = %upcase(&mpResource.);
	/* add row in config status table */
	%tech_log_event(mpMODE=START, mpPROCESS_NM=fmk_load_etl_stg_&lmvResource.);
	
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	proc sql noprint;
		select 	resource_id
				,resource_nm 
		into 	:lmvResId
				,:lmvResNm
		from etl_cfg.cfg_resource
		where upcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResNm.) eq 0 %then %do;
		%put ERROR: Invalid input parameters;
		%abort;
	%end;
	
	/* Определение типа загрузки для ресурса */
	proc sql noprint;
		select 	resource_type_load
				,field_time_frame
				,time_frame_value 
		into 	:lmvResTypeLoad
				,:lmvFieldTmFrame
				,:lmvTmFrameVal
		from etl_cfg.cfg_resource_type_load
		where upcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResTypeLoad.) eq 0 %then %do;
		%put ERROR: Invalid value for resource type load were expected;
		%abort;
	%end;
	
	%let lmvKeep = %member_vars (etl_stg.stg_&lmvResource.);
	%let lmvKeepComma = %member_vars (etl_stg.stg_&lmvResource., mpDlm=%str(, ));

	proc sql noprint;
		create table clms as
		select *
		from sashelp.vcolumn 
		where upcase(libname) = 'ETL_STG' 
			and upcase(memname) = "STG_&lmvResource."
			and format = 'DATE9.'
		;
	quit;

	%let lmvOutDtVarsCnt = %member_obs (mpData=work.clms);

	%if &lmvOutDtVarsCnt. gt 0 %then %do;
		proc sql noprint;
			select name into :lmvOutDtVarsKeep separated by ' '
			from work.clms
			;
			select name into :lmvOutDtVarsNm1 %if &lmvOutDtVarsCnt. gt 1 %then %do; - :lmvOutDtVarsNm&lmvOutDtVarsCnt. %end; 
			from work.clms
			;
		quit;
	%end;
	
	proc sql noprint;
		connect using etl_cfg;
		 execute by etl_cfg(
			update etl_cfg.cfg_resource_registry
			set status_cd = 'C' where resource_id = &lmvResId. and status_cd in ('A') 
			;
			);
		execute by etl_cfg(
			insert into etl_cfg.cfg_resource_registry(
				event_id, resource_id, process_nm, extract_id, status_cd, exec_dttm, uploaded_from_source, uploaded_to_target)
				VALUES (DEFAULT, &lmvResId., %str(%')load_etl_stg_&lmvResource.%str(%'), DEFAULT, 'P', current_timestamp, null, null)
			;
			);
		disconnect from etl_cfg;
	quit;
	
	proc sql noprint;
		select extract_id into :lmvVersionId
		from etl_cfg.cfg_resource_registry
		where resource_id = &lmvResId.
			and exec_dttm = (select max(exec_dttm) as max_dttm 
							from etl_cfg.cfg_resource_registry
							where resource_id = &lmvResId.
							and status_cd = 'P')
	;
	quit;
	
	%put &=lmvVersionId;
	
	
	data work.&lmvResource.(keep=&lmvKeep.);
	%if &lmvOutDtVarsCnt. gt 0 %then %do;
			format &lmvOutDtVarsKeep. date9.;
	%end;
	
	%if %upcase(&lmvResTypeLoad.) eq FULL %then %do;
		set IA.IA_&lmvResource.;
	%end;
	%else %if %upcase(&lmvResTypeLoad.) eq WINDOW %then %do;
		set IA.IA_&lmvResource.(where=(&lmvFieldTmFrame.>=intnx('day',&lmvFieldTmFrame., -&lmvTmFrameVal.,'s')));
	%end;
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
		etl_extract_id = &lmvVersionId.;
	run;
	
	/* load data to target table */
 	proc append base=ETL_STG.stg_&lmvResource.(&ETL_PG_BULKLOAD.) data=work.&lmvResource. force;
    run;
	
	%put &=SYSCC;

	/*** Update etl_cfg.cfg_resource_registry ***/
	/* Extract count(*) from source/target tables */
	%let lmvCntRowsTarget = %member_obs(mpData=work.&lmvResource.);
	
	%if &SYSCC le 4 %then %do;
		proc sql noprint;
			connect using etl_cfg;
			execute by etl_cfg(
				update etl_cfg.cfg_resource_registry
				set status_cd='A'
					,uploaded_from_source=&lmvCntRowsTarget.
				where resource_id = &lmvResId. and status_cd in ('P') and uploaded_from_source is null and uploaded_to_target is null;
			);
		quit;
		
		%put NOTE: &lmvResource. was uploaded successfully!;
	%end;
	%else %if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		
		proc sql noprint;
			connect using etl_cfg;
			execute by etl_cfg(
				update etl_cfg.cfg_resource_registry
				set uploaded_from_source=&lmvCntRowsTarget.
					,status_cd='E'
				where resource_id = &lmvResId. and status_cd in ('A') and uploaded_from_source is null and uploaded_to_target is null;
			);
		quit;
		
		%put ERROR: &lmvResource. was uploaded unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		/* Return session in noexecution mode */
		OPTIONS SYNTAXCHECK OBS=0;
	%end;
	
	/* add row in config status table */
	%tech_log_event(mpMODE=END, mpPROCESS_NM=fmk_load_etl_stg_&lmvResource.);

%mend fmk_load_etl_stg;