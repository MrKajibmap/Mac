%macro fmk_load_etl_ia(mpResource=);

	%local lmvResource
			lmvResId
			lmvResNm
	;
	%let lmvResource = %lowcase(&mpResource.);
	
	%tech_log_event(mpMODE=START, mpPROCESS_NM=fmk_load_etl_ia_&lmvResource.);
	
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	proc sql noprint;
		select 	resource_id
				,resource_nm 
		into 	:lmvResId
				,:lmvResNm
		from etl_cfg.cfg_resource
		where lowcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResNm.) eq 0 %then %do;
		%put ERROR: Invalid input parameters;
		%abort;
	%end;
	
	/* Забираем самую свежую выгрузку */
	proc sql noprint;
		create table work.tmp_&lmvResource._delta as
			select * 
					,'N' as etl_delta_cd length=1
					,(dhms(date(), hour(time()), minute(time()), second(time()))) as valid_from_dttm length=8 format=datetime.
					,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm length=8 format=datetime.
			from etl_stg.stg_&lmvResource.
			where etl_extract_id = select extract_id						
									from etl_cfg.cfg_resource_registry 
									where exec_dttm = select max(exec_dttm) as max 
														from etl_cfg.cfg_resource_registry 
														where resource_id = &lmvResId.
														and status_cd in ('A', 'E')  
	;													
	quit;
	
	/* Подсчет кол-ва строк во входном наборе данных */
	%let lmvCntRowsSource = %member_obs(mpData=work.tmp_&lmvResource._delta);
	
	/* Нет данных новых - выходим */
	%if &lmvCntRowsSource. eq 0 %then %do;
		%put WARNING: There are no data to load (Fresh extract_id does not exist) for resource = &lmvResource. (&lmvResId.) 
		in etl_stg.stg_&lmvResource. . Check out etl_cfg.cfg_resource_registry  for existence of new snapshot.;
		%return;
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
		where lowcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResTypeLoad.) eq 0 %then %do;
		%put ERROR: Invalid value for resource type load were expected;
		%abort;
	%end;
		
	proc sql noprint;
		/* Выявляем ключи/поля для таблицы, необходимые для расчета хеш-сумм */
		connect using etl_ia;
		create table work.cols_&lmvResource. as
		select * from connection to etl_ia 
		(
		select c.column_name, c.data_type, ccu.constraint_name, tc.constraint_type
					 from information_schema.columns as c 
					 left join information_schema.constraint_column_usage as ccu
						on c.table_name = ccu.table_name
							and c.column_name = ccu.column_name
					left join information_schema.table_constraints as tc
						on c.table_name = tc.table_name
						and ccu.constraint_name = tc.constraint_name
					 where c.table_name=%str(%')&lmvResource.%str(%')
					 and c.table_schema='etl_ia'
		)
		;
	quit;
	
	/*Проверка на корректность структуры таблицы */
	%if %member_obs(mpData=work.cols_&lmvResource.) eq 0 %then %do;
		%put ERROR: Invalid structure in table "etl_ia.&lmvResource." (&lmvResId.).;
		%abort;
	%end;

	proc sql noprint;
		select column_name into :lmvPkList separated by ","
		from work.cols_&lmvResource.
		where constraint_type = 'PRIMARY KEY'
			and column_name not in ('valid_from_dttm', 'valid_to_dttm')
		;
		select column_name into :lmvColumnsList separated by ","
		from work.cols_&lmvResource.
		where constraint_type not eq 'PRIMARY KEY'
			and column_name not in ('valid_from_dttm', 'valid_to_dttm')
		;
	quit;
	%put &=lmvPkList &=lmvColumnsList;
	
	/* Забираем окно из главной таблицы со старыми данными и расчитываем хеши (по фактам и по ключам)*/
	data work.tmp_&lmvResource._snap_hsh;
		length pk_hash value_hash $32;
		format  pk_hash value_hash $hex32.;
		%if %upcase(&lmvResTypeLoad.) eq FULL %then %do;
			set etl_ia.&lmvResource.(where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
		%end;
		%else %if %upcase(&lmvResTypeLoad.) eq WINDOW %then %do;
			set etl_ia.&lmvResource.(where=(&lmvFieldTmFrame. >= %syseval(&lmvFieldTmFrame.-&lmvTmFrameVal.)) and (valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
		%end;
		pk_hash = SHA256HEX(catx('_', &lmvPkList.));
		value_hash= SHA256HEX(catx('_', &lmvColumnsList.));
	run;
	
	/*Расчитываем хеш для новой пачки данных */
	data work.tmp_&lmvResource._delta_hsh;
		length pk_hash value_hash $32;
		format pk_hash value_hash $hex32.;
		set work.tmp_&lmvResource._delta; 
		pk_hash = SHA256HEX(catx('_', &lmvPkList.));
		value_hash= SHA256HEX(catx('_', &lmvColumnsList.));
	run;
	
	proc sql noprint;
		create table work.&lmvResource._hash_cfg as
			select distinct n.pk_hash, n.value_hash, o.valid_from_dttm,
					case 
						when n.value_hash = o.value_hash
						then 2 /*same values*/
						when n.value_hash ne o.value_hash and o.value_hash ne ' '
						then 3 /*diff values*/
						else 1 /*new*/
					end as flag 
			from work.tmp_&lmvResource._snap_hsh n
				left join work.tmp_&lmvResource._delta_hsh o 
					on n.pk_hash = o.pk_hash
					and o.valid_to_dttm = &ETL_SCD_FUTURE_DTTM.
					/* and o.valid_to_dttm = &ETL_SCD_FUTURE_DTTM_DB.*/
		;
		/*extract delta*/
		create table work.&lmvResource._chkd_dlt as
			select distinct mn.*
			from work.tmp_&lmvResource._snap_hsh mn
			left join work.&lmvResource._hash_cfg h
				on mn.pk_hash =h.pk_hash
			where h.flag in (1,3)
		;
		/*extract diffs (snup)*/
		create table work.&lmvResource._chkd_snup as
			select distinct mn.*
			from work.tmp_&lmvResource._snap_hsh mn
			left join work.&lmvResource._hash_cfg h
				on mn.pk_hash =h.pk_hash
			where h.flag = 3 
		;
	quit;
	
	/*create table for dcl hash*/
	data work.&lmvResource._hash_diffs (drop=flag valid_from_dttm pk_hash);
		length hash_value_dttm_id $32;
		set work.&lmvResource._hash_cfg(where=(FLAG=3) keep=flag valid_from_dttm pk_hash);
		hash_value_dttm_id = SHA256HEX(catx('_',pk_hash,valid_from_dttm));
	run;

	/* update with hash */
	/*data etl_ia.&lmvResource.(drop=rc hash_value_dttm_id);
		length hash_value_dttm_id $32;
		if _n_=1 then do;
			DECLARE HASH H (DATASET:"work.&lmvResource._hash_diffs");
			RC=H.DEFINEKEY("hash_value_dttm_id");
			RC=H.DEFINEDATA();
			RC=H.DEFINEDONE();
			call missing(hash_value_dttm_id);
		end;
		set work.tmp_&lmvResource._delta_hsh;
		hash_value_dttm_id = SHA256HEX(catx('_',pk_hash,valid_from_dttm));
		rc=h.find();
		if (rc=0) then do;
			valid_to_dttm = &ETL_SCD_FUTURE_DTTM.;
		end;
	run;*/

	data work.&lmvResource.(drop=rc hash_value_dttm_id);
		length hash_value_dttm_id $32 pk_hash $32;;
		if _n_=1 then do;
			DECLARE HASH H (DATASET:"work.&lmvResource._hash_diffs");
			RC=H.DEFINEKEY("hash_value_dttm_id");
			RC=H.DEFINEDATA();
			RC=H.DEFINEDONE();
			call missing(hash_value_dttm_id);
		end;
		set etl_ia.&lmvResource.;
		pk_hash = SHA256HEX(catx('_', &lmvPkList.));
		hash_value_dttm_id = SHA256HEX(catx('_',pk_hash,valid_from_dttm));
		rc=h.find();
		if (rc=0) then do;
			valid_to_dttm = &ETL_SCD_FUTURE_DTTM.;
		end;
	run;

	proc sql noprint;
		connect using etl_ia;
		execute by etl_ia (
			truncate etl_ia.&lmvResource.;
		);
	quit;
	
	/*Загружаем данные в артефакт */
	proc append base=etl_ia.&lmvResource.(&ETL_PG_BULKLOAD.) data=work.&lmvResource. force;
	run;
	
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		proc sql noprint;
			connect using etl_cfg;
			execute by etl_cfg(
				update etl_cfg.cfg_resource_registry
				set status_cd='E'
				where resource_id = &lmvResId. and status_cd in ('A') and uploaded_from_source is null and uploaded_to_target is null;
			);
		quit;
		
		%put ERROR: &lmvResource. was uploaded unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	/*Загружаем дельту */
	%cmn_load_etl_ia_artefacts(mpMode=delta
								,mpTargetTableNm=&lmvResource.
								,mpInputTableNm=work.&lmvResource._chkd_dlt
								);
	/* Загружаем snup */

	%cmn_load_etl_ia_artefacts(mpMode=snup
								,mpTargetTableNm=&lmvResource.
								,mpInputTableNm=work.&lmvResource._chkd_snup
								);
	/* Загружаем snap */
	%cmn_load_etl_ia_artefacts(mpMode=snup
								,mpTargetTableNm=&lmvResource.
								,mpInputTableNm= work.tmp_&lmvResource._snap_hsh
								);
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		proc sql noprint;
			connect using etl_cfg;
			execute by etl_cfg(
				update etl_cfg.cfg_resource_registry
				set status_cd='E'
				where resource_id = &lmvResId. and status_cd in ('A') and uploaded_from_source is null and uploaded_to_target is null;
			);
		quit;
		
		%put ERROR: &lmvResource. was uploaded unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	/* Добавляем дельту к главной таблице*/
	proc append base=etl_ia.&lmvResource. (&ETL_PG_BULKLOAD.) data=work.&lmvResource._chkd_dlt force;
	run;
	
	/* Calc uploaded/updated rows */
	%let lmvCntRowsTarget = %eval( %member_obs(mpData=work.tmp_&lmvResource._delta) + %member_obs(mpData=work.&lmvResource._hash_diffs) );
	%let lmvCntUpdatedRows = %member_obs(mpData=work.&lmvResource._chkd_snup);
	
	%put Count of updated rows in table &lmvResource. = &lmvCntUpdatedRows. and count of uploaded rows  = &lmvCntRowsTarget.;
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		proc sql noprint;
			connect using etl_cfg;
			execute by etl_cfg(
				update etl_cfg.cfg_resource_registry
				set status_cd='E'
				where resource_id = &lmvResId. and status_cd in ('A') and uploaded_from_source is null and uploaded_to_target is null;
			);
		quit;
		
		%put ERROR: &lmvResource. was uploaded unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	proc sql noprint;
		connect using etl_cfg;
		execute by etl_cfg(
		update etl_cfg.cfg_resource_registry 
		set status_cd = 'L'
			,uploaded_from_source=&lmvCntRowsSource.
			,uploaded_to_target=&lmvCntRowsTarget.		
		where exec_dttm = (select max(exec_dttm) as max
					from etl_cfg.cfg_resource_registry where resource_id = &lmvResId. and status_cd in ('A'))
				and resource_id = &lmvResId. and status_cd in ('A');
		)
	;
	quit;
	
	%tech_log_event(mpMODE=END, mpPROCESS_NM=fmk_load_etl_ia_&lmvResource.);
	
%mend fmk_load_etl_ia;