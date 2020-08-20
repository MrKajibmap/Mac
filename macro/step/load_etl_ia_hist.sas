/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки исторических данных в ETL_IA (включая DELTA, SNUP, SNAP)
*
*  ПАРАМЕТРЫ:
*     mvResource                 -  имя загружаемого ресурса
*
******************************************************************
*  Использует:
*			%etl_job_start
*			%etl_archive_get
*			%postgres_get_pk
*			%etl_get_delta_scd
*			%etl_transaction_start
*			%etl_update_dds
*			%etl_transaction_finish
*			%etl_job_finish
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %load_etl_ia_hist(media);
*
****************************************************************************
*  14-04-2020  Зотиков     Начальное кодирование
*  19-08-2020  Борзунов	   Параметр mpFieldTimeFrameDttm изменен на mpFieldTimeFrameDt 
****************************************************************************/
%macro load_etl_ia_hist(
					mpResource=);

	%let etls_jobName=load_etl_ia_hist;
	%etl_job_start;
	options sastrace=',,,dbs' sastraceloc=saslog nostsuffix;
	
	%let mvResource = %trim(&mpResource.);
	
	/*%etl_archive_get(mpInArchive=etl_stg.stg_&mvResource._HISTORY, mpOutData=work.tmp_&mvResource., mpFullStage=No);*/
	
	data work.tmp_&mvResource.;
		set etl_stg.stg_&mvResource._HISTORY;
		format VALID_FROM_DTTM VALID_TO_DTTM datetime25.6;
		VALID_FROM_DTTM = &JOB_START_DTTM;
		VALID_TO_DTTM = &ETL_SCD_FUTURE_DTTM.;
	run;
	
	%postgres_get_pk(mpSchema=ETL_IA, mpTable=&mvResource.);
	
	proc sql;
		select column_name into :lmvPK separated by ' '
		from PK_&mvResource.
		where lowcase(column_name) not in ("valid_from_dttm",
											"valid_to_dttm"
										)
	
		;
	quit;
	/* mpFieldTimeFrameDttm изменено на mpFieldTimeFrameDt */
	%etl_get_delta_scd(mpIn=work.tmp_&mvResource., mpSnap=etl_ia.&mvResource._snap, mpFieldPK=&lmvPK., mpFieldTimeFrameDt=, mpOut=work.&mvResource._delta, mpSnUp=work.&mvResource._snup);
	
	%let mvKeepDelta = %member_vars (etl_ia.&mvResource._delta, mpDlm=%str(, ));
	%let mvKeepSnup = %member_vars (etl_ia.&mvResource._snup, mpDlm=%str(, ));
	
	
	proc sql;
		connect using etl_ia;
		execute by etl_ia (
			truncate table etl_ia.&mvResource._delta
		);
		insert into etl_ia.&mvResource._delta
		select &mvKeepDelta. from work.&mvResource._delta
		;
	quit;
	
	proc sql;
		connect using etl_ia;
		execute by etl_ia (
			truncate table etl_ia.&mvResource._snup
		);
		insert into etl_ia.&mvResource._snup
		select &mvKeepSnup. from work.&mvResource._snup
		;
	quit;
	
	
	%etl_transaction_start (mpLoginSet=%unquote(ETL_IA));
	
	%etl_update_dds(mpIn=etl_ia.&mvResource._delta, mpFieldsPK=&lmvPK., mpFieldStartDttm=valid_from_dttm, mpFieldEndDttm=valid_to_dttm, mpFieldDelta=etl_delta_cd, mpOut=etl_ia.&mvResource.);
	%etl_update_dds(mpIn=etl_ia.&mvResource._snup, mpFieldsPK=&lmvPK., mpFieldStartDttm=valid_from_dttm, mpFieldEndDttm=valid_to_dttm, mpFieldDelta=etl_delta_cd, mpOut=etl_ia.&mvResource._snap);
	
	%etl_transaction_finish;
	
	%etl_job_finish;
%mend load_etl_ia_hist;
