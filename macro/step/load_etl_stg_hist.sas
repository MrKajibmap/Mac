/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос загрузки исторических данных в ETL_STG, регистрирует выгрузку в реестре.
*
*  ПАРАМЕТРЫ:
*     mpResource                 -  имя загружаемого ресурса
*
******************************************************************
*  Использует:
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %load_etl_stg_hist(mpResource = price);
*
****************************************************************************
*  15-04-2020  Зотиков     Начальное кодирование
****************************************************************************/
%macro load_etl_stg_hist(
					mpResource=);

	%let mvIn = %upcase(%trim(IA_&mpResource.)_HISTORY);
	%let mvOut = %upcase(%trim(STG_&mpResource.)_HISTORY);
	%let mvKeep = %member_vars (etl_stg.&mvOut.);
	%let mvKeepComma = %member_vars (etl_stg.&mvOut., mpDlm=%str(, ));

	proc sql;
		create table clms as
		select *
		from sashelp.vcolumn 
		where libname = 'ETL_STG' and memname = "&mvOut." and format = 'DATE9.'
		;
	quit;

	%let mvOutDtVarsCnt = %member_obs (mpData=work.clms);

	%if &mvOutDtVarsCnt. gt 0 %then %do;
		proc sql;
			select name into :mvOutDtVarsKeep separated by ' '
			from work.clms
			;
		quit;
	
		
		proc sql;
			select name into :mvOutDtVarsNm1 %if &mvOutDtVarsCnt. gt 1 %then %do; - :mvOutDtVarsNm&mvOutDtVarsCnt. %end; 
			from work.clms
			;
		quit;

	%end;

	data work.&mpResource.(keep=&mvKeep.);
		%if &mvOutDtVarsCnt. gt 0 %then %do;
			format &mvOutDtVarsKeep. date9.;
		%end;
		set IA.&mvIn.;
		%if &mvOutDtVarsCnt. gt 0 %then %do;
			%do i=1 %to &mvOutDtVarsCnt.;
				&&mvOutDtVarsNm&i.. = datepart(&&mvOutDtVarsNm&i..);
			%end;
		%end;
	run; 

	proc sql;
		insert into etl_stg.&mvOut. 
		select &mvKeepComma. from work.&mpResource.
		;
	quit;

%mend load_etl_stg_hist;