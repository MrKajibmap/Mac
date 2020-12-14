/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     получение иерархии в формате PT
*
*  ПАРАМЕТРЫ:
*     mpLvl				+  максимальный уровень
*     mpIn				+  входная таблица
*     
*
******************************************************************
*  Использует:
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. входная таблица (mpIn) должна содержать поля с ID, PARENT_ID и LVL (по смыслу и в названии)
*	  2. поля с ID, PARENT_ID и LVL - числовые 
*
******************************************************************
*  Пример использования:
*     %hier_pt(mpLvl=5, mpIn=work.IA_PRODUCT_HIERARCHY);
*
******************************************************************
*  21-04-2020  Зотиков     Начальное кодирование
*  25-08-2020  Борзунов	   Добавлена обработка для данных из ETL_IA
******************************************************************/
%macro hier_pt(mpLvl=, mpIn=, mpOut=);

	%macro dum; %mend dum;

	%let mvLib = %sysfunc(scan(&mpIn.,1,"."));
	%let mvTbl = %sysfunc(scan(&mpIn.,2,"."));
	%if %sysfunc(upcase(&mvLib.)) = ETL_IA %then %do;
		data etl_ia_&mvTbl.;
			set &mpIn.(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;
	%end;
	/*
	%if %sysfunc(count(&mvTbl.,PRODUCT)) > 0 %then %do; 
		%let mvOutClm = product;
	%end;
	%if %sysfunc(count(&mvTbl.,SEGMENT)) > 0 %then %do; 
		%let mvOutClm = segment;
	%end;
	%if %sysfunc(count(&mvTbl.,CHANNEL)) > 0 %then %do; 
		%let mvOutClm = channel;
	%end;
	%if %sysfunc(count(&mvTbl.,INT_ORG)) > 0 or %sysfunc(count(&mvTbl.,LOCATION)) > 0 or %sysfunc(count(&mvTbl.,PBO)) > 0 %then %do; 
		%let mvOutClm = int_org;
	%end;
	*/
	%let mvOutClm = member;

	proc sql;
		create table clmns as 
		select *
		from sashelp.vcolumn 
		where libname = %upcase("&mvLib.")
		and memname = %upcase("&mvTbl.")
		;
	quit;

	/*запись в переменные названия интересующих полей*/
	proc sql;
		select name into :mClmvId
		from clmns
		where upcase(name) contains "ID"
		and upcase(name) not contains "PARENT"
		and upcase(name) not contains "EXTRACT_ID"
		;
	quit;

	proc sql;
		select name into :mvClmParentId
		from clmns
		where upcase(name) contains "PARENT"
		;
	quit;

	proc sql;
		select name into :mvClmLvl
		from clmns
		where upcase(name) contains "LVL"
		;
	quit;

	%let mClmvId=%trim(&mClmvId);
	%let mvClmLvl=%trim(&mvClmLvl);
	%let mvClmParentId=%trim(&mvClmParentId);

	/*сбор каждого уровня в отдельную таблицу*/
	%do i=1 %to &mpLvl.;

		proc sql;
			create table lvl&i. as
			select &mClmvId., &mvClmLvl., &mvClmParentId.
			%if %sysfunc(upcase(&mvLib.)) = ETL_IA %then %do;
				from etl_ia_&mvTbl.
			%end;
			%else %do;
				from &mpIn.
			%end;
			where &mvClmLvl. = &i;
			;
		quit;

	%end;

	/*связь самого нижнего уровня с остальными*/
	proc sql;
		create table lvl_all as
		select %do i=&mpLvl. %to 1 %by -1;
			   	t&i..&mClmvId. as &&mClmvId._&i., t&i..&mvClmLvl. as &&mvClmLvl._&i. %if &i. ne 1 %then %do; , %end;
			   %end;
		from lvl&mpLvl. t&mpLvl. 
		%do i=&mpLvl. %to 2 %by -1;
			%let j = %sysevalf(&i.-1);
			inner join lvl&j. t&j. on t&i..&mvClmParentId. = t&j..&mClmvId.
		%end;
		order by %do i=&mpLvl. %to 2 %by -1; 
				 	t&i..&mClmvId. %if &i. ne 2 %then %do; , %end;
				 %end;
		;
	quit;

	/*связь каждого увроня с каждым*/
	%do i=&mpLvl. %to 1 %by -1;

		/*свзязь уровня с самим собой*/
		proc sql;
			create table lvl&i._&i. as 
			select distinct &&mClmvId._&i. as prnt_&mvOutClm._rk, &&mClmvId._&i. as &mvOutClm._rk, 0 as btwn_lvl_cnt, 
			case 
				when &&mvClmLvl._&i. = &mpLvl. 
				then 'Y'
				else 'N'
			end as is_bottom_flg,
			case 
				when &&mvClmLvl._&i. = 1 
				then 'Y'
				else 'N'
			end as is_top_flg
			from lvl_all
			;
		quit;

		/*свзязь уровня с уровнями выше*/
		%do j=&i.-1 %to 1 %by -1; 

			proc sql;
				create table lvl&i._&j. as 
				select distinct &&mClmvId._&j. as prnt_&mvOutClm._rk, &&mClmvId._&i. as &mvOutClm._rk, &&mvClmLvl._&i.-&&mvClmLvl._&j. as btwn_lvl_cnt, 
				case 
					when &&mvClmLvl._&i. = &mpLvl. 
					then 'Y'
					else 'N'
				end as is_bottom_flg,
				case 
					when &&mvClmLvl._&i. = 1 
					then 'Y'
					else 'N'
				end as is_top_flg
				from lvl_all
				;
			quit;

		%end;

	%end;

	data all_no;
	set %do i=&mpLvl. %to 1 %by -1; 
			%do j=&i. %to 1 %by -1;
				lvl&i._&j.  
			%end;
		%end;
		;
	run;

	proc sql;
		create table &mpOut. as
		select *
		from all_no
		order by 3, 2, 1
		;
	quit;

%mend hier_pt;