%macro dp_load_facts(mpFactGcMnth = dm_abt.fact_gc_month,
					mpFactPmixMnth = dm_abt.fact_pmix_month,
					mpFactUptMnth = dm_abt.fact_upt_month,
					mpPath = /data/dm_rep/
					);

	%M_ETL_REDIRECT_LOG(START, load_dp_facts, Main);
	%M_LOG_EVENT(START, load_dp_facts);
	
	%local lmvLibrefGc 
			lmvTabNmGc 
			lmvLibrefPmix
			lmvTabNmPmix
			lmvLibrefUpt
			lmvTabNmUpt
			;
	%member_names(mpTable=&mpFactGcMnth, mpLibrefNameKey=lmvLibrefGc, mpMemberNameKey=lmvTabNmGc);
	%member_names(mpTable=&mpFactPmixMnth, mpLibrefNameKey=lmvLibrefPmix, mpMemberNameKey=lmvTabNmPmix);
	%member_names(mpTable=&mpFactUptMnth, mpLibrefNameKey=lmvLibrefUpt, mpMemberNameKey=lmvTabNmUpt);

	cas casauto sessopts=(metrics=true);
	caslib _all_ assign;

	%let lmvInLib=ETL_IA;
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let BeginOfYear = %sysfunc(intnx(year,&etl_current_dt.,0,b));
	%put &=BeginOfYear;

	proc casutil;
		droptable casdata="&lmvTabNmGc." incaslib="&lmvLibrefGc." quiet;
	run;

	/* LOAD fact_gc_month */
	data CASUSER.pbo_sales (replace=yes);
		set &lmvInLib..pbo_sales(where=( (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.) and sales_dt>=&BeginOfYear.));
	run;

	proc fedsql sessref=casauto; 
		create table casuser.PBO_SALES_day{options replace=true} as 
		select distinct pbo_location_id, sales_dt,
		receipt_qty
		from CASUSER.pbo_sales
	;
	quit;

	proc fedsql sessref=casauto; 
		create table casuser.PBO_sales_prep_m{options replace=true} as
		select distinct *, intnx('month', sales_dt, 0, 'b') as month
		from casuser.PBO_SALES_day;
	quit;

	proc fedsql sessref=casauto; 
		create table casuser.fact_gc_month{options replace=true} as
		select distinct '1' as PROD, pbo_location_id as LOCATION, month as DATA, 'RUR' as CURRENCY, 'CORP' as ORG, sum(receipt_qty) as FACT_GC_MONTH
		from casuser.PBO_sales_prep_m
		group by pbo_location_id, month;
	quit;
	/* сброс форматов */
	data casuser.&lmvTabNmGc.(replace=yes);
		set casuser.fact_gc_month ;
		format data yymon7. LOCATION PROD 8.;
	;
	run;

	/* LOAD fact_pmix_month */
	proc casutil;
		droptable casdata="&lmvTabNmPmix." incaslib="&lmvLibrefPmix." quiet;
	run;

	data CASUSER.pmix_sales (replace=yes);
		set &lmvInLib..pmix_sales(where=( (valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.) and sales_dt>=&BeginOfYear.));
	run;
		
	proc fedsql sessref=casauto; 
		create table casuser.sales_prep_m{options replace=true} as
		select distinct product_id, 
		 pbo_location_id,
		 sales_dt,
		sales_qty,
		intnx('month', sales_dt, 0, 'b') as month
		 from CASUSER.pmix_sales
		 ;
	 quit;

	proc fedsql sessref=casauto; 
		create table casuser.fact_pmix_month{options replace=true} as
			select product_id as PROD, pbo_location_id as LOCATION, month as DATA, 'RUR' as CURRENCY, 'CORP' as ORG, sum(sales_qty) as FACT_QNT_MONTH
		from casuser.sales_prep_m
		group by product_id, pbo_location_id, month;
	quit;
	/* Сброс форматов */
	data casuser.&lmvTabNmPmix.(replace=yes);
		set casuser.fact_pmix_month;
		format data yymon7. LOCATION PROD 8.;
	;
	run;

	/* LOAD fact_upt_month */
	proc fedsql sessref=casauto; 
		Create table casuser.fact_upt_month{options replace=true} as
		Select distinct t1.PROD, t1.LOCATION, t1.DATA, 'RUR' as CURRENCY, 'CORP' as ORG, (FACT_QNT_MONTH/FACT_GC_MONTH*1000) as FACT_UPT
		From casuser.&lmvTabNmPmix. t1
		Inner join casuser.&lmvTabNmGc. t2 on t1.LOCATION=t2.LOCATION and t1.DATA=t2.DATA;
	quit;

	data casuser.&lmvTabNmUpt.(replace=yes);
		set casuser.fact_upt_month;
		format data yymon7. LOCATION PROD 8.;
	run;

	%load_komp_matrix;
	/* non-komp*/
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmGc._nonkomp{options replace=true} as
		select t1.* 
		from casuser.&lmvTabNmGc. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmGc._nonkomp" incaslib="&lmvLibrefGc." quiet;
		promote casdata="&lmvTabNmGc._nonkomp" incaslib="casuser" outcaslib="&lmvLibrefGc.";
	quit;
	
	%dp_export_csv(mpInput=&lmvLibrefGc..&lmvTabNmGc._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	/*			
	proc casutil;
		droptable casdata="&lmvTabNmGc._nonkomp" incaslib="&lmvLibrefGc." quiet;
	quit;
	*/
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmPmix._nonkomp{options replace=true} as
		select t1.* 
		from casuser.&lmvTabNmPmix. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmPmix._nonkomp" incaslib="&lmvLibrefPmix." quiet;
		promote casdata="&lmvTabNmPmix._nonkomp" incaslib="casuser" outcaslib="&lmvLibrefPmix.";
	quit;
	
	%dp_export_csv(mpInput=&lmvLibrefPmix..&lmvTabNmPmix._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	/*				
	proc casutil;
		droptable casdata="&lmvTabNmPmix._nonkomp" incaslib="&lmvLibrefPmix." quiet;
	quit;
	*/
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmUpt._nonkomp{options replace=true} as
		select t1.* 
		from casuser.&lmvTabNmUpt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmUpt._nonkomp" incaslib="&lmvLibrefUpt." quiet;
		promote casdata="&lmvTabNmUpt._nonkomp" incaslib="casuser" outcaslib="&lmvLibrefUpt.";
	quit;
	
	%dp_export_csv(mpInput=&lmvLibrefUpt..&lmvTabNmUpt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	/*				
	proc casutil;
		droptable casdata="&lmvTabNmUpt._nonkomp" incaslib="&lmvLibrefUpt." quiet;
	quit;
	*/
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmGc._komp{options replace=true} as
		select t1.* 
		from casuser.&lmvTabNmGc. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;

	proc casutil;
		droptable casdata="&lmvTabNmGc._komp" incaslib="&lmvLibrefGc." quiet;
		promote casdata="&lmvTabNmGc._komp" incaslib="casuser" outcaslib="&lmvLibrefGc.";
	quit;
	
	%dp_export_csv(mpInput=&lmvLibrefGc..&lmvTabNmGc._komp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	/*				
	proc casutil;
		droptable casdata="&lmvTabNmGc._komp" incaslib="&lmvLibrefGc." quiet;
	quit;
	*/

	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmPmix._komp{options replace=true} as
		select t1.* 
		from casuser.&lmvTabNmPmix. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmPmix._komp" incaslib="&lmvLibrefPmix." quiet;
		promote casdata="&lmvTabNmPmix._komp" incaslib="casuser" outcaslib="&lmvLibrefPmix.";
	quit;
	
	%dp_export_csv(mpInput=&lmvLibrefPmix..&lmvTabNmPmix._komp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	/*				
	proc casutil;
		droptable casdata="&lmvTabNmPmix._komp" incaslib="&lmvLibrefPmix." quiet;
	quit;
	*/
	
	proc fedsql sessref=casauto;
		create table casuser.&lmvTabNmUpt._komp{options replace=true} as
		select t1.* 
		from casuser.&lmvTabNmUpt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc casutil;
		droptable casdata="&lmvTabNmUpt._komp" incaslib="&lmvLibrefUpt." quiet;
		promote casdata="&lmvTabNmUpt._komp" incaslib="casuser" outcaslib="&lmvLibrefUpt.";
	quit;
	
	%dp_export_csv(mpInput=&lmvLibrefUpt..&lmvTabNmUpt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	/*				
	proc casutil;
		droptable casdata="&lmvTabNmUpt._komp" incaslib="&lmvLibrefUpt." quiet;
	quit;
	*/
	%M_ETL_REDIRECT_LOG(END, load_dp_facts, Main);
	%M_LOG_EVENT(END, load_dp_facts);
	
%mend dp_load_facts;