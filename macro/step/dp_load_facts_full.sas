%macro dp_load_facts_full(mpFactGcMnth = dm_abt.fact_gc_month,
					mpFactPmixMnth = dm_abt.fact_pmix_month,
					mpFactUptMnth = dm_abt.fact_upt_month,
					mpPath = /data/dm_rep/
					);

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

	proc casutil;
		promote casdata="&lmvTabNmGc." incaslib="casuser" outcaslib="&lmvLibrefGc.";
	quit;


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
		
	proc casutil;
			promote casdata="&lmvTabNmPmix." incaslib="casuser" outcaslib="&lmvLibrefPmix.";
	quit;

	/* LOAD fact_upt_month */
	proc casutil;
		droptable casdata="&lmvTabNmUpt." incaslib="&lmvLibrefUpt." quiet;
	run;
	proc fedsql sessref=casauto; 
		Create table casuser.fact_upt_month{options replace=true} as
		Select distinct t1.PROD, t1.LOCATION, t1.DATA, 'RUR' as CURRENCY, 'CORP' as ORG, (FACT_QNT_MONTH/FACT_GC_MONTH*1000) as FACT_UPT
		From &lmvLibrefPmix..&lmvTabNmPmix. t1
		Inner join &lmvLibrefGc..&lmvTabNmGc. t2 on t1.LOCATION=t2.LOCATION and t1.DATA=t2.DATA;
	quit;

	data casuser.&lmvTabNmUpt.(replace=yes);
		set casuser.fact_upt_month;
		format data yymon7. LOCATION PROD 8.;
	run;

	proc casutil;
		promote casdata="&lmvTabNmUpt." incaslib="casuser" outcaslib="&lmvLibrefUpt.";
	quit;
	
	%dp_export_csv(mpInput= &lmvLibrefGc..&lmvTabNmGc.
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	%dp_export_csv(mpInput= &lmvLibrefPmix..&lmvTabNmPmix.
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);
	%dp_export_csv(mpInput= &lmvLibrefUpt..&lmvTabNmUpt.
					, mpTHREAD_CNT=30
					, mpPath=&mpPath.);

%mend dp_load_facts_full;