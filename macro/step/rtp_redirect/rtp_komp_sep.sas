%macro rtp_komp_sep(mpVfPmixProjName=pmix_sales_v1,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=dm_abt.pmix_reconciled_full,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=dm_abt.plan_pmix_month,
							mpOutGcLt=dm_abt.plan_gc_month, 
							mpOutUptLt=dm_abt.plan_upt_month, 
							mpOutPmixSt=dm_abt.plan_pmix_day,
							mpOutGcSt=dm_abt.plan_gc_day, 
							mpOutUptSt=dm_abt.plan_upt_day, 
							mpOutOutforgc=dm_abt.TS_OUTFORGC,
							mpOutOutfor=dm_abt.TS_OUTFOR, 
							mpOutNnetWp=public.nnet_wp1,
							mpPrmt=N) ;
							
							
							
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%local	lmvOutLibrefPmixSt 
			lmvOutTabNamePmixSt 
			lmvOutLibrefGcSt 
			lmvOutTabNameGcSt 
			lmvOutLibrefUptSt 
			lmvOutTabNameUptSt 
			lmvOutLibrefPmixLt 
			lmvOutTabNamePmixLt 
			lmvOutLibrefGcLt 
			lmvOutTabNameGcLt
			lmvOutLibrefUptLt 
			lmvOutTabNameUptLt  
			lmvOutLibrefOutforgc 
			lmvOutTabNameOutforgc 
			lmvOutLibrefOutfor 
			lmvOutTabNameOutfor 
			lmvVfPmixName
			lmvVfPmixId
			lmvVfPboName
			lmvVfPboId
			lmvInEventsMkup
			;
			
	%let lmvInLib=ETL_IA;
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let mvOutpath=/data/dm_rep/;
	
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpOutGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpOutPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpOutUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
	%member_names (mpTable=&mpOutGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpOutPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpOutUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
		
	/* Создание разметки по комп-некомп ресторанам */
	%load_komp_matrix;
	/* non-komp*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;

	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
		promote casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
	quit;

	%dp_export_csv(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;

	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;

	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;



	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;

	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;



	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;

	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;


	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;

	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
		%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;



	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;	
		
	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;	
	
		
	/* komp*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;

	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp;
		format DATA yymon7.;
	run;

	proc casutil;
			promote casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;
	
		

	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp;
		format DATA yymon7.;
	run;
	proc casutil;
		promote casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;


	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;

	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp;
		format DATA yymon7.;
	run;
	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;


	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	proc casutil;
		promote casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;


	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;



	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;	

	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;

%mend rtp_komp_sep;