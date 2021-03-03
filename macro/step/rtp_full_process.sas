%macro rtp_full_process;
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;

	*%M_ETL_REDIRECT_LOG(START, rtp_full_process, Main);

	/*  Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (продукты) 
	*  Пример использования:
	*    %rtp_1_load_data_product(mpMode=S, mpOutScore=casuser.all_ml_scoring);
	*	 %rtp_1_load_data_product(mpMode=T, mpOutTrain=casuser.all_ml_train);
	*	 %rtp_1_load_data_product(mpMode=A, mpOutTrain=casuser.all_ml_train, mpOutScore=casuser.all_ml_scoring);
	*  ПАРАМЕТРЫ:
	*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
	*	  mpOutTrain	- выходная таблица набора для обучения
	*	  mpOutScore	- выходная таблица набора для скоринга */
	
	%M_ETL_REDIRECT_LOG(START, rtp_load_data_to_caslib, Main);
	%M_LOG_EVENT(START, rtp_load_data_to_caslib);
		%rtp_load_data_to_caslib(mpWorkCaslib=casshort);
	%M_LOG_EVENT(END, rtp_load_data_to_caslib);
	%M_ETL_REDIRECT_LOG(END, rtp_load_data_to_caslib, Main);
	
	%M_ETL_REDIRECT_LOG(START, rtp_1_load_data_product, Main);
	%M_LOG_EVENT(START, rtp_1_load_data_product);
		%rtp_1_load_data_product(mpMode=A,
					 mpOutTrain=casshort.all_ml_train,
					 mpOutScore=casshort.all_ml_scoring,
					 mpWorkCaslib=casshort);
	%M_LOG_EVENT(END, rtp_1_load_data_product);
	%M_ETL_REDIRECT_LOG(END, rtp_1_load_data_product, Main);

	%symdel mvcnt mvExist;

	proc sql;
		select count(*) as cnt into :mvcnt
		from casshort.all_ml_scoring
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;

	/* Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (мастеркоды)
	*  Пример использования:
	*	%macro rtp_2_load_data_mastercode(mpMode=A,
	*							mpInputTableScore=casuser.all_ml_scoring, 
	*							mpInputTableTrain=casuser.all_ml_train,
	*							mpOutputTableScore = casuser.master_code_score,
	*							mpOutputTableTrain = casuser.master_code_train
	*							);
	*	%macro rtp_2_load_data_mastercode(mpMode=T,
	*							mpInputTableScore=casuser.all_ml_scoring, 
	*							mpInputTableTrain=casuser.all_ml_train,
	*							mpOutputTableTrain = casuser.master_code_train
	*							);
	*	%macro rtp_2_load_data_mastercode(mpMode=S,
	*							mpInputTableScore=casuser.all_ml_scoring, 
	*							mpInputTableTrain=casuser.all_ml_train,
	*							mpOutputTableScore = casuser.master_code_score
	*							);
	*  ПАРАМЕТРЫ:
	*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
	*	  mpOutTrain	- выходная таблица набора для обучения
	*	  mpOutScore	- выходная таблица набора для скоринга */
	/*
	%M_ETL_REDIRECT_LOG(START, rtp_2_load_data_mastercode, Main);
	%M_LOG_EVENT(START, rtp_2_load_data_mastercode);
		%rtp_2_load_data_mastercode( mpMode=A,
							mpInputTableScore=casshort.all_ml_scoring, 
							mpInputTableTrain=casshort.all_ml_train,
							mpOutputTableScore = casshort.master_code_score,
							mpOutputTableTrain = casshort.master_code_train,
							mpWorkCaslib=casshort
							);
	%M_LOG_EVENT(END, rtp_2_load_data_mastercode);	
	%M_ETL_REDIRECT_LOG(END, rtp_2_load_data_mastercode, Main);
	
	%symdel mvcnt mvExist;
	proc sql;

		select count(*) as cnt into :mvcnt
		from casshort.master_code_score
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
	*/
	/* Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (PBO)
	*
	*  ПАРАМЕТРЫ:
	*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
	*	  mpOutTrain	- выходная таблица набора для обучения
	*	  mpOutScore	- выходная таблица набора для скоринга

	*  Пример использования:
	*	%macro rtp_3_load_data_pbo(mpMode=S,
								mpOutTableScore=dm_abt.pbo_score);
	*							);
	*	%macro rtp_3_load_data_pbo(mpMode=T,
	*							mpOutTableTrain=dm_abt.pbo_train);
	*							);
	*	%macro rtp_3_load_data_pbo(mpMode=A,
	*							mpOutTableTrain=dm_abt.pbo_train,
								mpOutTableScore=dm_abt.pbo_score);
	*							); */
	/*
	%M_ETL_REDIRECT_LOG(START, rtp_3_load_data_pbo, Main);
	%M_LOG_EVENT(START, rtp_3_load_data_pbo);
		%rtp_3_load_data_pbo(mpMode=A, 
							mpOutTableTrain=casshort.pbo_train,
							mpOutTableScore=casshort.pbo_score,
						    mpWorkCaslib=casshort);
	%M_LOG_EVENT(END, rtp_3_load_data_pbo);
	%M_ETL_REDIRECT_LOG(END, rtp_3_load_data_pbo, Main);
	
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from casshort.pbo_score
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
	*/
	/*
	%rtp_4_modeling(mode=TRAIN,
						external=1,
						ids = product_id pbo_location_id sales_dt,
						target=sum_qty,
						categories=lvl2_id prod_lvl2_id, 
						external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
						modeltable=PMIX_MODEL_TABLE,				
						traintable=casshort.all_ml_train,
						scoretable=casshort.all_ml_scoring,
						resulttable=casshort.pmix_days_result, 
						default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,				
						default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,				
						default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
					model_prefix=FOREST);	
	*/		
	
	
	%M_ETL_REDIRECT_LOG(START, rtp_4_modeling_pmix, Main);
	%M_LOG_EVENT(START, rtp_4_modeling_PMIX);
		%rtp_4_modeling(mode=SCORE,
					external=1,
					ids = product_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
					modeltable=PMIX_MODEL_TABLE,				
					traintable=casshort.all_ml_train,
					scoretable=casshort.all_ml_scoring,
					resulttable=casshort.pmix_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,				
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,				
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=FOREST);	
	%M_LOG_EVENT(END, rtp_4_modeling_PMIX);	
	%M_ETL_REDIRECT_LOG(END, rtp_4_modeling_pmix, Main);


	/*
	%rtp_4_modeling(mode=TRAIN,
					external=1,
					ids = prod_lvl4_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/MASTER_MODEL_TABLE.csv, 
					modeltable=MASTER_MODEL_TABLE,				
					traintable=casshort.master_code_train,
					scoretable=casshort.master_code_score,
					resulttable=casshort.pmix_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX NUNIQUE_PRODUCT,
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=MASTER_FOREST);
	*/
		
		
		/*
	%M_ETL_REDIRECT_LOG(START, rtp_4_modeling_mc, Main);
	%M_LOG_EVENT(START, rtp_4_modeling_MC);			
		%rtp_4_modeling(mode=SCORE,
						external=1,
						ids = prod_lvl4_id pbo_location_id sales_dt,
						target=sum_qty,
						categories=lvl2_id prod_lvl2_id, 
						external_modeltable=/data/files/input/MASTER_MODEL_TABLE.csv, 
						modeltable=MASTER_MODEL_TABLE,				
						traintable=casshort.master_code_train,
						scoretable=casshort.master_code_score,
						resulttable=casshort.master_code_days_result, 
						default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,
						default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX NUNIQUE_PRODUCT,
						default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
					model_prefix=MASTER_FOREST);
	%M_LOG_EVENT(END, rtp_4_modeling_MC);	
	%M_ETL_REDIRECT_LOG(END, rtp_4_modeling_mc, Main);
	
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from casshort.master_code_days_result
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;

	%M_ETL_REDIRECT_LOG(START, rtp_5_reconcil, Main);
	%M_LOG_EVENT(START, rtp_5_reconcil);	
		%rtp_5_reconcil(mpFSAbt = casshort.pbo_train,
							mpMasterCodeTbl = casshort.MASTER_CODE_DAYS_RESULT,
							mpProductTable = casshort.PMIX_DAYS_RESULT,
							mpResultTable = casshort.PMIX_RECONCILED_FULL
							);
	%M_LOG_EVENT(END, rtp_5_reconcil);		
	%M_ETL_REDIRECT_LOG(END, rtp_5_reconcil, Main);
	
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from casshort.PMIX_RECONCILED_FULL
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;					
	*/
	/* Обратная интеграция + ПЛМ */
	
	
	%M_ETL_REDIRECT_LOG(START, rtp_7_out_integration, Main);
	%M_LOG_EVENT(START, rtp_7_out_integration);	
		%rtp_7_out_integration(mpVfPmixProjName=&VF_PMIX_PROJ_NM.,
									mpVfPboProjName=&VF_PBO_PROJ_NM.,
									mpMLPmixTabName=casshort.pmix_days_result,
									mpInEventsMkup=dm_abt.events_mkup,
									mpInWpGc=dm_abt.wp_gc,
									mpOutPmixLt=mn_short.plan_pmix_month,
									mpOutGcLt=mn_short.plan_gc_month, 
									mpOutUptLt=mn_short.plan_upt_month, 
									mpOutPmixSt=mn_short.plan_pmix_day,
									mpOutGcSt=mn_short.plan_gc_day, 
									mpOutUptSt=mn_short.plan_upt_day, 
									mpOutOutforgc=mn_short.TS_OUTFORGC,
									mpOutOutfor=mn_short.TS_OUTFOR, 
									mpOutNnetWp=public.nnet_wp1,
									mpPrmt=Y,
									mpInLibref=casshort);
	%M_LOG_EVENT(END, rtp_7_out_integration);			
	%M_ETL_REDIRECT_LOG(END, rtp_7_out_integration, Main);
	
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from mn_short.plan_pmix_month
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
	
	
	
	%M_ETL_REDIRECT_LOG(START, rtp_komp_sep, Main);
	%M_LOG_EVENT(START, rtp_komp_sep);
	%rtp_komp_sep(mpInPmixLt=mn_short.plan_pmix_month,
					mpInGcLt=mn_short.plan_gc_month, 
					mpInUptLt=mn_short.plan_upt_month, 
					mpInPmixSt=mn_short.plan_pmix_day,
					mpInGcSt=mn_short.plan_gc_day, 
					mpInUptSt=mn_short.plan_upt_day, 
					mpPathOut=/data/dm_rep/);
	%M_LOG_EVENT(END, rtp_komp_sep);	
	%M_ETL_REDIRECT_LOG(END, rtp_komp_sep, Main);
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from mn_short.plan_pmix_month
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
	
	*%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_KOMP);
	
	
	%M_ETL_REDIRECT_LOG(END, load_to_dp, Main);
	%M_LOG_EVENT(START, load_to_dp);
	
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_KOMP);
	

	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_KOMP);


	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_KOMP);
	
	*%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_KOMP);  
	/* самый долгий */


	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_NONKOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_NONKOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_NONKOMP);
	
	%macro load_csv_to_dp(mpJobName=ACT_LOAD_QNT_FoD_KOMP);
		*%M_ETL_REDIRECT_LOG(START, test, Main);
		filename resp TEMP;
		%let lmvJobName=&mpJobName.;
		%let lmvUrl=&CUR_API_URL.;
		%global SYS_PROCHTTP_STATUS_CODE SYS_PROCHTTP_STATUS_PHRASE;
		%let SYS_PROCHTTP_STATUS_CODE=;
		%let SYS_PROCHTTP_STATUS_PHRASE=;
		filename jsn temp;
		proc http
			url="&lmvUrl./retailAnalytics/processModels/"
			method="GET"
			out=jsn 
			OAUTH_BEARER=SAS_SERVICES;
		run;

		libname posts JSON fileref=jsn ;
		title "Automap of JSON data";

		proc datasets noprint;
		   copy in= posts out=work memtype=data;
		   run; 
		quit;

		proc sql noprint;
			create table process_template as 
			select a.*,b.href,b.uri
			from ITEMS as a left join ITEMS_EXECUTE as b
			on a.ordinal_items=b.ordinal_items
			;
		quit;


		proc sql noprint;
			select %str(href) into: lmvJobUrl
			from process_template
			where name="&lmvJobName";
		quit;

		%put &=lmvJobUrl;


		proc http
			url="&lmvUrl.&lmvJobUrl."
			method="POST"
			OAUTH_BEARER=SAS_SERVICES
			out=resp;
			headers
			"Content-Type" ="application/vnd.sas.retail.process.data+json";
		run;
		
		%let SERVICESBASEURL=10.252.151.3/;

		libname respjson JSON fileref=resp;
		%put &=SYS_PROCHTTP_STATUS_CODE &=SYS_PROCHTTP_STATUS_PHRASE;
		%echo_File(resp);


		%local stateUri;
		%let stateUri=;
		  data _null_;
			set respjson.links;
			if rel='state' then 
				call symput('stateUri', uri);
		  run;
		
		%local jobState;
		
		%do %until(&jobState ^= running);
		
		  proc http
			method="GET"
			url="&SERVICESBASEURL.&stateUri"
			out=resp
			OAUTH_BEARER=SAS_SERVICES;
		  run;
		  %put Response status: &SYS_PROCHTTP_STATUS_CODE;
		
		  %echo_File(resp);
		  libname respjs1 JSON fileref=resp;
		  data _null_;
			 set respjs1.root;
			call symputx('jobState', state);
		  run;
		
		  %put jobState = &jobState;	
		
		  data _null_;
			call sleep(50000);
		  run;
		
		%end;
		
		%if not (&jobState = completed) %then %do;
		  %put ERROR: An invalid response was received.;
		  %abort;
		%end;
		*%M_ETL_REDIRECT_LOG(END, test, Main);
		
	%mend load_csv_to_dp;
	%load_csv_to_dp(mpJobName=ACT_LOAD_UPT_FoD_KOMP);
	*%load_csv_to_dp(mpJobName=ACT_LOAD_QNT_FoD_KOMP);
	
	/* start seeding */
	%dp_jobexecution(mpJobName=ACT_SEED_COMP_SALE_MONTH);
	%dp_jobexecution(mpJobName=ACT_SEED_COMP_SALE_DAY);
	%dp_jobexecution(mpJobName=ACT_SEED_COMP_GC_MONTH);
	%dp_jobexecution(mpJobName=ACT_SEED_COMP_GC_DAY);
	%dp_jobexecution(mpJobName=ACT_SEED_COMP_UPT_MONTH);
	%dp_jobexecution(mpJobName=ACT_SEED_COMP_UPT_DAY);
	
	%dp_jobexecution(mpJobName=ACT_QNT_SEED_MON_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_QNT_SEED_DAY_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_GC_SEED_MON_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_GC_SEED_DAY_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_UPT_SEED_MON_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_UPT_SEED_DAY_NONKOMP);

	%M_LOG_EVENT(END, load_to_dp);	
	%M_ETL_REDIRECT_LOG(END, load_to_dp, Main);

%mend rtp_full_process;