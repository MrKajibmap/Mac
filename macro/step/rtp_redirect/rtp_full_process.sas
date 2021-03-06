%macro rtp_full_process;
	cas casauto sessopts=(metrics=true);
	caslib _all_ assign;

	%M_ETL_REDIRECT_LOG(START, rtp_full_process, Main);

	/*  Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (продукты) 
	*  Пример использования:
	*    %rtp_1_load_data_product(mpMode=S, mpOutScore=casuser.all_ml_scoring);
	*	 %rtp_1_load_data_product(mpMode=T, mpOutTrain=casuser.all_ml_train);
	*	 %rtp_1_load_data_product(mpMode=A, mpOutTrain=casuser.all_ml_train, mpOutScore=casuser.all_ml_scoring);
	*  ПАРАМЕТРЫ:
	*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
	*	  mpOutTrain	- выходная таблица набора для обучения
	*	  mpOutScore	- выходная таблица набора для скоринга */
	
	%M_LOG_EVENT(START, rtp_1_load_data_product);
	%rtp_1_load_data_product(mpMode=A, mpOutTrain=dm_abt.all_ml_train, mpOutScore=dm_abt.all_ml_scoring);
	%M_LOG_EVENT(END, rtp_1_load_data_product);
	%symdel mvcnt mvExist;

	proc sql;
		select count(*) as cnt into :mvcnt
		from dm_abt.all_ml_scoring
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
	%M_LOG_EVENT(START, rtp_2_load_data_mastercode);
	%rtp_2_load_data_mastercode( mpMode=A,
								mpInputTableScore=dm_abt.all_ml_scoring, 
								mpInputTableTrain=dm_abt.all_ml_train,
								mpOutputTableScore = dm_abt.master_code_score,
								mpOutputTableTrain = dm_abt.master_code_train
								); 
	%M_LOG_EVENT(END, rtp_2_load_data_mastercode);		
	
	%symdel mvcnt mvExist;
	proc sql;

		select count(*) as cnt into :mvcnt
		from dm_abt.master_code_score
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
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
	%M_LOG_EVENT(START, rtp_3_load_data_pbo);
	%rtp_3_load_data_pbo(mpMode=A, 
								mpOutTableTrain=dm_abt.pbo_train,
								mpOutTableScore=dm_abt.pbo_score); 
	%M_LOG_EVENT(END, rtp_3_load_data_pbo);
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from dm_abt.pbo_score
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
	
	*%rtp_4_modeling(mode=TRAIN,
					external=1,
					ids = product_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
					modeltable=PMIX_MODEL_TABLE,				
					traintable=dm_abt.all_ml_train,
					scoretable=dm_abt.all_ml_scoring,
					resulttable=dm_abt.pmix_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,				
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,				
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=FOREST);	
	%M_LOG_EVENT(START, rtp_4_modeling_PMIX);
	%rtp_4_modeling(mode=SCORE,
					external=1,
					ids = product_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
					modeltable=PMIX_MODEL_TABLE,				
					traintable=dm_abt.all_ml_train,
					scoretable=dm_abt.all_ml_scoring,
					resulttable=dm_abt.pmix_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,				
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,				
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=FOREST);	
	%M_LOG_EVENT(END, rtp_4_modeling_PMIX);			
	*%rtp_4_modeling(mode=TRAIN,
					external=1,
					ids = prod_lvl4_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/MASTER_MODEL_TABLE.csv, 
					modeltable=MASTER_MODEL_TABLE,				
					traintable=dm_abt.master_code_train,
					scoretable=dm_abt.master_code_score,
					resulttable=dm_abt.pmix_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX NUNIQUE_PRODUCT,
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=MASTER_FOREST);	
	%M_LOG_EVENT(START, rtp_4_modeling_MC);			
	%rtp_4_modeling(mode=SCORE,
					external=1,
					ids = prod_lvl4_id pbo_location_id sales_dt,
					target=sum_qty,
					categories=lvl2_id prod_lvl2_id, 
					external_modeltable=/data/files/input/MASTER_MODEL_TABLE.csv, 
					modeltable=MASTER_MODEL_TABLE,				
					traintable=dm_abt.master_code_train,
					scoretable=dm_abt.master_code_score,
					resulttable=dm_abt.master_code_days_result, 
					default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,
					default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX NUNIQUE_PRODUCT,
					default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
				model_prefix=MASTER_FOREST);
	%M_LOG_EVENT(END, rtp_4_modeling_MC);				
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from dm_abt.master_code_days_result
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;

	%M_LOG_EVENT(START, rtp_5_reconcil);	
	%rtp_5_reconcil(mpFSAbt = dm_abt.pbo_train,
						mpMasterCodeTbl = dm_abt.MASTER_CODE_DAYS_RESULT,
						mpProductTable = DM_ABT.PMIX_DAYS_RESULT,
						mpResultTable = DM_ABT.PMIX_RECONCILED_FULL
						);
	%M_LOG_EVENT(END, rtp_5_reconcil);						
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from dm_abt.PMIX_RECONCILED_FULL
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;					
	/* Обратная интеграция + ПЛМ */
	%M_LOG_EVENT(START, rtp_7_out_integration);	
	%rtp_7_out_integration(mpVfPmixProjName=pmix_sales_v2,
								mpVfPboProjName=pbo_sales_v1,
								mpMLPmixTabName=dm_abt.pmix_reconciled_full,
								mpInEventsMkup=dm_abt.events_mkup,
								mpInWpGc=dm_abt.wp_gc,
								mpOutPmixLt=casuser.plan_pmix_month,
								mpOutGcLt=casuser.plan_gc_month, 
								mpOutUptLt=casuser.plan_upt_month, 
								mpOutPmixSt=casuser.plan_pmix_day,
								mpOutGcSt=casuser.plan_gc_day, 
								mpOutUptSt=casuser.plan_upt_day, 
								mpOutOutforgc=casuser.TS_OUTFORGC,
								mpOutOutfor=casuser.TS_OUTFOR, 
								mpOutNnetWp=public.nnet_wp1,
								mpPrmt=Y) ;
	%M_LOG_EVENT(END, rtp_7_out_integration);						
	%symdel mvcnt mvExist;
	proc sql;
		select count(*) as cnt into :mvcnt
		from casuser.plan_pmix_month
		;
	quit;
	%let mvExist=%symexist(mvcnt);

	%if &mvExist=0 %then %do;
		%abort;
	%end;
	%M_LOG_EVENT(START, rtp_komp_sep);
	%rtp_komp_sep(mpVfPmixProjName=pmix_sales_v1,
								mpVfPboProjName=pbo_sales_v1,
								mpMLPmixTabName=dm_abt.pmix_reconciled_full,
								mpInEventsMkup=dm_abt.events_mkup,
								mpInWpGc=dm_abt.wp_gc,
								mpOutPmixLt=casuser.plan_pmix_month,
								mpOutGcLt=casuser.plan_gc_month, 
								mpOutUptLt=casuser.plan_upt_month, 
								mpOutPmixSt=casuser.plan_pmix_day,
								mpOutGcSt=casuser.plan_gc_day, 
								mpOutUptSt=casuser.plan_upt_day, 
								mpOutOutforgc=casuser.TS_OUTFORGC,
								mpOutOutfor=casuser.TS_OUTFOR, 
								mpOutNnetWp=public.nnet_wp1,
								mpPrmt=N) ;
	%M_LOG_EVENT(END, rtp_komp_sep);	
	
	%M_LOG_EVENT(START, load_to_dp);
	
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_KOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_KOMP);


	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_KOMP);


	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_NONKOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_NONKOMP);

	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_NONKOMP);
	
	%M_LOG_EVENT(END, load_to_dp);	
	%M_ETL_REDIRECT_LOG(END, rtp_full_process, Main);

%mend rtp_full_process;