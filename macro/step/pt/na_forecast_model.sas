options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
/*Создать cas-сессию, если её нет*/
%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

libname etl_stg "/data2/etl_stg_23_11_2020";
libname tmp "/data2/TMP";
%let inlib=etl_stg;

/****** Разделение на обучение и скоринг ******/
data public.ia_promo;
	set etl_stg.ia_promo;
run;

proc fedsql sessref=casauto;;
	create table public.promo_for_test{options replace=true} as
		select
			promo_id
		from 
			public.ia_promo
		where
			datepart(start_dt) >= date'2020-05-01' and
			channel_cd = 'ALL'
	;
	create table public.promo_for_train{options replace=true} as
		select 
			promo_id
		from 
			public.ia_promo
		where
			datepart(start_dt) < date'2020-05-01' and
			channel_cd = 'ALL'
	;
quit;

proc fedsql sessref=casauto;
	create table public.russca_npf_scoring{options replace=true} as
		select
			t1.*
		from
			public.na_abt9 as t1
		inner join
			public.promo_for_test as t2
		on
			t1.promo_id = t2.promo_id
	;
	create table public.russca_npf_train{options replace=true} as
		select
			t1.*
		from
			public.na_abt9 as t1
		inner join
			public.promo_for_train as t2
		on 
			t1.promo_id = t2.promo_id
	;
	select count(1) as test_size from public.russca_npf_scoring;
	select count(1) as train_size from public.russca_npf_train;
	select count(1) as na_abt9 from public.na_abt9;
quit;


/****** Обучение модели ******/
proc casutil;
	droptable casdata="russca_models_na" incaslib="public" quiet;
run;

%let pmix_default_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=100
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;

proc forest data=public.russca_npf_train
	&pmix_default_params.;
	input 
		MEAN_RECEIPT_QTY
		MEAN_SALES_QTY
		STD_RECEIPT_QTY
		STD_SALES_QTY
		PROMO_LIFETIME / level = interval;
	input 
		BREAKFAST
		CHRISTMAS
		CHRISTMAS_DAY
		COLD_DRINKS
		DAY_AFTER_NEW_YEAR
		DEFENDENCE_OF_THE_FATHERLAND
		DRIVE_THRU
		FRIES
		INTERNATIONAL_WOMENS_DAY
		LABOUR_DAY
		NATIONAL_DAY
		NEW_YEAR
		NUGGETS
		regular_weekend_flag
		SN_CORE
		STARTERS_SALAD
		VICTORY_DAY
		weekend_flag 
		AGREEMENT_TYPE
		BUILDING_TYPE
		COMPANY
		DELIVERY
		DESSERTS
		HOT_DRINKS
		MCCAFE_TYPE
		month
		NUMBER_OF_PRODUCTS
		PRICE_LEVEL
		SN_EDAP
		week
		weekday
		WINDOW_TYPE
		year / level = nominal;
	id promo_id pbo_location_id sales_dt;
	target n_a / level = interval;
	*partition FRACTION(VALIDATE=0.3);
	savestate rstore=public.russca_models_na;
	;
run;

/****** Скоринг ******/
proc casutil;
    droptable casdata="russca_npf_scoring_pred" incaslib="public" quiet;
run;


proc astore;
  score data=public.russca_npf_scoring
  copyvars=(_all_)
  rstore=public.russca_models_na
  out=public.russca_npf_scoring_pred;
quit;

proc casutil;
    promote casdata="russca_npf_scoring_pred" incaslib="public" outcaslib="public";
run;


/****** Расчет ошибки ******/

/* MAE */
proc fedsql sessref=casauto;
	select
		promo_id,
		mean(abs(n_a - P_n_a)) as mae
	from
		public.russca_npf_scoring_pred
	group by
		promo_id
	;
quit;

/* MAPE */
proc fedsql sessref=casauto;
	select
		promo_id,
		mean(divide(abs(n_a - P_n_a), n_a)) as mape,
		divide(sum(abs(n_a - P_n_a)), sum(n_a)) as wape,
		divide(sum((n_a - P_n_a)), sum(n_a)) as bias
	from
		public.russca_npf_scoring_pred
	group by
		promo_id
	;
quit;

