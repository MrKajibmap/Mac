/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для общего процесса: загрузка таблицы моделей, обучение моделей, скоринг по моделяц из таблицы
*
*  ПАРАМЕТРЫ:
*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
*	  mpOutTrain	- выходная таблица набора для обучения
*	  mpOutScore	- выходная таблица набора для скоринга
*	
*	
******************************************************************
*  Использует: 
*	  %load_model_table(mpFile=&external_modeltable., mpModTable=&modeltable.);
*	  %m_rtp_create_model_table(cts=&categories., abt=&traintable., modtable=&modeltable., params=&default_params., interval=&default_interval., nominal=&default_nominal., prefix=&model_prefix.);=
*	  %rtp_train_multi(mpThreadCnt=10,
*						mpModelTable=&modeltable.,
*						mpId = &ids.,
*						mpTarget =&target.,
*						mpAbt = &traintable.,
*						mpPrefix = &model_prefix.,
*						mpStart = 1);
*
*	%rtp_score_multi(mpThreadCnt=10,
*					mpModelTable=&modeltable.,
*					mpId = &ids.,
*					mpTarget =sum_qty,
*					mpAbt = &scoretable.,
*					mpPrefix = FOREST,
*					mpStart = 1,
*					mpOut = &resulttable.)
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
* Набор для товаров 
*%rtp_4_modeling(mode=SCORE,
*				external=1,
*				ids = product_id pbo_location_id sales_dt,
*				target=sum_qty.,
*				categories=lvl2_id prod_lvl2_id, 
*				external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv, 
*				modeltable=PMIX_MODEL_TABLE,
*				traintable=dm_abt.all_ml_train,
*				scoretable=dm_abt.all_ml_scoring,
*				resulttable=dm_abt.PMIX_DAYS_RESULT, 
*				default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,
*				default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX,
*				default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
*				model_prefix=FOREST);
*
*
* Набор для мастер-кодов 
*%rtp_4_modeling(external=1, 
*			ids=&master_ids.,
*			target=&master_target., 
*			categories=&master_categories.,
*			external_modeltable=&master_external_modeltable., 
*			modeltable=&master_modeltable.,
*			traintable=&master_traintable., 
*			scoretable=&master_scoretable., 
*			resulttable=&master_resulttable., 
*			default_params=&master_default_params., 
*			default_interval=&master_default_interval., 
*			default_nominal=&master_default_nominal.,
*			model_prefix=&master_model_prefix.);
*
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
****************************************************************************/

/* Загрузка таблицы моделей извне */
/* mpFile - путь к файлу с таблицей моделей */
/* mpModTable - таблица моделей в Models */
%macro load_model_table(mpFile=&external_modeltable., mpModTable=&modeltable.);
	proc casutil incaslib="Models" outcaslib="Models";
		droptable casdata="&mpModTable." quiet;
	run;

	%let max_length = $500;

	data models.&mpModTable.;
		length filter model params interval nominal &max_length.;
		infile "&mpFile." dsd firstobs=2;                 
		input filter $ model $ params $ interval $ nominal $ train score n;                            
	run;
	
	proc casutil;                           
	    save casdata="&mpModTable." incaslib="models" outcaslib="models" replace; 
		promote casdata="&mpModTable." incaslib="Models" outcaslib="Models";
	run;
%mend load_model_table;


/* Создание таблицы моделей */
/* Можно запускать, если нет внешнего файла */

/* mpCts - набор категорий, по которым нужно разбить модели */
/* mpAbt - витрина на которой будет обучение моделей  */
/* mpModelTable - название таблицы моделей в Models*/
/* mpParams - дефолтные гиперпараметры */
/* mpInterval - входные интервальные фичи */
/* mpNominal - входные категориальные фичи */
/* mpPrefix - префикс названий моделей */
%macro m_rtp_create_model_table(mpCts=&categories.,
								mpAbt=&traintable.,
								mpModelTable=&modeltable., 
								mpParams=&default_params., 
								mpInterval=&default_interval., 
								mpNominal=&default_nominal., 
								mpPrefix=&model_prefix.);

	proc casutil incaslib="Models" outcaslib="Models";
		droptable casdata="&mpModelTable." quiet;
	run;

	%local lmvLastCat lmvLibrefAbt lmvTabNmAbt;

	%let lmvLastCat = %scan(&mpCts., -1);
	%member_names (mpTable=&mpAbt, 
					mpLibrefNameKey=lmvLibrefAbt,
					mpMemberNameKey=lmvTabNmAbt);
	
	/* Получение всех комбинаций категорий */
	data casuser.categories / sessref="casauto" single=yes;
		set &lmvLibrefAbt..&lmvTabNmAbt.(keep=&mpCts.);
		by &mpCts.;
		if first.&lmvLastCat.;
	run;
	
	/* Генерация таблицы моделей с условиями фильтрации */
	data models.&mpModelTable.;
		set casuser.categories;
		length filter model params interval nominal $300;
		keep filter model params interval nominal train score n; 
		array cats[*] &mpCts.;
	
		filter = '';
		string = "&mpCts.";
		do i=1 to dim(cats);
			if i > 1 then filter = catx('', filter, 'and');
			filter = catx('', filter, scan(string, i), '=', input(strip(cats[i]), $10.));
		end;
		model = catx('_', "&mpPrefix.", _n_);
		params = "&mpParams.";
		interval = "&mpInterval.";
		nominal = "&mpNominal.";
		train = 1;
		score = 1;
		n = _n_;
	run;
	
	proc casutil incaslib="Models" outcaslib="Models";
		promote casdata="&mpModelTable.";
	run;
%mend m_rtp_create_model_table;

/* Общий процесс: загрузка таблицы, обучение, скоринг */
/* Нужно менять макропеременные наверху, здесь - только комментировать ненужные строки */
/* external - флаг того, нужно ли загружать внешнюю таблицу */
%macro rtp_4_modeling(mode=,
					external=,
					ids=,
					target=,
					categories=, 
					external_modeltable=, 
					modeltable=,
					traintable=,
					scoretable=,
					resulttable=, 
					default_params=,
					default_interval=,
					default_nominal=,
					model_prefix=);

	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	%local lmvMode;
	%let lmvMode = %sysfunc(upcase(&mode.));
	/* check for input params */
	/* %if &lmvMode. <> SCORE or &lmvMode. <> TRAIN or &lmvMode. <> FULL %then %do;
		%put ERROR: INVALID VALUE FOR PARAMETER >> MODE << (SCORE|TRAIN|FULL);
		%abort;
	%end; */

	%if &external. %then %do;
		%load_model_table(mpFile=&external_modeltable.,
							mpModTable=&modeltable.);
	%end;
	%else %do;
		%m_rtp_create_model_table(cts=&categories.,
							abt=&traintable.,
							modtable=&modeltable., 
							params=&default_params.,
							interval=&default_interval.,
							nominal=&default_nominal.,
							prefix=&model_prefix.);
	%end;

	data _null_;
		set models.&modeltable. nobs=nobs;
		call symputx('length', nobs, 'G');
		stop;
	run;
	
	%if &lmvMode. = TRAIN or &lmvMode. = FULL %then %do;
		%rtp_train_multi(mpThreadCnt=10,
							mpModelTable=&modeltable.,
							mpId = &ids.,
							mpTarget =&target.,
							mpAbt = &traintable.,
							mpPrefix = &model_prefix.,
							mpStart = 1);
	%end;
	%if &lmvMode. = SCORE or &lmvMode. = FULL %then %do;
		%rtp_score_multi(mpThreadCnt=10,
						mpModelTable=&modeltable.,
						mpId = &ids.,
						mpTarget =&target.,
						mpAbt = &scoretable.,
						mpPrefix = &model_prefix.,
						mpStart = 1,
						mpOut = &resulttable.);
	%end;
%mend rtp_4_modeling;