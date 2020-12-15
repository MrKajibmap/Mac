/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (мастеркоды)
*		Строится на основе выходных таблиц процесса rtp1_load_data_product
*
*  ПАРАМЕТРЫ:
*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
*	  mpOutTrain	- выходная таблица набора для обучения
*	  mpOutScore	- выходная таблица набора для скоринга
*	
*	
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*	%macro rtp_2_load_data_mastercode(mpMode=A,
*							mpInputTableScore=casuser.all_ml_scoring, 
*							mpInputTableTrain=casuser.all_ml_train,
*							mpOutputTableScore = casuser.master_code_score,
*							mpOutputTableTrain = casuser.master_code_train
*							);
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
*  27-08-2020  Борзунов		Добавлено сохранение целевых таблиц на диск
****************************************************************************/
%macro rtp_2_load_data_mastercode( mpMode=A,
							mpInputTableScore=casshort.all_ml_scoring, 
							mpInputTableTrain=casshort.all_ml_train,
							mpOutputTableScore = casshort.master_code_score,
							mpOutputTableTrain = casshort.master_code_train,
							mpWorkCaslib=casshort
							);
							
	/****** 0: Объявление макропеременных ******/
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	%local lmvMode 
		lmvStartDate 
		lmvEndDate 
		lmvLibrefScore
		lmvTabNmScore
		lmvLibrefTrain
		lmvTabNmTrain
		lmvLibrefInTr 
		lmvTabNmInTr
		lmvWorkCaslib
		;
			
	%let lmvMode = &mpMode.;
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvStartDateScore =%sysfunc(intnx(year,&etl_current_dt.,-1,s));
	%let lmvWorkCaslib = &mpWorkCaslib.;
	
	%if &lmvMode. = S %then %do;
		%let lmvStartDate =%eval(%sysfunc(intnx(year,&etl_current_dt.,-1,s))-91);
		%let lmvEndDate = &VF_HIST_END_DT_SAS.;
		%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	%end;
	%else %if &lmvMode = T or &lmvMode. = A %then %do;
		%let lmvStartDate = %eval(%sysfunc(intnx(year,&etl_current_dt.,-3,s))-91);
		%let lmvEndDate = &VF_HIST_END_DT_SAS.;
		%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	%end;
	
	%member_names (mpTable=&mpOutputTableScore, mpLibrefNameKey=lmvLibrefScore, mpMemberNameKey=lmvTabNmScore);
	%member_names (mpTable=&mpOutputTableTrain, mpLibrefNameKey=lmvLibrefTrain, mpMemberNameKey=lmvTabNmTrain);
	%member_names (mpTable=&mpInputTableTrain, mpLibrefNameKey=lmvLibrefInTr, mpMemberNameKey=lmvTabNmInTr);
	
	
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND(casauto))  = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto;
	 caslib _all_ assign;
	%end;
	/*  Проверка на существование входных таблиц  */
	%member_exists_list(mpMemberList=&mpInputTableScore.
									&mpInputTableTrain.
									);

	/* 0. Удаление целевых таблиц */
	proc casutil;
		%if &lmvMode. = A or &lmvMode = T %then %do;
			droptable casdata="&lmvTabNmTrain." incaslib="&lmvLibrefTrain." quiet;
		%end;
		%if &lmvMode. = A or &lmvMode = S %then %do;
			droptable casdata="&lmvTabNmScore." incaslib="&lmvLibrefScore." quiet;
		%end;		
	run;
	
	/* Объединение наборов */
	data casuser.all_ml / sessref=casauto;
		set &mpInputTableScore.
			&mpInputTableTrain.
		;
	run;
	/***** 1. Агрегация переменных *****/
	proc casutil;
		droptable casdata="mastercode_abt1_ml" incaslib="casuser" quiet;
		droptable casdata="&lmvTabNmInTr." incaslib="&lmvLibrefInTr." quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.mastercode_abt1_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt,
				count(distinct t1.product_id) as nunique_product,
				sum(t1.sum_qty) as sum_qty,
				mean(t1.GROSS_PRICE_AMT) as GROSS_PRICE_AMT,
				sum(t1.support) as support,
				sum(t1.other_promo) as other_promo,
				sum(t1.side_promo_flag) as side_promo_flag,
				sum(t1.bogo) as bogo,
				sum(t1.discount) as discount,
				sum(t1.evm_set) as evm_set,
				sum(t1.non_product_gift) as non_product_gift,
				sum(t1.pairs) as pairs,
				sum(t1.product_gift) as product_gift,
				sum(t1.sum_trp) as sum_trp
			from
				casuser.all_ml as t1
			group by
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt
		;
	quit;

	/***** 2. Рассчет лагов *****/
	proc casutil;
	  droptable casdata='lag_abt1' incaslib='casuser' quiet;
	  droptable casdata='lag_abt2' incaslib='casuser' quiet;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata='mastercode_abt2_ml' incaslib='casuser' quiet;
	run;

	/* считаем медиану и среднее арифметическое */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='mastercode_abt1_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = "prod_lvl4_id"},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT", 
		code=
			%unquote(%str(%"))
			%let names=; /*будущий список выходных переменных для proc cas */
			%let minlag=91; /*параметр MinLag*/
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
			  %let window=%scan(&window_list,&ic); /*текущее окно*/
			  %let intnm=%rtp_namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._avg[t]=mean(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
				lag_&intnm._med[t]=median(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
			  end;
			 %let names={name=%tslit(lag_&intnm._avg)}, &names;
			 %let names={name=%tslit(lag_&intnm._med)}, &names; 

			%end; /* ic over window_list*/
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt1', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	/* Считаем стандартное отклонение */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='mastercode_abt1_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = "prod_lvl4_id"},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT",
		code=
			%unquote(%str(%"))
			%let names=; /*будущий список выходных переменных для proc cas */
			%let minlag=91; /*параметр MinLag*/
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
			  %let window=%scan(&window_list,&ic); /*текущее окно*/
			  %let intnm=%rtp_namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._std[t]=std(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
			  end;
			 %let names={name=%tslit(lag_&intnm._std)}, &names;

			%end; /* ic over window_list*/
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt2', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	/* Считаем процентили */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='mastercode_abt1_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = "prod_lvl4_id"},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT",
		code=
			%unquote(%str(%"))
			%let names=; /*будущий список выходных переменных для proc cas */
			%let minlag=91; /*параметр MinLag*/
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
			  %let window=%scan(&window_list,&ic); /*текущее окно*/
			  %let intnm=%rtp_namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._pct10[t]=pctl(10,%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
				lag_&intnm._pct90[t]=pctl(90,%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
			  end;
			 %let names={name=%tslit(lag_&intnm._pct10)}, &names;
			 %let names={name=%tslit(lag_&intnm._pct90)}, &names;

			%end; /* ic over window_list*/
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt3', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	/* соеденим среднее, медиану, стд, процентили вместе, убирая пропуску вр ВР */
	proc fedsql sessref=casauto;
		create table casuser.mastercode_abt2_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt,
				t1.nunique_product,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.support,
				t1.other_promo,
				t1.side_promo_flag,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.sum_trp,
				t2.lag_halfyear_avg,
				t2.lag_halfyear_med,
				t2.lag_month_avg,
				t2.lag_month_med,
				t2.lag_qtr_avg,
				t2.lag_qtr_med,
				t2.lag_week_avg,
				t2.lag_week_med,
				t2.lag_year_avg,
				t2.lag_year_med
			from
				casuser.mastercode_abt1_ml as t1,
				casuser.lag_abt1 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.prod_lvl4_id = t2.prod_lvl4_id and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.mastercode_abt2_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt,
				t1.nunique_product,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.support,
				t1.other_promo,
				t1.side_promo_flag,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.sum_trp,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t2.lag_halfyear_std,
				t2.lag_month_std,
				t2.lag_qtr_std,
				t2.lag_week_std,
				t2.lag_year_std
			from
				casuser.mastercode_abt2_ml as t1,
				casuser.lag_abt2 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.prod_lvl4_id = t2.prod_lvl4_id and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.mastercode_abt2_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt,
				t1.nunique_product,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.support,
				t1.other_promo,
				t1.side_promo_flag,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.sum_trp,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t2.lag_halfyear_pct10,		 
				t2.lag_halfyear_pct90,		 
				t2.lag_month_pct10	,
				t2.lag_month_pct90	,
				t2.lag_qtr_pct10,	
				t2.lag_qtr_pct90,	
				t2.lag_week_pct10,	
				t2.lag_week_pct90,	
				t2.lag_year_pct10,	
				t2.lag_year_pct90
			from
				casuser.mastercode_abt2_ml as t1,
				casuser.lag_abt3 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.prod_lvl4_id = t2.prod_lvl4_id and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc casutil;
	  droptable casdata='lag_abt1' incaslib='casuser' quiet;
	  droptable casdata='lag_abt2' incaslib='casuser' quiet;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	run;

	/***** 3. Добваление неизменных переменных *****/
	proc casutil;
	  droptable casdata='const_feature' incaslib='casuser' quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.const_feature{options replace=true} as
			select distinct
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t1.week, 
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				t1.a_cpi,
				t1.a_gpd,
				t1.a_rdi,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC
			from
				casuser.all_ml as t1
		;
		create table casuser.mastercode_full{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.prod_lvl4_id,			
				t1.sales_dt,
				t1.nunique_product,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.support,
				t1.other_promo,
				t1.side_promo_flag,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.sum_trp,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t2.lvl3_id,
				t2.lvl2_id,
				t2.prod_lvl3_id,
				t2.prod_lvl2_id,
				t2.agreement_type,
				t2.breakfast,
				t2.building_type,
				t2.company,
				t2.delivery,
				t2.drive_thru,
				t2.mccafe_type,
				t2.price_level,
				t2.window_type,
				t2.defender_day,
				t2.female_day,
				t2.may_holiday,
				t2.new_year,
				t2.russia_day,
				t2.school_start,
				t2.student_day,
				t2.summer_start,
				t2.valentine_day, 
				t2.week, 
				t2.weekday,
				t2.month,
				t2.weekend_flag,
				t2.a_cpi,
				t2.a_gpd,
				t2.a_rdi,
				t2.TEMPERATURE,
				t2.PRECIPITATION,
				t2.comp_trp_BK,
				t2.comp_trp_KFC
			from
				casuser.mastercode_abt2_ml as t1
			left join
				casuser.const_feature as t2
			on
				t1.channel_cd = t2.channel_cd and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.prod_lvl4_id = t2.prod_lvl4_id and			
				t1.sales_dt = t2.sales_dt
			;
	quit;
	
	/*** Разделение на обучение и скоринг ***/
	proc fedsql sessref=casauto;
		%if &lmvMode. = A or &lmvMode = T %then %do;
			create table casuser.&lmvTabNmTrain.{options replace = true} as
				select 
					* 
				from
					casuser.mastercode_full
				where sales_dt <= date %str(%')%sysfunc(putn(&lmvEndDate., yymmdd10.))%str(%')
			;
		%end;
		%if &lmvMode. = A or &lmvMode = S %then %do;
			create table casuser.&lmvTabNmScore.{options replace = true} as
				select 
					* 
				from
					casuser.mastercode_full
				where
					sales_dt > date %str(%')%sysfunc(putn(&lmvStartDateScore., yymmdd10.))%str(%') and
				sales_dt <= date %str(%')%sysfunc(putn(&lmvScoreEndDate., yymmdd10.))%str(%')
			;
		%end;
	quit;	

	proc casutil;
	  %if &lmvMode = T %then %do;
		 save incaslib="&lmvLibrefTrain." outcaslib="&lmvLibrefTrain." casdata="&lmvTabNmTrain." casout="&lmvTabNmTrain..sashdat" replace; 
	  %end;
	  %if &lmvMode. = A or &lmvMode = S %then %do;
		promote casdata="&lmvTabNmTrain." incaslib="casuser" outcaslib="&lmvLibrefTrain."; 
		promote casdata="&lmvTabNmScore." incaslib="casuser" outcaslib="&lmvLibrefScore.";
		*save incaslib="&lmvLibrefScore." outcaslib="&lmvLibrefScore." casdata="&lmvTabNmScore." casout="&lmvTabNmScore..sashdat" replace; 
	  %end;
	  
	  droptable casdata='const_feature' incaslib='casuser' quiet;
	  droptable casdata="&lmvTabNmTrain." incaslib='casuser' quiet;
	  droptable casdata="&lmvTabNmScore." incaslib='casuser' quiet;
	  droptable casdata='mastercode_full' incaslib='casuser' quiet;
	  droptable casdata='all_ml' incaslib='casuser' quiet;
	  droptable casdata="mastercode_abt1_ml" incaslib="casuser" quiet;
	  droptable casdata='mastercode_abt2_ml' incaslib='casuser' quiet;
	run;

%mend rtp_2_load_data_mastercode;