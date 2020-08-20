/* 
	Схема программы:

	0: Объявление макропеременных. Даты начала и конца истории, название таблицы
		для агрегации, переменная по которой будем агрегировать
	1: Агрегация переменных:
		* Цены усредняем
		* Флаги промо суммируем
		* Целевую переменную суммируем
	2: Считаем лаги
	3: Считаем константные переменные, которые не меняются для группы товаров:
		* Атрибуты ПБО
		* События
		* Календарные признаки
		* TRP конкурентов
		* Погода
		* Макроэкономика
	4: Разделение на обучение и скоринг
*/

%macro rtp_train_score_sep;
	/****** 0: Объявление макропеременных ******/
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND(casauto))  = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto;
	 caslib _all_ assign;
	%end;

	/* %let aggr_table = public.russca_pmix_sample; */
	%let hist_start_dt = date '2017-01-02';
	%let hist_end_dt = date '2020-02-29';
	/* %let aggr_var = prod_lvl4_id; */

	%macro middle_lvl_data(aggr_table, aggr_var, prefix);

	/***** 1. Агрегация переменных *****/
	proc casutil;
		droptable casdata="mastercode_abt1_ml" incaslib="public" quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.mastercode_abt1_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
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
				&aggr_table. as t1
			group by
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
				t1.sales_dt
		;
	quit;


	/***** 2. Рассчет лагов *****/
	proc casutil;
	  droptable casdata='lag_abt1' incaslib='public' quiet;
	  droptable casdata='lag_abt2' incaslib='public' quiet;
	  droptable casdata='lag_abt3' incaslib='public' quiet;
	  droptable casdata='mastercode_abt2_ml' incaslib='public' quiet;
	run;

	/* Макрос разворачивает переменную var в список элементов массива, разделенных запятой */
	/* var[t-0],var[t-1],var[t-2],... */
	%macro transform_var_to_array_dlm(var,index,start,end);
	%do ii=&start. %to &end.;
	 &var.[&index.-&ii.]
	 %if &ii. ne &end. %then %do;
	  ,
	 %end;
	%end;
	%mend transform_var_to_array_dlm;

	/* Перекодирование числа дней в название интервала*/
	%macro namet(l_int);
	%if &l_int=7 %then week ;
	%if &l_int=30 %then month ;
	%if &l_int=90 %then qtr ;
	%if &l_int=180 %then halfyear ;
	%if &l_int=365 %then year ;
	%mend namet;

	/* считаем медиану и среднее арифметическое */
	%macro cmpcode;
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='mastercode_abt1_ml',
			caslib = 'public', 
			groupBy = {
				{name = "&aggr_var."},
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
			  %let intnm=%namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._avg[t]=mean(%transform_var_to_array_dlm(mpVar=sum_qty,mpIndex=t,mpStart=%eval(&lag),mpEnd=%eval(&lag+&window-1)));
				lag_&intnm._med[t]=median(%transform_var_to_array_dlm(mpVar=sum_qty,mpIndex=t,mpStart=%eval(&lag),mpEnd=%eval(&lag+&window-1)));
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
			table={name='lag_abt1', replace=true, caslib='Public'},
			arrays={&names}
		}
	;
	run;
	quit;
	%mend cmpcode;

	%cmpcode

	/* Считаем стандартное отклонение */
	%macro cmpcode2;
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='mastercode_abt1_ml',
			caslib = 'public', 
			groupBy = {
				{name = "&aggr_var."},
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
			  %let intnm=%namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._std[t]=std(%transform_var_to_array_dlm(mpVar=sum_qty,mpIndex=t,mpStart=%eval(&lag),mpEnd=%eval(&lag+&window-1)));
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
			table={name='lag_abt2', replace=true, caslib='Public'},
			arrays={&names}
		}
	;
	run;
	quit;
	%mend cmpcode2;

	%cmpcode2

	/* Считаем процентили */
	%macro cmpcode3;
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='mastercode_abt1_ml',
			caslib = 'public', 
			groupBy = {
				{name = "&aggr_var."},
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
			  %let intnm=%namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._pct10[t]=pctl(10,%transform_var_to_array_dlm(mpVar=sum_qty,mpIndex=t,mpStart=%eval(&lag),mpEnd=%eval(&lag+&window-1))) ;
				lag_&intnm._pct90[t]=pctl(90,%transform_var_to_array_dlm(mpVar=sum_qty,mpIndex=t,mpStart=%eval(&lag),mpEnd=%eval(&lag+&window-1))) ;
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
			table={name='lag_abt3', replace=true, caslib='Public'},
			arrays={&names}
		}
	;
	run;
	quit;
	%mend cmpcode3;
	%cmpcode3

	/* соеденим среднее, медиану, стд, процентили вместе, убирая пропуску вр ВР */
	proc fedsql sessref=casauto;
		create table public.mastercode_abt2_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
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
				public.mastercode_abt1_ml as t1,
				public.lag_abt1 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.&aggr_var. = t2.&aggr_var. and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;
	
		proc casutil;
		promote casdata="mastercode_abt1_ml" incaslib="сasuser" outcaslib="public";
	run;

	proc fedsql sessref=casauto;
		create table public.mastercode_abt2_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
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
				public.mastercode_abt2_ml as t1,
				public.lag_abt2 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.&aggr_var. = t2.&aggr_var. and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc fedsql sessref=casauto;
		create table public.mastercode_abt2_ml{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
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
				public.mastercode_abt2_ml as t1,
				public.lag_abt3 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.&aggr_var. = t2.&aggr_var. and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc casutil;

	  droptable casdata='lag_abt1' incaslib='public' quiet;
	  droptable casdata='lag_abt2' incaslib='public' quiet;
	  droptable casdata='lag_abt3' incaslib='public' quiet;
	run;


	/***** 3. Добваление неизменных переменных *****/
	proc casutil;
	  droptable casdata="russca_&aggr_var._sample" incaslib='public' quiet;
	  droptable casdata='const_feature' incaslib='public' quiet;
	run;

	proc fedsql sessref=casauto;
		create table public.const_feature{options replace=true} as
			select distinct
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
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
				&aggr_table. as t1
		;
		create table public.russca_&aggr_var._sample{options replace=true} as
			select
				t1.channel_cd,
				t1.pbo_location_id,
				t1.&aggr_var.,			
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
				public.mastercode_abt2_ml as t1
			left join
				public.const_feature as t2
			on
				t1.channel_cd = t2.channel_cd and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.&aggr_var. = t2.&aggr_var. and			
				t1.sales_dt = t2.sales_dt
			;
	quit;

	proc casutil;
	  droptable casdata='const_feature' incaslib='public' quiet;
	  droptable casdata="russa_&aggr_var._train_&prefix." incaslib='public' quiet;
	  droptable casdata="russa_&aggr_var._score_&prefix." incaslib='public' quiet;
	  promote casdata="mastercode_abt2_ml" incaslib="public" outcaslib="public";
	run;

	/*** Разделение на обучение и скоринг ***/
	proc fedsql sessref=casauto;
		create table public.russa_&aggr_var._train_&prefix.{options replace = true} as
			select 
				* 
			from
				public.russca_&aggr_var._sample 
			where
				sales_dt < date'2019-11-01'
		;
		create table public.russa_&aggr_var._score_&prefix.{options replace = true} as
			select 
				* 
			from
				public.russca_&aggr_var._sample 
			where
				sales_dt >= date '2019-11-01' and
				sales_dt < date '2020-03-01'
		;
	quit;

	proc casutil;
	  promote casdata="russa_&aggr_var._train_&prefix." incaslib="public" outcaslib="public";
	  promote casdata="russa_&aggr_var._score_&prefix." incaslib="public" outcaslib="public";
	run;

	%mend;


	/* от Ани: сначала сливаем воедино товарные витрины */
	data public.all_ml;
		set public.russca_all_ml_train public.russca_all_ml_scoring;
	run;

	%middle_lvl_data(public.all_ml, prod_lvl4_id, full)

%mend rtp_train_score_sep;