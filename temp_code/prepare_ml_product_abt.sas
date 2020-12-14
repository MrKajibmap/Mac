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

/* Подключение библиотек */
libname ETL_STG "/data/ETL_STG";
/* Объявление макропеременных */
%let inlib=ETL_STG;
/* только бургеры в канале ALL */
%let filter = t2.prod_lvl3_id in (900116, 2420116, 2430116) and t1.channel_cd = 'ALL'; 
%let hist_start_dt = date '2017-01-02';
%let hist_end_dt = date '2020-05-31';


/****** 1. Сбор "каркаса" из таблиц ia_pmix и ia_pmix_history ******/
/* 
	Подготовка словаря продуктов с иерархиями и атрибутами.
	Сейчас он нужен, чтобы оставить только бургеры.
	Заодно сразу добавляем атрибуты товаров и календарные признаки
*/
proc casutil;
  droptable casdata="product_dictionary_ml" incaslib="public" quiet;
  load data=&inlib..IA_product casout='ia_product' outcaslib='public' replace;
  load data=&inlib..IA_product_HIERARCHY casout='IA_product_HIERARCHY' outcaslib='public' replace;
  load data=&inlib..IA_product_ATTRIBUTES casout='IA_product_ATTRIBUTES' outcaslib='public' replace;
run;
  
proc cas;
transpose.transpose /
   table={name="ia_product_attributes", caslib="public", groupby={"product_id"}} 
   attributes={{name="product_id"}} 
   transpose={"PRODUCT_ATTR_VALUE"} 
   prefix="A_" 
   id={"PRODUCT_ATTR_NM"} 
   casout={name="attr_transposed", caslib="public", replace=true};
quit;

proc fedsql sessref=casauto;
   create table public.product_hier_flat{options replace=true} as
		select t1.product_id, 
			   t2.product_id  as LVL4_ID,
			   t3.product_id  as LVL3_ID,
			   t3.PARENT_product_id as LVL2_ID, 
			   1 as LVL1_ID
		from 
		(select * from public.ia_product_hierarchy where product_lvl=5) as t1
		left join 
		(select * from public.ia_product_hierarchy where product_lvl=4) as t2
		on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
		left join 
		(select * from public.ia_product_hierarchy where product_lvl=3) as t3
		on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
 		;
quit;

proc fedsql sessref=casauto;
   create table public.product_dictionary_ml{options replace=true} as
   select t1.product_id, 
	   coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
	   coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
	   coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
	   coalesce(t15.product_nm,'NA') as product_nm,
	   coalesce(t14.product_nm,'NA') as prod_lvl4_nm,
	   coalesce(t13.product_nm,'NA') as prod_lvl3_nm,
	   coalesce(t12.product_nm,'NA') as prod_lvl2_nm,
       t3.A_HERO,
       t3.A_ITEM_SIZE,
	   t3.A_OFFER_TYPE,
	   t3.A_PRICE_TIER
   from public.product_hier_flat t1
   left join public.attr_transposed t3
   on t1.product_id=t3.product_id
   left join PUBLIC.IA_product t15
   on t1.product_id=t15.product_id
   left join PUBLIC.IA_product t14
   on t1.lvl4_id=t14.product_id
   left join PUBLIC.IA_product t13
   on t1.lvl3_id=t13.product_id
   left join PUBLIC.IA_product t12
   on t1.lvl2_id=t12.product_id;
quit;

/* Перекодировка текстовых переменных. */
%macro text_encoding(table, variable);
	/*
	Параметры:
		table : таблица в которой хотим заненить текстовую переменную
		variable : название текстовой переменной
	Выход:
		* Таблица table с дополнительным столбцом variable_id
		* Таблица encoding_variable с привозкой id к старым значениям
	*/
	proc casutil;
 		droptable casdata="encoding_&variable." incaslib="public" quiet;
 	run;

	proc fedsql sessref=casauto;
		create table public.unique{options replace=true} as
			select distinct
				&variable
			from
				&table. 
			;
	quit;

	data public.encoding_&variable.;
		set public.unique;
		&variable._id = _N_;
	run;

	proc fedsql sessref = casauto;
		create table public.&table.{options replace=true} as 
			select
				t1.*,
				t2.&variable._id
			from
				&table. as t1
			left join
				public.encoding_&variable. as t2
			on
				t1.&variable = t2.&variable
		;
	quit;

	proc casutil;
		promote casdata="encoding_&variable." incaslib="public" outcaslib="public";
	run;
%mend;

%text_encoding(public.product_dictionary_ml, a_hero)
%text_encoding(public.product_dictionary_ml, a_item_size)
%text_encoding(public.product_dictionary_ml, a_offer_type)
%text_encoding(public.product_dictionary_ml, a_price_tier)

proc casutil;
  promote casdata="product_dictionary_ml" incaslib="public" outcaslib="public";
  droptable casdata='ia_product' incaslib='public' quiet;
  droptable casdata='IA_product_HIERARCHY' incaslib='public' quiet;
  droptable casdata='IA_product_ATTRIBUTES' incaslib='public' quiet;
  droptable casdata='product_hier_flat' incaslib='public' quiet;
  droptable casdata='attr_transposed' incaslib='public' quiet;
run;

/*
	Подготовка таблицы с продажами.
	Соеденияем ia_pmix_sales и ia_pmix_sales_history.
*/
proc casutil;
  droptable casdata="abt1_ml" incaslib="public" quiet;
  load data=&inlib..IA_pmix_sales casout='ia_pmix_sales' outcaslib='public' replace;
  load data=&inlib..IA_pmix_sales_HISTORY casout='IA_pmix_sales_HISTORY' outcaslib='public' replace;
run;

proc fedsql sessref=casauto; 
	create table public.abt1_ml{options replace=true} as
	select 
		t1.PBO_LOCATION_ID,
		t1.PRODUCT_ID,
		t1.CHANNEL_CD,
		t1.SALES_DT,
		t1.sum_qty,
		t2.prod_lvl4_id, 
		t2.prod_lvl3_id,
		t2.prod_lvl2_id,
		t2.a_hero_id as hero,
		t2.a_item_size_id as item_size,
		t2.a_offer_type_id as offer_type,
		t2.a_price_tier_id as price_tier
	from (
		select 
			coalesce(t1.PBO_LOCATION_ID, t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
			coalesce(t1.PRODUCT_ID, t2.PRODUCT_ID) as PRODUCT_ID,
			coalesce(t1.CHANNEL_CD, t2.CHANNEL_CD) as CHANNEL_CD,
			coalesce(t1.SALES_D, t2.SALES_D) as SALES_DT,
			coalesce(t1.SALES_QTY, t2.SALES_QTY, 0) + coalesce(t1.SALES_QTY_PROMO, t2.SALES_QTY_PROMO, 0) as sum_qty
		from (
			select *, datepart(sales_dt) as sales_d from public.ia_pmix_sales 
		) t1 full outer join (
			select *, datepart(sales_dt) as sales_d from public.ia_pmix_sales_history
		) t2 on 
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.CHANNEL_CD = t2.CHANNEL_CD and 
			t1.SALES_D = t2.SALES_D
		) as t1 
	left join
		 public.product_dictionary_ml as t2 
	on
		t1.product_id = t2.product_id
	where
		&filter and
		t1.SALES_DT >= &hist_start_dt and
		t1.SALES_DT <= &hist_end_dt 
;
quit;

proc casutil;
  promote casdata="abt1_ml" incaslib="public" outcaslib="public";
  droptable casdata='ia_pmix_sales' incaslib='public' quiet;
  droptable casdata='IA_pmix_sales_HISTORY' incaslib='public' quiet;
run;


/****** 2. Добавляем цены. ******/
proc casutil;
  droptable casdata="price_ml" incaslib="public" quiet;
  droptable casdata="abt2_ml" incaslib="public" quiet;
  load data=&inlib..ia_price_history casout='ia_price_history' outcaslib='public' replace;
  load data=&inlib..ia_price casout='ia_price' outcaslib='public' replace;
run;

proc fedsql sessref=casauto; 
	/* Объединяем историю с актуальными данными */
	create table public.price_ml{options replace=true} as
		select 
			coalesce(t1.PBO_LOCATION_ID, t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
			coalesce(t1.PRODUCT_ID, t2.PRODUCT_ID) as PRODUCT_ID,
			coalesce(datepart(t1.start_dt), datepart(t2.start_dt)) as start_dt,
			coalesce(datepart(t1.end_dt), datepart(t2.end_dt)) as end_dt,
			coalesce(t1.GROSS_PRICE_AMT, t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT,
			coalesce(t1.NET_PRICE_AMT, t2.NET_PRICE_AMT) as NET_PRICE_AMT,
			coalesce(t1.PRICE_TYPE, t2.PRICE_TYPE) as PRICE_TYPE
		from 
			public.ia_price as t1
			full outer join
			public.ia_price_history as t2 on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and 
			t1.start_dt = t2.start_dt and
			t1.end_dt = t2.end_dt and 
			t1.PRICE_TYPE = t2.PRICE_TYPE
	;
	/* Добавляем к продажам цены */
	create table public.abt2_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t2.GROSS_PRICE_AMT
		from
			public.abt1_ml as t1 left join
			public.price_ml as t2 
		on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT <= t2.end_dt and	
			t1.SALES_DT >= t2.start_dt
	;
quit;

proc casutil;
  promote casdata="abt2_ml" incaslib="public" outcaslib="public";
  droptable casdata='price_ml' incaslib='public' quiet;
  droptable casdata='ia_price_history' incaslib='public' quiet;
  droptable casdata='ia_price' incaslib='public' quiet;  
run;

/****** 3. Добавляем лаги ******/
proc casutil;
  droptable casdata='lag_abt1' incaslib='public' quiet;
  droptable casdata='lag_abt2' incaslib='public' quiet;
  droptable casdata='lag_abt3' incaslib='public' quiet;
  droptable casdata='abt3_ml' incaslib='public' quiet;
run;


/* Макрос разворачивает переменную var в список элементов массива, разделенных запятой */
/* var[t-0],var[t-1],var[t-2],... */
%macro argt(var,index,start,end);
%do ii=&start. %to &end.;
 &var.[&index.-&ii.]
 %if &ii. ne &end. %then %do;
  ,
 %end;
%end;
%mend argt;

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
		name ='abt2_ml',
		caslib = 'public', 
        groupBy = {
			{name = 'PRODUCT_ID'},
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
		    lag_&intnm._avg[t]=mean(%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
		    lag_&intnm._med[t]=median(%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
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
		name ='abt2_ml',
		caslib = 'public', 
        groupBy = {
			{name = 'PRODUCT_ID'},
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
		    lag_&intnm._std[t]=std(%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
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
		name ='abt2_ml',
		caslib = 'public', 
        groupBy = {
			{name = 'PRODUCT_ID'},
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
			lag_&intnm._pct10[t]=pctl(10,%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
   			lag_&intnm._pct90[t]=pctl(90,%argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
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
	create table public.abt3_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			public.abt2_ml as t1,
			public.lag_abt1 as t2
		where
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT = t2.SALES_DT
	;
quit;

proc fedsql sessref=casauto;
	create table public.abt3_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			public.abt3_ml as t1,
			public.lag_abt2 as t2
		where
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT = t2.SALES_DT
	;
quit;

proc fedsql sessref=casauto;
	create table public.abt3_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			public.abt3_ml as t1,
			public.lag_abt3 as t2
		where
			t1.CHANNEL_CD = t2.CHANNEL_CD and
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
			t1.PRODUCT_ID = t2.PRODUCT_ID and
			t1.SALES_DT = t2.SALES_DT
	;
quit;

proc casutil;
  promote casdata="abt3_ml" incaslib="public" outcaslib="public";
  droptable casdata='lag_abt1' incaslib='public' quiet;
  droptable casdata='lag_abt2' incaslib='public' quiet;
  droptable casdata='lag_abt3' incaslib='public' quiet;
run;


/***** 4. Добавляем промо *****/
proc casutil;
	load data=&inlib..ia_pbo_loc_hierarchy casout='ia_pbo_loc_hierarchy' outcaslib='public' replace;
	load data=&inlib..ia_product_hierarchy casout='ia_product_hierarchy' outcaslib='public' replace;
	load data=&inlib..ia_promo casout='ia_promo' outcaslib='public' replace;
	load data=&inlib..ia_promo_x_pbo casout='ia_promo_x_pbo' outcaslib='public' replace;	
	load data=&inlib..ia_promo_x_product casout='ia_promo_x_product' outcaslib='public' replace;

	droptable casdata="pbo_hier_flat" incaslib="public" quiet;
	droptable casdata="product_hier_flat" incaslib="public" quiet;
	droptable casdata="lvl5" incaslib="public" quiet;
	droptable casdata="lvl4" incaslib="public" quiet;
	droptable casdata="lvl3" incaslib="public" quiet;
	droptable casdata="lvl2" incaslib="public" quiet;
	droptable casdata="lvl1" incaslib="public" quiet;
	droptable casdata="pbo_lvl_all" incaslib="public" quiet;
	droptable casdata="product_lvl_all" incaslib="public" quiet;
  	droptable casdata="promo_ml" incaslib="public" quiet;
  	droptable casdata="promo_transposed" incaslib="public" quiet;
  	droptable casdata="abt4_ml" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
  	droptable casdata="promo_ml_main_code" incaslib="public" quiet;
run;

/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
proc fedsql sessref=casauto;
	create table public.pbo_hier_flat{options replace=true} as
		select
			t1.pbo_location_id, 
			t2.PBO_LOCATION_ID as LVL3_ID,
			t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
			1 as LVL1_ID
		from 
			(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
		left join 
			(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
		on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
	;
	create table public.lvl4{options replace=true} as 
		select 
			pbo_location_id as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl3{options replace=true} as 
		select 
			LVL3_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl2{options replace=true} as 
		select 
			LVL2_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl1{options replace=true} as 
		select 
			1 as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
quit;

/* Соединяем в единый справочник ПБО */
data public.pbo_lvl_all;
	set public.lvl4 public.lvl3 public.lvl2 public.lvl1;
run;

/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
proc fedsql sessref=casauto;
   create table public.product_hier_flat{options replace=true} as
		select t1.product_id, 
			   t2.product_id  as LVL4_ID,
			   t3.product_id  as LVL3_ID,
			   t3.PARENT_product_id as LVL2_ID, 
			   1 as LVL1_ID
		from 
		(select * from public.ia_product_hierarchy where product_lvl=5) as t1
		left join 
		(select * from public.ia_product_hierarchy where product_lvl=4) as t2
		on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
		left join 
		(select * from public.ia_product_hierarchy where product_lvl=3) as t3
		on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
 	;
	create table public.lvl5{options replace=true} as 
		select 
			product_id as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl4{options replace=true} as 
		select 
			LVL4_ID as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl3{options replace=true} as 
		select 
			LVL3_ID as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl2{options replace=true} as 
		select 
			LVL2_ID as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl1{options replace=true} as 
		select 
			1 as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
quit;

/* Соединяем в единый справочник ПБО */
data public.product_lvl_all;
	set public.lvl5 public.lvl4 public.lvl3 public.lvl2 public.lvl1;
run;

/* Добавляем к таблице промо ПБО и товары */
proc fedsql sessref = casauto;
	create table public.ia_promo_x_pbo_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t2.PBO_LEAF_ID
		from
			public.ia_promo_x_pbo as t1,
			public.pbo_lvl_all as t2
		where t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table public.ia_promo_x_product_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t2.product_LEAF_ID
		from
			public.ia_promo_x_product as t1,
			public.product_lvl_all as t2
		where t1.product_id = t2.product_id
	;

	create table public.promo_ml{options replace = true} as 
		select
			t1.PROMO_ID,
			t3.product_LEAF_ID,
			t2.PBO_LEAF_ID,
			t1.PROMO_NM,
			t1.PROMO_PRICE_AMT,
			t1.START_DT,
			t1.END_DT,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_MECHANICS,
			(case
				when t1.PROMO_MECHANICS = 'BOGO / 1+1' then 'bogo'
				when t1.PROMO_MECHANICS = 'Discount' then 'discount'
				when t1.PROMO_MECHANICS = 'EVM/Set' then 'evm_set'
				when t1.PROMO_MECHANICS = 'General' then 'general'
				when t1.PROMO_MECHANICS = 'Non-Product Gift' then 'non_product_gift'
				when t1.PROMO_MECHANICS = 'Pairs' then 'pairs'
				when t1.PROMO_MECHANICS = 'Product Gift' then 'product_gift'
			end) as promo_mechanics_name,
			1 as promo_flag		
		from
			public.ia_promo as t1 
		left join
			public.ia_promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		left join
			public.ia_promo_x_product_leaf as t3
		on
			t1.PROMO_ID = t3.PROMO_ID 
	;
	
	/* Добавляем ID регулярного товара */
	/* 
		Тут есть тонкий момент: side_promo_flag должен говорить модели:	 
			обрати внимание, есть другой (такой же) sku с промо, который отъедает
			продажи. Ожидается, что модель будет занижать прогноз, увидев данный флаг.
			Но вот в механике bogo, акция как-бы на два товара (например, регулярный
			и нерегулярный). Что делать в таком случае? Пока что у регулярного
			товара будет выставлен флаг, несмотря на то, что он как бы в промо.  
	*/
	create table public.promo_ml_main_code{options replace = true} as 
		select
			t1.PROMO_ID,
			t1.product_LEAF_ID,
			(MOD(t2.LVL4_ID, 10000)) AS product_MAIN_CODE,
			t1.PBO_LEAF_ID,
			t1.PROMO_NM,
			t1.START_DT,
			t1.END_DT,
			t1.CHANNEL_CD,
			t1.PROMO_MECHANICS,
			t1.promo_mechanics_name,
			t1.promo_flag,
			case
				when product_LEAF_ID = MOD(t2.LVL4_ID, 10000) then 0
				else 1
			end as side_promo_flag
				
		from
			public.promo_ml as t1 
		left join
			public.public.product_hier_flat as t2
		on 
			t1.product_LEAF_ID = t2.product_id
	;
quit;

/* транспонируем таблицу с промо по типам промо механк */
proc cas;
transpose.transpose /
	table = {
		name="promo_ml",
		caslib="public",
		groupby={"promo_id", "product_LEAF_ID", "PBO_LEAF_ID", "CHANNEL_CD", "START_DT", "END_DT"}}
	transpose={"promo_flag"} 
	id={"promo_mechanics_name"} 
	casout={name="promo_transposed", caslib="public", replace=true};
quit;

/* Соединяем с витриной */
proc fedsql sessref = casauto;
	create table public.abt4_ml{options replace = true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			coalesce(t3.side_promo_flag, 0) as side_promo_flag,
			coalesce(t2.bogo,0) as bogo,
			coalesce(t2.discount,0) as discount,
			coalesce(t2.evm_set,0) as evm_set,
			coalesce(t2.non_product_gift,0) as non_product_gift,
			coalesce(t2.pairs,0) as pairs,
			coalesce(t2.product_gift, 0) as product_gift
		from
			public.abt3_ml as t1
		left join
			public.promo_transposed as t2
		on
			t1.product_id = t2.product_LEAF_ID and
			t1.pbo_location_id = t2.PBO_LEAF_ID and
			t1.SALES_DT <= t2.END_DT and
			t1.SALES_DT >= t2.START_DT
		left join
			public.promo_ml_main_code as t3
		on
			t1.product_id = t3.product_MAIN_CODE and
			t1.pbo_location_id = t3.PBO_LEAF_ID and
			t1.SALES_DT <= t3.END_DT and
			t1.SALES_DT >= t3.START_DT
	;
quit;

proc casutil;
	promote casdata="abt4_ml" incaslib="public" outcaslib="public";
	promote casdata="pbo_lvl_all" incaslib="public" outcaslib="public";
	promote casdata="product_lvl_all" incaslib="public" outcaslib="public";

	droptable casdata="pbo_hier_flat" incaslib="public" quiet;
	droptable casdata="product_hier_flat" incaslib="public" quiet;
	droptable casdata="lvl5" incaslib="public" quiet;
	droptable casdata="lvl4" incaslib="public" quiet;
	droptable casdata="lvl3" incaslib="public" quiet;
	droptable casdata="lvl2" incaslib="public" quiet;
	droptable casdata="lvl1" incaslib="public" quiet;
  	droptable casdata="ia_pbo_loc_hierarchy" incaslib="public" quiet;
  	droptable casdata="ia_product_hierarchy" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_product" incaslib="public" quiet;
  	droptable casdata="promo_ml" incaslib="public" quiet;
  	droptable casdata="promo_transposed" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
  	droptable casdata="promo_ml_main_code" incaslib="public" quiet;
run;


/***** 5. Добавляем мароэкономику *****/ 
proc casutil;
  droptable casdata="macro_ml" incaslib="public" quiet;
  droptable casdata="macro2_ml" incaslib="public" quiet;
  droptable casdata="macro_transposed_ml" incaslib="public" quiet;
  droptable casdata="abt5_ml" incaslib="public" quiet;
  load data=&inlib..IA_macro casout='ia_macro' outcaslib='public' replace;
run;

proc fedsql sessref=casauto;
	create table public.macro_ml{options replace=true} as 
		select 
			NAME,
			datepart(cast(REPORT_DT as timestamp)) as period_dt,
			FACTOR_PCT
		from public.ia_macro;
quit;

data public.macro2_ml;
  format period_dt date9.;
  drop pdt;
  set public.macro_ml(rename=(period_dt=pdt));
  by name pdt;
  name=substr(name,1,3);
  period_dt=pdt;
  do until (period_dt>=intnx('day',intnx('month',pdt,3,'b'),0,'b'));
    output;
    period_dt=intnx('day',period_dt,1,'b');
  end;
run;

proc cas;
transpose.transpose /
   table={name="macro2_ml", caslib="public", groupby={"period_dt"}} 
   attributes={{name="period_dt"}} 
   transpose={"factor_pct"} 
   prefix="A_" 
   id={"name"} 
   casout={name="macro_transposed_ml", caslib="public", replace=true};
quit;

/* Соединяем с ABT */
proc fedsql sessref = casauto;
	create table public.abt5_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t2.A_CPI,
			t2.A_GPD,
			t2.A_RDI
		from
			public.abt4_ml as t1 left join 
			public.macro_transposed_ml as t2
		on
			t1.sales_dt = t2.period_dt
	;
quit;

proc casutil;
  droptable casdata="macro_transposed_ml" incaslib="public" quiet;
  droptable casdata="macro2_ml" incaslib="public" quiet;
  droptable casdata="ia_macro" incaslib="public" quiet;
  droptable casdata="macro_ml" incaslib="public" quiet;
  promote casdata="abt5_ml" incaslib="public" outcaslib="public";
run;


/***** 6. Добавляем погоду. *****/
proc casutil;
  load data=&inlib..ia_weather casout = 'ia_weather' outcaslib = 'public' replace;
  droptable casdata = "abt6_ml" incaslib = "public" quiet;
run;

proc fedsql sessref =casauto;
	create table public.abt6_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t2.TEMPERATURE,
			t2.PRECIPITATION
		from
			public.abt5_ml as t1
		left join
			public.ia_weather as t2
		on 
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt = datepart(t2.REPORT_DT)
	;
quit;

proc casutil;
  droptable casdata="ia_weather" incaslib="public" quiet;
  promote casdata="abt6_ml" incaslib="public" outcaslib="public";
run;


/***** 7. Добавляем trp конкурентов *****/
proc casutil;
	droptable casdata="comp_media_ml" incaslib="public" quiet;
	droptable casdata="abt7_ml" incaslib="public" quiet;
	load data=&inlib..IA_comp_media casout='ia_comp_media' outcaslib='public' replace;
run;

proc fedsql sessref=casauto;
	create table public.comp_media_ml{options replace=true} as 
		select
			COMPETITOR_CD,
			TRP,
			datepart(cast(report_dt as timestamp)) as report_dt
		from 
			public.IA_COMP_MEDIA
	;
quit;

/* Транспонируем таблицу */
proc cas;
transpose.transpose /
   table={name="comp_media_ml", caslib="public", groupby={"REPORT_DT"}} 
   transpose={"TRP"} 
   prefix="comp_trp_" 
   id={"COMPETITOR_CD"} 
   casout={name="comp_transposed_ml", caslib="public", replace=true};
quit;

/* Протягиваем trp на всю неделю вперед */
data public.comp_transposed_ml_expand;
	set public.comp_transposed_ml;
	by REPORT_DT;
	do i = 1 to 7;
	   output;
	   REPORT_DT + 1;
	end;
run;

/*
	Пока в данных есть ошибка, все интевалы report_dt указаны
	с интервалом в неделю, но есть одно наблюдение
	в котором этот порядок рушится 16dec2019 и 22dec2019 (6 Дней)
	Поэтому, пока в таблице есть дубль, который мы убираем путем усреднения
*/
proc fedsql sessref=casauto;
	create table public.comp_transposed_ml_expand{options replace=true} as
		select
			REPORT_DT,
			mean(comp_trp_BK) as comp_trp_BK,
			mean(comp_trp_KFC) as comp_trp_KFC
		from
			public.comp_transposed_ml_expand
		group by report_dt
	;
quit;

/* Соединяем с ABT */
proc fedsql sessref = casauto;
	create table public.abt7_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t2.comp_trp_BK,
			t2.comp_trp_KFC
		from
			public.abt6_ml as t1
		left join
			public.comp_transposed_ml_expand as t2
		on
			t1.sales_dt = t2.REPORT_DT
	;
quit;

proc casutil;
	droptable casdata='ia_comp_media' incaslib='public' quiet;
	droptable casdata='comp_media_ml' incaslib='public' quiet;
	droptable casdata='comp_transposed_ml' incaslib='public' quiet;
	droptable casdata='comp_transposed_ml_expand' incaslib='public' quiet;
	promote casdata="abt7_ml" incaslib="public" outcaslib="public";
run;


/***** 8. Добавляем медиаподдержку *****/
proc casutil;
  droptable casdata="media_ml" incaslib="public" quiet;
  droptable casdata="abt8_ml" incaslib="public" quiet;
  load data=&inlib..IA_media casout='ia_media' outcaslib='public' replace;
  load data=&inlib..IA_promo casout='ia_promo' outcaslib='public' replace;
  load data=&inlib..ia_promo_x_product casout='ia_promo_x_product' outcaslib='public' replace;
  load data=&inlib..ia_promo_x_pbo casout='ia_promo_x_pbo' outcaslib='public' replace;
run;

proc fedsql sessref=casauto;
	create table public.ia_promo_x_pbo_leaf{options replace = true} as 
		select
			t1.promo_id,
			t2.PBO_LEAF_ID
		from
			public.ia_promo_x_pbo as t1,
			public.pbo_lvl_all as t2
		where t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table public.ia_promo_x_product_leaf{options replace = true} as 
		select
			t1.promo_id,
			t2.product_LEAF_ID
		from
			public.ia_promo_x_product as t1,
			public.product_lvl_all as t2
		where t1.product_id = t2.product_id
	;
	create table public.promo_ml_trp{options replace = true} as 
		select
			t1.PROMO_ID,
			t3.product_LEAF_ID,
			t2.PBO_LEAF_ID,
			t1.PROMO_NM,
			t1.START_DT,
			t1.END_DT,
			datepart(t4.REPORT_DT) as report_dt,
			t4.TRP
		from
			public.ia_promo as t1 
		left join
			public.ia_promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		left join
			public.ia_promo_x_product_leaf as t3
		on
			t1.PROMO_ID = t3.PROMO_ID
		left join
			public.ia_media as t4
		on
			t1.PROMO_GROUP_ID = t4.PROMO_GROUP_ID
	;
quit;

data public.promo_ml_trp_expand;
	set public.promo_ml_trp;
	do i = 1 to 7;
		output;
		REPORT_DT + 1;
	end;
run;

proc fedsql sessref=casauto;
	create table public.sum_trp{options replace=true} as 
		select
			t1.PRODUCT_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.REPORT_DT,
			sum(t1.trp) as sum_trp
		from
			public.promo_ml_trp_expand as t1
		group by
			t1.PRODUCT_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.report_dt
	;
	create table public.abt8_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t2.sum_trp
		from
			public.abt7_ml as t1
		left join
			public.sum_trp as t2
		on 
			t1.product_id = t2.PRODUCT_LEAF_ID and
			t1.pbo_location_id = t2.PBO_LEAF_ID and
			t1.sales_dt = t2.report_dt
	;
quit;

proc casutil;
  droptable casdata="IA_media" incaslib="public" quiet;
  droptable casdata="IA_promo" incaslib="public" quiet;
  droptable casdata="ia_promo_x_product" incaslib="public" quiet;
  droptable casdata="ia_promo_x_pbo" incaslib="public" quiet;
  droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
  droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  droptable casdata="promo_ml_trp" incaslib="public" quiet;
  droptable casdata="promo_ml_trp_expand" incaslib="public" quiet;
  droptable casdata="sum_trp" incaslib="public" quiet;
  promote casdata="abt8_ml" incaslib="public" outcaslib="public";
run;


/***** 9. Добавим атрибуты магазинов *****/
proc casutil;
  droptable casdata="abt9_ml" incaslib="public" quiet;
  load data=&inlib..IA_pbo_location casout='ia_pbo_location' outcaslib='public' replace;
  load data=&inlib..IA_PBO_LOC_HIERARCHY casout='IA_PBO_LOC_HIERARCHY' outcaslib='public' replace;
  load data=&inlib..IA_PBO_LOC_ATTRIBUTES casout='IA_PBO_LOC_ATTRIBUTES' outcaslib='public' replace;
run;

proc cas;
transpose.transpose /
   table={name="ia_pbo_loc_attributes", caslib="public", groupby={"pbo_location_id"}} 
   attributes={{name="pbo_location_id"}} 
   transpose={"PBO_LOC_ATTR_VALUE"} 
   prefix="A_" 
   id={"PBO_LOC_ATTR_NM"} 
   casout={name="attr_transposed", caslib="public", replace=true};
quit;

proc fedsql sessref=casauto;
   create table public.pbo_hier_flat{options replace=true} as
		select t1.pbo_location_id, 
			   t2.PBO_LOCATION_ID as LVL3_ID,
			   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
			   1 as LVL1_ID
		from 
		(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
		left join 
		(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
		on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
 		;
quit;

proc fedsql sessref=casauto;
	create table public.pbo_dictionary_ml{options replace=true} as
		select 
			t2.pbo_location_id, 
			coalesce(t2.lvl3_id,-999) as lvl3_id,
			coalesce(t2.lvl2_id,-99) as lvl2_id,
			coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
			coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
			coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
			t3.A_AGREEMENT_TYPE,
			t3.A_BREAKFAST,
			t3.A_BUILDING_TYPE,
			t3.A_COMPANY,
			t3.A_DELIVERY,
			t3.A_DRIVE_THRU,
			t3.A_MCCAFE_TYPE,
			t3.A_PRICE_LEVEL,
			t3.A_WINDOW_TYPE
		from 
			public.pbo_hier_flat t2
		left join
			public.attr_transposed t3
		on
			t2.pbo_location_id=t3.pbo_location_id
		left join
			PUBLIC.IA_PBO_LOCATION t14
		on 
			t2.pbo_location_id=t14.pbo_location_id
		left join
			PUBLIC.IA_PBO_LOCATION t13
		on 
			t2.lvl3_id=t13.pbo_location_id
		left join
			PUBLIC.IA_PBO_LOCATION t12
		on
			t2.lvl2_id=t12.pbo_location_id;
quit;

%text_encoding(public.pbo_dictionary_ml, A_AGREEMENT_TYPE)
%text_encoding(public.pbo_dictionary_ml, A_BREAKFAST)
%text_encoding(public.pbo_dictionary_ml, A_BUILDING_TYPE)
%text_encoding(public.pbo_dictionary_ml, A_COMPANY)
%text_encoding(public.pbo_dictionary_ml, A_DELIVERY)
%text_encoding(public.pbo_dictionary_ml, A_MCCAFE_TYPE)
%text_encoding(public.pbo_dictionary_ml, A_PRICE_LEVEL)
%text_encoding(public.pbo_dictionary_ml, A_DRIVE_THRU)
%text_encoding(public.pbo_dictionary_ml, A_WINDOW_TYPE)

proc fedsql sessref=casauto;
	create table public.abt9_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t2.lvl3_id,
			t2.lvl2_id,
			t2.A_AGREEMENT_TYPE_id as agreement_type,
			t2.A_BREAKFAST_id as breakfast,
			t2.A_BUILDING_TYPE_id as building_type,
			t2.A_COMPANY_id as company,
			t2.A_DELIVERY_id as delivery,
			t2.A_DRIVE_THRU_id as drive_thru,
			t2.A_MCCAFE_TYPE_id as mccafe_type,
			t2.A_PRICE_LEVEL_id as price_level,
			t2.A_WINDOW_TYPE_id as window_type
		from
			public.abt8_ml as t1
		left join
			public.pbo_dictionary_ml as t2
		on
			t1.pbo_location_id = t2.pbo_location_id
	;
quit;

proc casutil;
  droptable casdata='ia_pbo_location' incaslib='public' quiet;
  droptable casdata='IA_PBO_LOC_HIERARCHY' incaslib='public' quiet;
  droptable casdata='IA_PBO_LOC_ATTRIBUTES' incaslib='public' quiet;
  droptable casdata='pbo_hier_flat' incaslib='public' quiet;
  droptable casdata='attr_transposed' incaslib='public' quiet;
  droptable casdata='pbo_dictionary_ml' incaslib='public' quiet;
  promote casdata="abt9_ml" incaslib="public" outcaslib="public";
run;


/***** 10. Добавим события *****/
/* 
	1. ia_events в качестве pbo_location_id использует любой 
	уровень иерархии
	2. Большинство событий - дни городов
	3. Проверить может ли на один магазин день приходится больше одного события?
	4. Названия событий довольно странные, хотелось бы иметь единообразный формат
	5. Одно событие может иметь пересекающиеся интервалы
	Например: PBO_LOCATION_ID=1 and EVENT_ID='RULE-GRADUA' содержит два интервала
		20 июня 2020 - 21 июня 2020 и 19 июня 2020 - 21 июня 2020. Зачем эти дубли?
*/
proc casutil;
  load data=&inlib..ia_events casout='ia_events' outcaslib='public' replace;
  droptable casdata="abt10_ml" incaslib="public" quiet;
  droptable casdata="events_leaf" incaslib="public" quiet;
  droptable casdata="events_leaf_expand" incaslib="public" quiet;
  droptable casdata="events_leaf_expand_no_duplicates" incaslib="public" quiet;
  droptable casdata="event_transposed" incaslib="public" quiet;
run;

/* Укажем события на листовом уровне и разметим дни городов */
proc fedsql sessref = casauto;
	create table public.events_leaf{options replace=true} as
		select
			t2.PBO_LEAF_ID,
			t1.EVENT_ID,
			t1.EVENT_NM,
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			case
				when 
					upcase(t1.event_nm) like '%CITY%' or
					upcase(t1.event_nm) = 'CHUVASHIA DAY' or
					upcase(t1.event_nm) = 'BASHKIRIA DAY' or
					upcase(t1.event_nm) = 'NOVOMOSKOVSK'
				then 'city_day'
				when
					t1.event_nm is missing or
					t1.event_nm = '0'
				then 'valent_day'
				when upcase(t1.event_nm) like '%VDV%' then 'VDVS_DAY'
				else tranwrd(strip(t1.event_nm),' ','_')
			end as event_nm_short,
			1 as event_flag
		from
			public.ia_events as t1
		left join
			public.pbo_lvl_all as t2
		on 
			t1.pbo_location_id = t2.pbo_location_id
	;
quit;

/* Превратим таблицу событий во ВР  */
data public.events_leaf_expand;
	set public.events_leaf;
	format period_dt date9.;
	drop last;
	period_dt = start_dt;
	last = end_dt;
	do while (period_dt <= last);
		output;
		period_dt + 1;
	end;
run;

/* Избавляемся от странных дублей */
proc fedsql sessref = casauto;
	create table public.events_leaf_expand_no_duplicates{options replace=true} as
		select distinct
			PBO_LEAF_ID,
			period_dt,
			EVENT_NM_SHORT,
			EVENT_FLAG
		from
			public.events_leaf_expand;
	;
quit;

/* Транспонируем таблицу */
proc cas;
transpose.transpose /
   table={name="events_leaf_expand_no_duplicates", caslib="public", groupby={"PBO_LEAF_ID", "period_dt"}}
   transpose={"event_flag"} 
   prefix="a_"
   id={"event_nm_short"} 
   casout={name="event_transposed", caslib="public", replace=true};
run;

/* Соединяем результат с ABT  витриной */
proc fedsql sessref=casauto;
	create table public.abt10_ml{options replace=true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			coalesce(t2.a_1ST_OF_SEPTEMBER, 0) as SEPTEMBER1,
			coalesce(t2.a_23rd_February, 0) as February23,
			coalesce(t2.a_4TH_OF_NOVEMBER, 0) as NOVEMBER4,
			coalesce(t2.a_8th_of_March, 0) as March8,
			coalesce(t2.a_AUTUMN_SCHOOL_HOLIDAYS, 0) as AUTUMN_SCHOOL_HOLIDAYS,
			coalesce(t2.a_BLACK_FRIDAY, 0) as BLACK_FRIDAY,
			coalesce(t2.a_CHILD_CARE_DAY, 0) as CHILD_CARE_DAY,
			coalesce(t2.a_EASTER, 0) as EASTER,
			coalesce(t2.a_METALLURGIST_DAY, 0) as METALLURGIST_DAY,
			coalesce(t2.a_MUSEUM_NIGHT, 0) as MUSEUM_NIGHT,
			coalesce(t2.a_RUSSIA_DAY, 0) as RUSSIA_DAY,
			coalesce(t2.a_RUSSIA_YOUTH_DAY, 0) as RUSSIA_YOUTH_DAY,
			coalesce(t2.a_SCARLETSAILS, 0) as SCARLETSAILS,
			coalesce(t2.a_SCHOOL_GRADUATE_EVENING, 0) as SCHOOL_GRADUATE_EVENING,
			coalesce(t2.a_SCHOOL_LAST_BELL, 0) as SCHOOL_LAST_BELL,
			coalesce(t2.a_SPACEMEN_DAY, 0) as SPACEMEN_DAY,
			coalesce(t2.a_SPRING_SCHOOL_HOLIDAYS, 0) as SPRING_SCHOOL_HOLIDAYS,
			coalesce(t2.a_VDVS_DAY, 0) as VDVS_DAY,
			coalesce(t2.a_WINTER_SCHOOL_HOLIDAYS, 0) as WINTER_SCHOOL_HOLIDAYS,
			coalesce(t2.a_city_day, 0) as city_day,
			coalesce(t2.a_valent_day, 0) as valent_day
		from
			public.abt9_ml as t1
		left join
			public.event_transposed as t2
		on
			t1.pbo_location_id = t2.pbo_leaf_id and
			t1.sales_dt = t2.period_dt
	;
quit;

proc casutil;
  droptable casdata="ia_events" incaslib="public" quiet;
  droptable casdata="events_leaf" incaslib="public" quiet;
  droptable casdata="events_leaf_expand" incaslib="public" quiet;
  droptable casdata="events_leaf_expand_no_duplicates" incaslib="public" quiet;
  droptable casdata="event_transposed" incaslib="public" quiet;
  promote casdata="abt10_ml" incaslib="public" outcaslib="public";
run;


/***** 11. Добавим календарные признаки *****/
proc casutil;
  droptable casdata="abt11_ml" incaslib="public" quiet;
run;

%let first_date = '01jan2017'd;
%let last_date = '31dec2021'd;

data work.cldr_prep;
	retain date &first_date;
	do while(date <= &last_date);
		output;
		date + 1;		
	end;
	format date ddmmyy10.;
run;

proc sql;
	create table work.cldr_prep_features as 
		select
			date, 
			week(date) as week,
			weekday(date) as weekday,
			month(date) as month,
			(case
				when weekday(date) in (1, 7) then 1
				else 0
			end) as weekend_flag
		from
			work.cldr_prep
	;
quit;

/* Список выходных дней в РФ с 2017 по 2021 */
data work.russia_weekend;
input date :yymmdd10.;
format date yymmddd10.;
datalines;
2017-01-02
2017-01-03
2017-01-04
2017-01-05
2017-01-06
2017-02-23
2017-02-24
2017-03-08
2017-05-01
2017-05-08
2017-05-09
2017-06-12
2017-11-06
2018-01-01
2018-01-02
2018-01-03
2018-01-04
2018-01-05
2018-01-08
2018-02-23
2018-03-08
2018-03-09
2018-04-30
2018-05-01
2018-05-02
2018-05-09
2018-06-11
2018-06-12
2018-11-05
2018-12-31
2019-01-01
2019-01-02
2019-01-03
2019-01-04
2019-01-07
2019-01-08
2019-03-08
2019-05-01
2019-05-02
2019-05-03
2019-05-09
2019-05-10
2019-06-12
2019-11-04
2020-01-01
2020-01-02
2020-01-03
2020-01-06
2020-01-07
2020-01-08
2020-02-24
2020-03-09
2020-05-01
2020-05-04
2020-05-05
2020-05-11
2020-06-12
2020-11-04
2021-01-01
2021-01-04
2021-01-05
2021-01-06
2021-01-07
2021-01-08
2021-02-23
2021-03-08
2021-05-03
2021-05-10
2021-06-14
2021-11-04
;
run;

/* Объединяем государственные выходные с субботой и воскресеньем */
proc sql;
	create table work.cldr_prep_features2 as 
		select
			t1.date,
			t1.week,
			t1.weekday,
			t1.month,
			case
				when t2.date is not missing then 1
				else t1.weekend_flag
			end as weekend_flag
		from
			work.cldr_prep_features as t1
		left join
			work.russia_weekend as t2
		on
			t1.date = t2.date
	;
quit;


proc casutil;
  load data=work.cldr_prep_features2 casout='cldr_prep_features' outcaslib='public' replace;
run;

proc fedsql sessref = casauto;
	create table public.abt11_ml{options replace = true} as
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.SEPTEMBER1,
			t1.February23,
			t1.NOVEMBER4,
			t1.March8,
			t1.AUTUMN_SCHOOL_HOLIDAYS,
			t1.BLACK_FRIDAY,
			t1.CHILD_CARE_DAY,
			t1.EASTER,
			t1.METALLURGIST_DAY,
			t1.MUSEUM_NIGHT,
			t1.RUSSIA_DAY,
			t1.RUSSIA_YOUTH_DAY,
			t1.SCARLETSAILS,
			t1.SCHOOL_GRADUATE_EVENING,
			t1.SCHOOL_LAST_BELL,
			t1.SPACEMEN_DAY,
			t1.SPRING_SCHOOL_HOLIDAYS,
			t1.VDVS_DAY,
			t1.WINTER_SCHOOL_HOLIDAYS,
			t1.city_day,
			t1.valent_day,
			t2.week, 
			t2.weekday,
			t2.month,
			t2.weekend_flag
		from
			public.abt10_ml as t1
		left join
			public.cldr_prep_features as t2
		on
			t1.sales_dt = t2.date
	;
quit;

proc casutil;
  promote casdata="abt11_ml" incaslib="public" outcaslib="public";
run;

proc datasets nolist nowarn;
	delete cldr_prep cldr_prep_features;
run;


/***** 12.Добавляем ценовые ранги *****/
proc casutil;
	droptable casdata="abt12_ml" incaslib="public" quiet;
	droptable casdata="unique_day_price" incaslib="public" quiet;
	droptable casdata="sum_count_price" incaslib="public" quiet;
	droptable casdata="price_rank" incaslib="public" quiet;
	droptable casdata="price_rank2" incaslib="public" quiet;
run;

/* уникальные ПБО/день/категория товаров/товар/цена */
proc fedsql sessref = casauto;
	create table public.unique_day_price as 
		select distinct
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt,
			t1.product_id,
			t1.GROSS_PRICE_AMT
		from
			public.abt11_ml as t1
	;
quit;

/* Считаем суммарную цену в групе и количество товаров */
proc fedsql sessref = casauto;
	create table public.sum_count_price{options replace = true} as
		select
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt,
			count(t1.product_id) as count_product,
			sum(t1.GROSS_PRICE_AMT) as sum_gross_price_amt
		from public.unique_day_price as t1
		group by
			t1.pbo_location_id,
			t1.PROD_LVL3_ID,
			t1.sales_dt
	;
quit;

/* считаем позицию товара в отсортированном списке цен */
data public.price_rank;
set public.unique_day_price;
by pbo_location_id sales_dt PROD_LVL3_ID GROSS_PRICE_AMT ;
if first.PROD_LVL3_ID then i = 0;
if GROSS_PRICE_AMT ^= lag(GROSS_PRICE_AMT) then i+1;
run;

proc fedsql sessref = casauto;
	create table public.price_rank2 as
		select
			t1.pbo_location_id,
			t1.sales_dt,
			t1.PROD_LVL3_ID,
			max(t1.i) as max_i
		from
			public.price_rank as t1
		group by
			t1.pbo_location_id,
			t1.sales_dt,
			t1.PROD_LVL3_ID
	; 
quit;

/* Добавляем в витрину */
proc fedsql sessref = casauto;
	create table public.abt12_ml as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.SEPTEMBER1,
			t1.February23,
			t1.NOVEMBER4,
			t1.March8,
			t1.AUTUMN_SCHOOL_HOLIDAYS,
			t1.BLACK_FRIDAY,
			t1.CHILD_CARE_DAY,
			t1.EASTER,
			t1.METALLURGIST_DAY,
			t1.MUSEUM_NIGHT,
			t1.RUSSIA_DAY,
			t1.RUSSIA_YOUTH_DAY,
			t1.SCARLETSAILS,
			t1.SCHOOL_GRADUATE_EVENING,
			t1.SCHOOL_LAST_BELL,
			t1.SPACEMEN_DAY,
			t1.SPRING_SCHOOL_HOLIDAYS,
			t1.VDVS_DAY,
			t1.WINTER_SCHOOL_HOLIDAYS,
			t1.city_day,
			t1.valent_day,
			t1.week, 
			t1.weekday,
			t1.month,
			t1.weekend_flag,
			t3.i / t4.max_i as price_rank,
			(case
				when t2.sum_gross_price_amt = t1.GROSS_PRICE_AMT then 1
				else t1.GROSS_PRICE_AMT / ((t2.sum_gross_price_amt - t1.GROSS_PRICE_AMT) / (t2.count_product - 1))
			end) as price_index
		from
			public.abt11_ml as t1
		left join
			public.sum_count_price as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
			t1.sales_dt = t2.sales_dt
		left join
			public.price_rank as t3
		on
			t1.pbo_location_id = t3.pbo_location_id and
			t1.product_id = t3.product_id and
			t1.sales_dt = t3.sales_dt
		left join
			public.price_rank2 as t4
		on
			t1.pbo_location_id = t4.pbo_location_id and
			t1.PROD_LVL3_ID = t4.PROD_LVL3_ID and
			t1.sales_dt = t4.sales_dt
	;
quit;

proc casutil;
	droptable casdata="unique_day_price" incaslib="public" quiet;
	droptable casdata="sum_count_price" incaslib="public" quiet;
	droptable casdata="price_rank" incaslib="public" quiet;
	droptable casdata="price_rank2" incaslib="public" quiet;
	promote casdata="abt12_ml" incaslib="public" outcaslib="public";
run;


/***** n. Убираем дни закрытия ПБО *****/
%let n = 13;
proc casutil;
	droptable casdata="pbo_closed_ml" incaslib="public" quiet;
	droptable casdata="abt&n._ml" incaslib="public" quiet;
	load data=&inlib..ia_pbo_close_period casout='ia_pbo_close_period' outcaslib='public' replace;
run;

/* заполняем пропуски в end_dt */
proc fedsql sessref=casauto;
	create table public.pbo_closed_ml {options replace=true} as
		select 
			PBO_LOCATION_ID,
			datepart(start_dt) as start_dt,
			coalesce(datepart(end_dt), date '2100-01-01') as end_dt,
			CLOSE_PERIOD_DESC
		from
			public.ia_pbo_close_period
	;
quit;

/* Удалаем даты закрытия pbo из abt */
proc fedsql sessref=casauto;
	create table public.abt&n._ml{options replace=true} as
		select 
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.SEPTEMBER1,
			t1.February23,
			t1.NOVEMBER4,
			t1.March8,
			t1.AUTUMN_SCHOOL_HOLIDAYS,
			t1.BLACK_FRIDAY,
			t1.CHILD_CARE_DAY,
			t1.EASTER,
			t1.METALLURGIST_DAY,
			t1.MUSEUM_NIGHT,
			t1.RUSSIA_DAY,
			t1.RUSSIA_YOUTH_DAY,
			t1.SCARLETSAILS,
			t1.SCHOOL_GRADUATE_EVENING,
			t1.SCHOOL_LAST_BELL,
			t1.SPACEMEN_DAY,
			t1.SPRING_SCHOOL_HOLIDAYS,
			t1.VDVS_DAY,
			t1.WINTER_SCHOOL_HOLIDAYS,
			t1.city_day,
			t1.valent_day,
			t1.week, 
			t1.weekday,
			t1.month,
			t1.weekend_flag,
			t1.price_rank,
			t1.price_index
		from
			public.abt%eval(&n. - 1)_ml as t1 left join
			public.pbo_closed_ml as t2
		on
			t1.sales_dt >= start_dt and
			t1.sales_dt <= end_dt and
			t1.pbo_location_id = t2.pbo_location_id
		where
			t2.pbo_location_id is missing
	;
quit;

/* 
	Перекодируем поле Channel_cd.
	Делаем это в самом конце, чтобы до этого было проще
	join-ить таблицы 
*/
%text_encoding(public.abt13_ml, channel_cd)

/* Заменяем текстовое поле на числовое */
proc fedsql sessref = casauto;
	create table public.abt13_ml{options replace=true} as 
		select
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD_id as channel_cd,
			t1.SALES_DT,
			t1.sum_qty,
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.GROSS_PRICE_AMT,
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
			t1.lag_month_pct10	,
			t1.lag_month_pct90	,
			t1.lag_qtr_pct10,	
			t1.lag_qtr_pct90,	
			t1.lag_week_pct10,	
			t1.lag_week_pct90,	
			t1.lag_year_pct10,	
			t1.lag_year_pct90,
			t1.side_promo_flag,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.A_CPI,
			t1.A_GPD,
			t1.A_RDI,
			t1.TEMPERATURE,
			t1.PRECIPITATION,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.lvl3_id,
			t1.lvl2_id,
			t1.agreement_type,
			t1.breakfast,
			t1.building_type,
			t1.company,
			t1.delivery,
			t1.drive_thru,
			t1.mccafe_type,
			t1.price_level,
			t1.window_type,
			t1.SEPTEMBER1,
			t1.February23,
			t1.NOVEMBER4,
			t1.March8,
			t1.AUTUMN_SCHOOL_HOLIDAYS,
			t1.BLACK_FRIDAY,
			t1.CHILD_CARE_DAY,
			t1.EASTER,
			t1.METALLURGIST_DAY,
			t1.MUSEUM_NIGHT,
			t1.RUSSIA_DAY,
			t1.RUSSIA_YOUTH_DAY,
			t1.SCARLETSAILS,
			t1.SCHOOL_GRADUATE_EVENING,
			t1.SCHOOL_LAST_BELL,
			t1.SPACEMEN_DAY,
			t1.SPRING_SCHOOL_HOLIDAYS,
			t1.VDVS_DAY,
			t1.WINTER_SCHOOL_HOLIDAYS,
			t1.city_day,
			t1.valent_day,
			t1.week, 
			t1.weekday,
			t1.month,
			t1.weekend_flag,
			t1.price_rank,
			t1.price_index
		from
			public.abt13_ml as t1
	;
quit;

proc casutil;
  droptable casdata="ia_pbo_close_period" incaslib="public" quiet;
  droptable casdata="pbo_closed_ml" incaslib="public" quiet;
  promote casdata="abt&n._ml" incaslib="public" outcaslib="public";
run;