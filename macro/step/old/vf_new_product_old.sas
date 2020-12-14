%macro vf_new_product_old;
%let filter = t1.channel_cd = 'ALL'; 
option dsoptions=nonote2err;
/***** 1. Создание интервалов продаж *****/
%let lmvReportDttm = %sysfunc(datetime());
/* Фильтруем Lifecycle = N и меняем типы дат */
proc fedsql sessref=casauto;
	create table public.n_product_chain{options replace=true} as
		select
			LIFECYCLE_CD,
			PREDECESSOR_PRODUCT_ID,
			PREDECESSOR_DIM2_ID,
			SUCCESSOR_PRODUCT_ID,
			SUCCESSOR_DIM2_ID,
			datepart(SUCCESSOR_START_DT) as SUCCESSOR_START_DT,
			datepart(PREDECESSOR_END_DT) as PREDECESSOR_END_DT,
			SCALE_FACTOR_PCT
		from
			casuser.product_chain
		where
			LIFECYCLE_CD = 'N'
	;
quit;

/* Объединяем интервалы продаж */
data public.n_product_chain_expand;
	set public.n_product_chain;
	keep PREDECESSOR_PRODUCT_ID period_dt;
	format period_dt DATE9.;
	period_dt=SUCCESSOR_START_DT;
	do until (period_dt>=PREDECESSOR_END_DT);
    	output;
    	period_dt=intnx('day',period_dt,1,'b');
  	end;
run;

/* Оставляем уникальные пары товар-дата */
proc fedsql sessref=casauto;
	create table public.n_product_chain_union{options replace=true} as
		select distinct
			PREDECESSOR_PRODUCT_ID,
			period_dt
		from
			public.n_product_chain_expand
	;
quit;

/* Подготоваливаем таблицу для рассчета интервалов продаж */
data public.n_product_chain_union_interval;
	set public.n_product_chain_union;
	format period_dt intnx_lag DATE9.;
	drop intnx_lag perioddt_lag;
	by PREDECESSOR_PRODUCT_ID period_dt;
	perioddt_lag=lag(period_dt);
	if perioddt_lag ne . then 
		intnx_lag = intnx('day',perioddt_lag,1,'b');
	if first.PREDECESSOR_PRODUCT_ID then 
		do;
			i=0;
			output;
		end;
	else
		do;
			if period_dt ^= intnx_lag then
				do;
					i + 1;
					output;
				end;
			else	
				output;
		end;
run;

/* Считаем интервалы продаж */
proc fedsql sessref=casauto;
	create table public.n_product_chain_union_interval2{options replace=true} as
		select
			PREDECESSOR_PRODUCT_ID,
			i,
			min(period_dt) as period_start_dt,
			max(period_dt) as period_end_dt
		from
			public.n_product_chain_union_interval as t1
		group by
			PREDECESSOR_PRODUCT_ID,
			i
	;
quit;

/* Соединяем историю продаж с актуальными значениями */
proc fedsql sessref=casauto; 
	create table public.pmix_sales{options replace=true} as
		select 
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			datepart(t1.SALES_DT) as sales_dt,
			coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0) as sum_qty,
			divide(coalesce(t1.GROSS_SALES_AMT, 0), coalesce(sum(t1.SALES_QTY,t1.SALES_QTY_PROMO), 0)) as price
 from casuser.pmix_sales as t1 
		where
			&filter and
			t1.SALES_DT >= &vf_hist_start_dt and
			t1.SALES_DT <= date %tslit(&vf_hist_end_dt) 
;
quit;

/* Агрегируем таблицу с продажами */
proc fedsql sessref=casauto; 
	create table public.pmix_sales_aggr{options replace=true} as
		select
			channel_cd,
			product_id,
			sales_dt,
			sum(sum_qty) as sum_qty
		from
			public.pmix_sales
		group by
			channel_cd,
			product_id,
			sales_dt			
	;
quit;

/* Соединяем объединенный PLM с агрегированными продажами */
proc fedsql sessref=casauto; 
	create table public.pmix_plm{options replace=true} as
		select
			t1.channel_cd,
			t1.product_id,
			t1.sales_dt,
			t1.sum_qty,
			t2.period_start_dt,
			t2.period_end_dt
		from
			public.pmix_sales_aggr as t1
		inner join
			public.n_product_chain_union_interval2 as t2
		on
			t1.product_id = t2.PREDECESSOR_PRODUCT_ID and
			t1.sales_dt <= t2.period_end_dt and
			t1.sales_dt >= t2.period_start_dt
	;
quit;

/* Считаем настощую дату старта (когда продажы ненулевые) */
proc fedsql sessref=casauto; 
	create table public.pmix_plm2{options replace=true} as
		select
			channel_cd,
			product_id,
			period_start_dt,
			period_end_dt,
			min(sales_dt) as real_start_dt
		from
			public.pmix_plm as t1
		where
			sum_qty > 0 
		group by
			channel_cd,
			product_id,
			period_start_dt,
			period_end_dt		
	;
quit;

/* Оставляем только новые товары (начало не 2 января 2017) */
proc fedsql sessref=casauto; 
	create table public.pmix_plm3{options replace=true} as
		select
			'N' as lifecycle_cd,
			channel_cd,
			product_id,
			real_start_dt as period_start_dt,
			period_end_dt
		from
			public.pmix_plm2
		where
			real_start_dt ^= &VF_HIST_START_DT.
	;
quit;

/* Убираем короткие интервалы */
proc fedsql sessref=casauto; 
	create table public.new_product_interval{options replace=true} as
		select
			lifecycle_cd,
			channel_cd,
			product_id,
			period_start_dt,
			period_end_dt
		from
			public.pmix_plm3
		where
			(period_end_dt - period_start_dt) > 1
	;
quit;

proc casutil;
  droptable casdata="n_product_chain" incaslib="public" quiet;
  droptable casdata="n_product_chain_expand" incaslib="public" quiet;
  droptable casdata="n_product_chain_union" incaslib="public" quiet;
  droptable casdata="n_product_chain_union_interval" incaslib="public" quiet;
  droptable casdata="n_product_chain_union_interval2" incaslib="public" quiet;
  droptable casdata="pmix_sales_aggr" incaslib="public" quiet;
  droptable casdata="pmix_plm" incaslib="public" quiet;
  droptable casdata="pmix_plm2" incaslib="public" quiet;
  droptable casdata="pmix_plm3" incaslib="public" quiet;
	
 * promote casdata="new_product_interval" incaslib="public" outcaslib="public";

run;


/****** 2. Собираем обучающую выборку ******/
proc casutil;
	droptable casdata="real_end_interval" incaslib="public" quiet;
	droptable casdata="new_product_train" incaslib="public" quiet;
run;

/* Отрезаем интервал продаж последней датой истории или годом */
proc fedsql sessref=casauto;
	create table public.real_end_interval{options replace=true} as
		select
			lifecycle_cd,
			channel_cd,
			product_id,
			period_start_dt,
			period_end_dt,
			cast((case
				when (period_start_dt <=  date %tslit(&vf_hist_end_dt)) and (date %tslit(&vf_hist_end_dt) - period_start_dt) <= 365 then min(date %tslit(&vf_hist_end_dt), period_end_dt)
				when (date %tslit(&vf_hist_end_dt) - period_start_dt) > 365 then min(period_start_dt + 365, period_end_dt)
			end) as date) as real_end_dt 
		from
			public.new_product_interval	
	;
quit;

/* Добавляем продажи */
proc fedsql sessref=casauto;
	create table public.new_product_train{options replace=true} as
		select
			t1.CHANNEL_CD,
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.SALES_DT,
			t1.sum_qty,
			t1.price,
			t2.lifecycle_cd,
			t2.period_start_dt,
			t2.real_end_dt
		from
			public.pmix_sales as t1
		inner join
			public.real_end_interval as t2
		on
			t1.channel_cd = t2.channel_cd and
			t1.product_id = t2.product_id and
			t1.sales_dt <= t2.real_end_dt and
			t1.sales_dt >= t2.period_start_dt
	;
quit;

proc casutil;
	droptable casdata="real_end_interval" incaslib="public" quiet;
run;


/****** 3. Добаляем коэффициенты недельного профиля к обучающей выборке *******/

/* 3.2 Собираем коэффициенты для недельного профиля */
proc casutil;
	droptable casdata="npf_frame" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile3" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile4" incaslib="public" quiet;
run;

/* Создаем каркас */
proc fedsql sessref=casauto;
	create table public.npf_frame{options replace=true} as
		select
			t2.PROD_LVL2_ID,
			t1.month,
			t1.weekday
		from
			(select distinct month(sales_dt), weekday(sales_dt) from public.pmix_sales) as t1,
			(select distinct PROD_LVL2_ID from casuser.product_dictionary) as t2	
	;
quit;

/* Добавляем категории товаров и считаем недельный профиль */
proc fedsql sessref=casauto;
	create table public.pmix_sales2{options replace=true} as
		select
			*, month(sales_dt) as month, weekday(sales_dt) as weekday
		from
			public.pmix_sales
	;
	create table public.npf_weekday_mean{options replace=true} as
		select
			t2.PROD_LVL2_ID,
			t1.month,
			t1.weekday,
			mean(t1.sum_qty) as weekday_mean_sum_qty
		from 
			public.pmix_sales2 as t1
		inner join
			casuser.product_dictionary as t2
		on
			t1.product_id = t2.product_id
		group by
			t2.PROD_LVL2_ID,
			t1.month,
			t1.weekday
	;
	create table public.npf_weekday_profile{options replace=true} as
		select
			t2.PROD_LVL2_ID,
			t2.month,
			t2.weekday,
			divide(t2.weekday_mean_sum_qty, t1.weekday_profile) as weekday_profile
		from (
			select
				t1.PROD_LVL2_ID,
				t1.month,
				sum(t1.weekday_mean_sum_qty) as weekday_profile			
			from
				public.npf_weekday_mean as t1
			group by
				t1.PROD_LVL2_ID,
				t1.month
		) as t1
		inner join
			public.npf_weekday_mean as t2
		on
			t1.PROD_LVL2_ID = t2.PROD_LVL2_ID and
			t1.month = t2.month
	;
quit;

/* Не для всех категорий достаточно статистики, чтобы определить недельный профиль */
/* Для таких категорий поставим недельный профиль, независящий от категории продаж */
proc fedsql sessref=casauto;
	create table public.npf_weekday_mean2{options replace=true} as
		select
			t1.month,
			t1.weekday,
			mean(t1.sum_qty) as weekday_mean_sum_qty
		from 
			public.pmix_sales2 as t1
		inner join
			casuser.product_dictionary as t2
		on
			t1.product_id = t2.product_id
		group by
			t1.month,
			t1.weekday
	;
	create table public.npf_weekday_profile2{options replace=true} as
		select
			t2.month,
			t2.weekday,
			divide(t2.weekday_mean_sum_qty, t1.weekday_profile) as weekday_profile
		from (
			select
				t1.month,
				sum(t1.weekday_mean_sum_qty) as weekday_profile			
			from
				public.npf_weekday_mean2 as t1
			group by
				t1.month
		) as t1
		inner join
			public.npf_weekday_mean2 as t2
		on
			t1.month = t2.month
	;
quit;

/* Ставим флаг категориям, имеющим недостаточно статистики */
proc fedsql sessref=casauto;
	create table public.npf_weekday_profile3{options replace=true} as
		select
			t2.PROD_LVL2_ID,
			t2.month,
			t2.weekday,
			t2.weekday_profile,
			t1.low_stat_category
		from (
			select
				PROD_LVL2_ID,
				(case when count(1) < 7*12 then 1 else 0 end) as low_stat_category
			from
				public.npf_weekday_profile
			group by
				PROD_LVL2_ID
			) as t1
		inner join
			public.npf_weekday_profile as t2
		on
			t1.PROD_LVL2_ID = t2.PROD_LVL2_ID
	;
quit;

/* Объединяем таблицы */
proc fedsql sessref=casauto;
	create table public.npf_weekday_profile4{options replace=true} as 
		select distinct
			t1.PROD_LVL2_ID,
			t1.month,
			t1.weekday,
			(case
				when coalesce(t3.low_stat_category, 1) = 1 then t2.weekday_profile
				else t3.weekday_profile
			end) as weekday_profile
		from
			public.npf_frame as t1
		left join
			public.npf_weekday_profile2 as t2
		on
			t1.month = t2.month and
			t1.weekday = t2.weekday
		left join
			public.npf_weekday_profile3 as t3
		on
			t1.month = t3.month and
			t1.weekday = t3.weekday and
			t1.PROD_LVL2_ID = t3.PROD_LVL2_ID	
	;
quit;

proc casutil;
	droptable casdata="npf_frame" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile3" incaslib="public" quiet;
run;


/* 3.3 Добавляем посчитанные коэффициенты к обучающей выборке */
/* Добавляем категории товаров, день недели и месяц */
proc casutil;
	droptable casdata="new_product_train2" incaslib="public" quiet;
	droptable casdata="new_product_train3" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	create table public.new_product_train2{options replace=true} as
		select
			t1.CHANNEL_CD,
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.SALES_DT,
			weekday(t1.sales_dt) as weekday,
			month(t1.sales_dt) as month,
			t1.sum_qty,
			t1.price,
			t1.lifecycle_cd,
			t1.period_start_dt,
			t1.real_end_dt,
			t2.PROD_LVL2_ID
		from
			public.new_product_train as t1
		left join
			casuser.product_dictionary as t2
		on
			t1.product_id = t2.product_id
	;
quit;

/* Добаляем коэффициент недельного профиля */
proc fedsql sessref=casauto;
	create table public.new_product_train3{options replace=true} as
		select
			t1.CHANNEL_CD,
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.SALES_DT,
			t1.weekday,
			t1.month,
			t1.sum_qty,
			t1.price,
			t1.lifecycle_cd,
			t1.period_start_dt,
			t1.real_end_dt,
			t1.PROD_LVL2_ID,
			t2.weekday_profile
		from
			public.new_product_train2 as t1
		left join
			public.npf_weekday_profile4 as t2
		on
			t1.PROD_LVL2_ID = t2.PROD_LVL2_ID and
			t1.month = t2.month and
			t1.weekday = t2.weekday
	;
quit;

proc casutil;
	droptable casdata="new_product_train" incaslib="public" quiet;
	droptable casdata="new_product_train2" incaslib="public" quiet;
run;

/****** 3. Соберем скоринговую витрину ******/
proc casutil;
	droptable casdata="last_day_ts" incaslib="public" quiet;
	droptable casdata="future_assort_matrix" incaslib="public" quiet;
	droptable casdata="new_product_scoring" incaslib="public" quiet;
	droptable casdata="existing_product_scoring" incaslib="public" quiet;
	droptable casdata="existing_product_scoring2" incaslib="public" quiet;
	droptable casdata="future_product_scoring" incaslib="public" quiet;
	droptable casdata="new_product_abt" incaslib="public" quiet;

	*load data=&inlib..ia_assort_matrix casout='ia_assort_matrix' outcaslib='public' replace;
run;
/* Определяем временные ряды, закончившиеся в последний день истории */
proc fedsql sessref=casauto;
	create table public.last_day_ts{options replace=true} as
		select
			t1.CHANNEL_CD,
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			. as sum_qty,
			date %tslit(&vf_hist_end_dt) + 1 as sales_dt,
			t1.period_start_dt
		from (
			select distinct
				CHANNEL_CD,
				PBO_LOCATION_ID,
				PRODUCT_ID,
				period_start_dt
			from
				public.new_product_train3
			where
				real_end_dt = date %tslit(&vf_hist_end_dt)
		) as t1
	;
quit;

%let fc_end= %sysfunc(intnx(day,&vf_fc_start_dt_sas, 365),yymmddd10.); /*горизонт прогнозирования модели новых товаров*/

/* Протягиваем временные ряды на год */
proc cas;
timeData.timeSeries result =r /
	series={
		{name="sum_qty", setmiss="MISSING"},
		{name="period_start_dt", setmiss="PREV"}
	}
	tEnd= "&fc_end"
	table={
		caslib="public",
		name="last_day_ts",
		groupby={"PBO_LOCATION_ID","PRODUCT_ID", "CHANNEL_CD"}
	}
	timeId="SALES_DT"
	trimId="LEFT"
	interval="day"
	casOut={caslib="public", name="existing_product_scoring", replace=True}
	;
run;
quit;

/* Берем из product_chain новые товары, начавшие продаваться в будующем */
proc fedsql sessref=casauto;
	create table public.future_product_chain{options replace=true} as
		select
			SUCCESSOR_PRODUCT_ID as product_id,
			SUCCESSOR_DIM2_ID as pbo_location_id,
			datepart(SUCCESSOR_START_DT) as period_start_dt,
			datepart(PREDECESSOR_END_DT) as period_end_dt
		from
			casuser.product_chain as t1
		where
			LIFECYCLE_CD = 'N' and
			datepart(SUCCESSOR_START_DT) > date %tslit(&vf_hist_end_dt)
	;
quit;

/* Продляем эти интервалы */
data public.future_product_scoring;
	set public.future_product_chain;
	drop period_end_dt;
	format sales_dt DATE9.;
	channel_cd = 'ALL';
	do sales_dt = period_start_dt to period_end_dt;
	   output;
	end;
run;

/* Объединяем скоринговую витрину для новых товаров без истории и с историей */
data public.new_product_scoring;
	set public.future_product_scoring public.existing_product_scoring;
run;

/* Убираем дубликаты в скоринговой витрине */
proc fedsql sessref=casauto;
	create table public.new_product_scoring{options replace=true} as
		select
			channel_cd,
			pbo_location_id,
			product_id,
			sales_dt,
			mean(sum_qty) as sum_qty,
			max(PERIOD_START_DT) as PERIOD_START_DT /* Дату старта берем последнюю */
		from
			public.new_product_scoring
		group by
			channel_cd,
			pbo_location_id,
			product_id,
			sales_dt
	;
quit;

/* Объединяем скоринговую витрину с обучающей */
data public.new_product_abt;
set public.new_product_scoring public.new_product_train3;
run;

proc casutil;
	droptable casdata="last_day_ts" incaslib="public" quiet;
	droptable casdata="existing_product_scoring" incaslib="public" quiet;
	droptable casdata="future_product_chain" incaslib="public" quiet;
	droptable casdata="new_product_scoring" incaslib="public" quiet;
run;

/****** 4. Агрегация до недель ******/
proc casutil;
	droptable casdata="week_start" incaslib="public" quiet;
	droptable casdata="week_aggr" incaslib="public" quiet;
run;

/* Преобразуем даты в недели */
proc fedsql sessref=casauto;
	create table public.week_start{options replace=true} as
		select
			channel_cd,
			pbo_location_id,
			product_id,
			PERIOD_START_DT,
			cast(intnx('week.2',PERIOD_START_DT,0,'b') as date)  as start_week,
			cast(intnx('week.2',sales_dt,0,'b') as date)  as week,
			sum_qty,
			sales_dt,
			weekday_profile
		from
			public.new_product_abt
	;
quit;

/* Считем количество дней продаж товара в неделю и суммарные продажи за неделю */
proc fedsql sessref=casauto;
	create table public.week_aggr{options replace=true} as
		select
			channel_cd,
			pbo_location_id,
			product_id,
			PERIOD_START_DT,
			start_week,
			week,
			count(sales_dt) as count_sales_dt,
			sum(sum_qty) as sum_qty,
			sum(weekday_profile) as sum_weekday_profile		
		from
			public.week_start
		group by
			channel_cd,
			pbo_location_id,
			product_id,
			PERIOD_START_DT,
			start_week,
			week
	;
quit;


/****** 6. Исправляем продажи в первую неделю ******/
proc casutil;
	droptable casdata="new_product_abt2" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	create table public.new_product_abt2{options replace=true} as
		select
			channel_cd,
			pbo_location_id,
			product_id,
			start_week,
			week,
			count_sales_dt,
			(case
				when (start_week = week) and (count_sales_dt<7) then divide(1,sum_weekday_profile)*sum_qty
				else sum_qty
			end) as sum_qty_corrected,
			sum_weekday_profile,
			sum_qty
		from
			public.week_aggr
	;
quit;


/****** 7. Считаем количество недель с начала продаж ******/
proc casutil;
	droptable casdata="new_product_abt3" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	create table public.new_product_abt3{options replace=true} as
		select
			channel_cd,
			pbo_location_id,
			product_id,
			start_week,
			week,
			divide((week - start_week), 7) as weeks_from_start,
			count_sales_dt,
			sum_qty,
			sum_qty_corrected,
			sum_weekday_profile		
		from
			public.new_product_abt2
	;
quit;

proc casutil;
	droptable casdata="new_product_abt2" incaslib="public" quiet;
run;

/****** 7. Добавляем атрибуты ресторана ******/
proc casutil;
  droptable casdata="new_product_abt4" incaslib="public" quiet;
run;

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

	data work.unique;
		set public.unique;
	run;

	data work.encoding_&variable.;
		set work.unique;
		&variable._id = _N_;
	run;

	data public.encoding_&variable.;
		set work.encoding_&variable.;
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

%text_encoding(casuser.pbo_dictionary, A_AGREEMENT_TYPE);
%text_encoding(casuser.pbo_dictionary, A_BREAKFAST);
%text_encoding(casuser.pbo_dictionary, A_BUILDING_TYPE);
%text_encoding(casuser.pbo_dictionary, A_COMPANY);
%text_encoding(casuser.pbo_dictionary, A_DELIVERY);
%text_encoding(casuser.pbo_dictionary, A_MCCAFE_TYPE);
%text_encoding(casuser.pbo_dictionary, A_PRICE_LEVEL);
%text_encoding(casuser.pbo_dictionary, A_DRIVE_THRU);
%text_encoding(casuser.pbo_dictionary, A_WINDOW_TYPE);

proc fedsql sessref=casauto;
	create table public.new_product_abt4{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
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
			public.new_product_abt3 as t1
		left join
			casuser.pbo_dictionary as t2
		on
			t1.pbo_location_id = t2.pbo_location_id
	;
quit;

proc casutil;
  droptable casdata="new_product_abt3" incaslib="public" quiet;
run;


/****** 8. Добавляем атрибуты товара ******/
proc casutil;
  droptable casdata="new_product_abt5" incaslib="public" quiet;
  *droptable casdata="product_dictionary_ml" incaslib="public" quiet;
run;
  
%text_encoding(casuser.product_dictionary, a_hero);
%text_encoding(casuser.product_dictionary, a_item_size);
%text_encoding(casuser.product_dictionary, a_offer_type);
%text_encoding(casuser.product_dictionary, a_price_tier);

proc fedsql sessref=casauto;
	create table public.new_product_abt5{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
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
			t2.prod_lvl4_id, 
			t2.prod_lvl3_id,
			t2.prod_lvl2_id,
			t2.a_hero_id as hero,
			t2.a_item_size_id as item_size,
			t2.a_offer_type_id as offer_type,
			t2.a_price_tier_id as price_tier
	from
		public.new_product_abt4 as t1
	left join
		casuser.product_dictionary as t2
	on
		t1.product_id = t2.product_id
	;
quit;

proc casutil;
  droptable casdata='new_product_abt4' incaslib='public' quiet;
run;


/****** 9. Добавляем макроэкономику ******/
proc casutil;
  droptable casdata="macro_ml" incaslib="public" quiet;
  droptable casdata="macro2_ml" incaslib="public" quiet;
  droptable casdata="macro_transposed_ml" incaslib="public" quiet;
  droptable casdata="macro_transposed_ml2" incaslib="public" quiet;
  droptable casdata="new_product_abt6" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	create table public.macro_ml{options replace=true} as 
		select 
			name as factor_cd,
			/*datepart(cast(REPORT_DT as timestamp))*/ period_dt as period_dt,
			factor_pct as FACTOR_CHNG_PCT
		from casuser.macro;
quit;

data public.macro2_ml;
	format period_dt date9.;
	drop pdt;
	set public.macro_ml(rename=(period_dt=pdt));
	by factor_cd pdt;
	factor_cd=substr(factor_cd,1,3);
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
   transpose={"FACTOR_CHNG_PCT"} 
   prefix="A_" 
   id={"factor_cd"} 
   casout={name="macro_transposed_ml", caslib="public", replace=true};
quit;

/* Усредняем макроэкономику по неделям */
proc fedsql sessref=casauto;
	create table public.macro_transposed_ml2{options replace=true} as
		select
			t1.week,
			mean(A_CPI) as CPI,
			mean(A_GPD) as GPD,
			mean(A_RDI) as RDI			
		from (
			select
				cast(intnx('week.2',period_dt,0,'b') as date)  as week,
				A_CPI,
				A_GPD,
				A_RDI
			from
				public.macro_transposed_ml
		) as t1
		group by
			t1.week
	;
quit;

/* Добавляем макроэкономику */
proc fedsql sessref=casauto;
	create table public.new_product_abt6{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,
			t1.sum_qty_corrected,	
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			coalesce(t2.CPI, 0) as CPI,
			coalesce(t2.GPD, 0) as GPD,
			coalesce(t2.RDI, 0) as RDI
	from
		public.new_product_abt5 as t1
	left join
		public.macro_transposed_ml2 as t2
	on
		t1.week = t2.week
	;
quit;

proc casutil;
  droptable casdata="macro2_ml" incaslib="public" quiet;
  droptable casdata="macro_transposed_ml" incaslib="public" quiet;
  droptable casdata="macro_transposed_ml2" incaslib="public" quiet;
  droptable casdata="new_product_abt5" incaslib="public" quiet;
run;


/****** 10. Промо ******/
proc casutil;
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
  	droptable casdata="promo_transposed2" incaslib="public" quiet;
  	droptable casdata="promo_transposed3" incaslib="public" quiet;
  	droptable casdata="promo_transposed4" incaslib="public" quiet;
  	droptable casdata="promo_transposed5" incaslib="public" quiet;
  	droptable casdata="promo_transposed6" incaslib="public" quiet;
  	droptable casdata="new_product_abt7" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
  	droptable casdata="promo_ml_main_code" incaslib="public" quiet;
  	droptable casdata="abt_promo" incaslib="public" quiet;
run;

/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
proc fedsql sessref=casauto;
*	create table public.pbo_hier_flat{options replace=true} as
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
		select distinct
			pbo_location_id as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_dictionary
	;
	create table public.lvl3{options replace=true} as 
		select distinct
			LVL3_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_dictionary
	;
	create table public.lvl2{options replace=true} as 
		select distinct
			LVL2_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_dictionary
	;
	create table public.lvl1{options replace=true} as 
		select 
			1 as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_dictionary
	;
quit;

/* Соединяем в единый справочник ПБО */
data public.pbo_lvl_all;
	set public.lvl4 public.lvl3 public.lvl2 public.lvl1;
run;

/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
proc fedsql sessref=casauto;
   *create table public.product_hier_flat{options replace=true} as
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
		select distinct
			product_id as product_id,
			product_id as product_leaf_id
		from
			casuser.product_dictionary
	;
	create table public.lvl4{options replace=true} as 
		select distinct
			prod_LVL4_ID as product_id,
			product_id as product_leaf_id
		from
			casuser.product_dictionary
	;
	create table public.lvl3{options replace=true} as 
		select distinct
			prod_LVL3_ID as product_id,
			product_id as product_leaf_id
		from
			casuser.product_dictionary
	;
	create table public.lvl2{options replace=true} as 
		select distinct
			prod_LVL2_ID as product_id,
			product_id as product_leaf_id
		from
			casuser.product_dictionary
	;
	create table public.lvl1{options replace=true} as 
		select distinct
			1 as product_id,
			product_id as product_leaf_id
		from
			casuser.product_dictionary
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
			casuser.promo_pbo as t1,
			public.pbo_lvl_all as t2
		where t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table public.ia_promo_x_product_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t2.product_LEAF_ID
		from
			casuser.promo_prod as t1,
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
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_MECHANICS,
			t1.PROMO_GROUP_ID,
			(case
				when t1.PROMO_MECHANICS = 'BOGO / 1+1' then 'bogo'
				when t1.PROMO_MECHANICS = 'Discount' then 'discount'
				when t1.PROMO_MECHANICS = 'EVM/Set' then 'evm_set'
				when t1.PROMO_MECHANICS = 'Non-Product Gift' then 'non_product_gift'
				when t1.PROMO_MECHANICS = 'Pairs' then 'pairs'
				when t1.PROMO_MECHANICS = 'Product Gift' then 'product_gift'
				when t1.PROMO_MECHANICS = 'Other: Discount for volume' then 'other_discount'
				when t1.PROMO_MECHANICS = 'Other: Digital (app)' then 'other_digital'
				when t1.PROMO_MECHANICS = 'NP Promo Support' then 'support'
			end) as promo_mechanics_name,
			1 as promo_flag		
		from
			casuser.promo as t1 
		left join
			public.ia_promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		left join
			public.ia_promo_x_product_leaf as t3
		on
			t1.PROMO_ID = t3.PROMO_ID 
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

proc fedsql sessref=casauto;
	create table public.promo_transposed2{options replace=true} as
		select 
			product_leaf_id,
			pbo_leaf_id,
			channel_cd,
			start_dt,
			end_dt,
			coalesce(bogo, 0) as bogo,
			coalesce(discount, 0) as discount,
			coalesce(evm_set, 0) as evm_set,
			coalesce(non_product_gift, 0) as non_product_gift,
			coalesce(pairs, 0) as pairs,
			coalesce(product_gift, 0) as product_gift,
			coalesce(other_discount, 0) as other_discount,
			coalesce(other_digital, 0) as other_digital,
			coalesce(support, 0) as support
		from
			public.promo_transposed
	;
quit;

data public.promo_transposed3;
	set public.promo_transposed2;
	format sales_dt DATE9.;
	do sales_dt=start_dt to end_dt;
		output;
	end;
run;

/* Считаем суммарный флаг промо */
proc fedsql sessref=casauto;
	create table public.promo_transposed4{options replace=true} as
		select
			t1.*,
			cast(intnx('week.2',t1.sales_dt,0,'b') as date)  as week
		from
			public.promo_transposed3 as t1
	;
quit;

/* Избавляемся от дублей */
proc fedsql sessref=casauto;
	create table public.promo_transposed5{options replace=true} as
		select
			t1.week,
			t1.pbo_leaf_id,
			t1.product_leaf_id,
			sum(t1.bogo) as bogo,
			sum(t1.discount) as discount,
			sum(t1.evm_set) as evm_set,
			sum(t1.non_product_gift) as non_product_gift,
			sum(t1.other_discount) as other_discount,
			sum(t1.other_digital) as other_digital,
			sum(t1.pairs) as pairs,
			sum(t1.product_gift) as product_gift,
			sum(t1.support) as support
		from
			public.promo_transposed4 as t1
		group by
			t1.week,
			t1.pbo_leaf_id,
			t1.product_leaf_id
	;
quit;

/* Усредняем по неделям */
proc fedsql sessref=casauto;
	create table public.promo_transposed6{options replace=true} as
		select	
			t1.pbo_leaf_id,
			t1.product_leaf_id,
			divide(sum(t1.bogo), 7) as bogo,
			divide(sum(t1.discount), 7) as discount,
			divide(sum(t1.evm_set), 7) as evm_set,
			divide(sum(t1.non_product_gift), 7) as non_product_gift,
			divide(sum(t1.other_discount), 7) as other_discount,
			divide(sum(t1.other_digital), 7) as other_digital,
			divide(sum(t1.pairs), 7) as pairs,
			divide(sum(t1.product_gift), 7) as product_gift,
			divide(sum(t1.support), 7) as support
		from public.promo_transposed4 as t1
		group by
			t1.pbo_leaf_id,
			t1.product_leaf_id,
			t1.week
	;
quit;

/* Соединяем с витриной */
proc fedsql sessref=casauto;
	create table public.new_product_abt7{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,
			t1.sum_qty_corrected,	
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.CPI,
			t1.GPD,
			t1.RDI,
			coalesce(t2.bogo, 0) as bogo,
			coalesce(t2.discount, 0) as discount,
			coalesce(t2.evm_set, 0) as evm_set,
			coalesce(t2.non_product_gift, 0) as non_product_gift,
			coalesce(t2.other_discount, 0) as other_discount,
			coalesce(t2.other_digital, 0) as other_digital,
			coalesce(t2.pairs, 0) as pairs,
			coalesce(t2.product_gift, 0) as product_gift,
			coalesce(t2.support, 0) as support
	from
		public.new_product_abt6 as t1
	left join
		public.promo_transposed5 as t2
	on
		t1.product_id = t2.product_leaf_id and
		t1.pbo_location_id = t2.pbo_leaf_id and
		t1.week = t2.week 
	;
quit;

proc casutil;
	droptable casdata="ia_pbo_loc_hierarchy" incaslib="public" quiet;
	droptable casdata="ia_product_hierarchy" incaslib="public" quiet;
	droptable casdata="ia_promo" incaslib="public" quiet;
	droptable casdata="ia_promo_x_pbo" incaslib="public" quiet;
	droptable casdata="ia_promo_x_product" incaslib="public" quiet;	
	droptable casdata="pbo_hier_flat" incaslib="public" quiet;
	droptable casdata="product_hier_flat" incaslib="public" quiet;
	droptable casdata="lvl5" incaslib="public" quiet;
	droptable casdata="lvl4" incaslib="public" quiet;
	droptable casdata="lvl3" incaslib="public" quiet;
	droptable casdata="lvl2" incaslib="public" quiet;
	droptable casdata="lvl1" incaslib="public" quiet;
/* 	droptable casdata="pbo_lvl_all" incaslib="public" quiet; */
/* 	droptable casdata="product_lvl_all" incaslib="public" quiet; */
/*  	droptable casdata="promo_ml" incaslib="public" quiet;*/
  	droptable casdata="promo_transposed" incaslib="public" quiet;
  	droptable casdata="promo_transposed2" incaslib="public" quiet;
  	droptable casdata="promo_transposed3" incaslib="public" quiet;
  	droptable casdata="promo_transposed4" incaslib="public" quiet;
  	droptable casdata="promo_transposed5" incaslib="public" quiet;
  	droptable casdata="promo_transposed6" incaslib="public" quiet;
  	droptable casdata="new_product_abt6" incaslib="public" quiet;
/*   	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet; */
/*   	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet; */
  	droptable casdata="promo_ml_main_code" incaslib="public" quiet;
  	droptable casdata="abt_promo" incaslib="public" quiet;
run;


/****** 11. Погода ******/
proc casutil;
	droptable casdata="ia_weather2" incaslib="public" quiet;
	droptable casdata="new_product_abt8" incaslib="public" quiet;

	*load data=&inlib..ia_weather casout = 'ia_weather' outcaslib = 'public' replace;
run;

data CASUSER.weather (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set etl_ia.weather(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
/* Считаем среднюю температуру в неделю */
proc fedsql sessref=casauto;
	create table public.ia_weather2{options replace=true} as
		select
			t1.pbo_location_id,
			t1.week,
			mean(t1.TEMPERATURE) as TEMPERATURE,
			mean(t1.PRECIPITATION) as PRECIPITATION
		from (
			select
				pbo_location_id,
				intnx('week.2',report_dt,0,'b') as week, 
				TEMPERATURE,
				PRECIPITATION
			from
				casuser.weather
		) as t1
		group by
			t1.pbo_location_id,
			t1.week
	;
quit;

/* Соединяем с витриной */
proc fedsql sessref=casauto;
	create table public.new_product_abt8{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.CPI,
			t1.GPD,
			t1.RDI,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.pairs,
			t1.product_gift,
			t1.support,
			t2.PRECIPITATION,
			t2.TEMPERATURE
	from
		public.new_product_abt7 as t1
	left join
		public.ia_weather2 as t2
	on
	    t1.pbo_location_id = t2.pbo_location_id and
		t1.week = t2.week
	;
quit;

proc casutil;
	droptable casdata="new_product_abt7" incaslib="public" quiet;
	droptable casdata="ia_weather2" incaslib="public" quiet;
*	droptable casdata="ia_weather" incaslib="public" quiet;
run;


/****** 12. TRP конкурентов ******/
proc casutil;
	droptable casdata="comp_media_ml" incaslib="public" quiet;
	droptable casdata="comp_transposed_ml" incaslib="public" quiet;
	droptable casdata="comp_transposed_ml_expand" incaslib="public" quiet;
	droptable casdata="comp_transposed_ml_expand2" incaslib="public" quiet;
	droptable casdata="new_product_abt9" incaslib="public" quiet;
	
	*load data=&inlib..IA_comp_media casout='ia_comp_media' outcaslib='public' replace;
run;

proc fedsql sessref=casauto;
	create table public.comp_media_ml{options replace=true} as 
		select
			COMPETITOR_CD,
			TRP,
			report_dt as report_dt
		from 
			casuser.COMP_MEDIA
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
/* data public.comp_transposed_ml_expand; */
/* 	set public.comp_transposed_ml; */
/* 	by REPORT_DT; */
/* 	do i = 1 to 7; */
/* 	   output; */
/* 	   REPORT_DT + 1; */
/* 	end; */
/* run; */

/*
	Пока в данных есть ошибка, все интевалы report_dt указаны
	с интервалом в неделю, но есть одно наблюдение
	в котором этот порядок рушится 16dec2019 и 22dec2019 (6 Дней)
	Поэтому, пока в таблице есть дубль, который мы убираем путем усреднения
*/
/* proc fedsql sessref=casauto; */
/* 	create table public.comp_transposed_ml_expand{options replace=true} as */
/* 		select */
/* 			REPORT_DT, */
/* 			mean(comp_trp_BK) as comp_trp_BK, */
/* 			mean(comp_trp_KFC) as comp_trp_KFC */
/* 		from */
/* 			public.comp_transposed_ml_expand */
/* 		 */
/* 		group by report_dt */
/* 	; */
/* quit; */
/*  */
/* proc fedsql sessref=casauto; */
/* 	create table public.comp_transposed_ml_expand2{options replace=true} as */
/* 		select */
/* 			t1.week, */
/* 			sum(comp_trp_BK) as comp_trp_BK, */
/* 			sum(comp_trp_KFC) as comp_trp_KFC */
/* 		from ( */
/* 			select */
/* 				cast(intnx('week.2',datepart(report_dt),0,'b') as date)  as week, */
/* 				comp_trp_BK, */
/* 				comp_trp_KFC */
/* 			from */
/* 				public.comp_transposed_ml_expand */
/* 		) as t1 */
/* 		group by */
/* 			t1.week */
/* 	; */
/* quit; */

proc fedsql sessref=casauto;
	create table public.new_product_abt9{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.CPI,
			t1.GPD,
			t1.RDI,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.pairs,
			t1.product_gift,
			t1.support,
			t1.PRECIPITATION,
			t1.TEMPERATURE,
			coalesce(t2.comp_trp_BK, 0) as comp_trp_BK,
			coalesce(t2.comp_trp_KFC, 0) as comp_trp_KFC
	from
		public.new_product_abt8 as t1
	left join
		public.comp_transposed_ml as t2
	on
		t1.week = t2.REPORT_DT
	;
quit;

proc casutil;
	droptable casdata="comp_media_ml" incaslib="public" quiet;
	droptable casdata="comp_transposed_ml" incaslib="public" quiet;
	droptable casdata="comp_transposed_ml_expand" incaslib="public" quiet;
	droptable casdata="comp_transposed_ml_expand2" incaslib="public" quiet;
	droptable casdata="new_product_abt8" incaslib="public" quiet;
	*droptable casdata="IA_comp_media" incaslib="public" quiet;
run;


/****** 13. Медиа поддержка ******/
proc casutil;
  droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
  droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  droptable casdata="promo_ml_trp" incaslib="public" quiet;
  droptable casdata="promo_ml_trp_expand" incaslib="public" quiet;
  droptable casdata="sum_trp" incaslib="public" quiet;
  droptable casdata="sum_trp2" incaslib="public" quiet;
  droptable casdata="new_product_abt10" incaslib="public" quiet;

*  load data=&inlib..IA_media casout='ia_media' outcaslib='public' replace;
*  load data=&inlib..IA_promo casout='ia_promo' outcaslib='public' replace;
*  load data=&inlib..ia_promo_x_product casout='ia_promo_x_product' outcaslib='public' replace;
*  load data=&inlib..ia_promo_x_pbo casout='ia_promo_x_pbo' outcaslib='public' replace;
run;

/* Добавляем trp к таблице промо */
proc fedsql sessref=casauto;
	create table public.promo_ml_trp{options replace = true} as 
		select 
			t1.PROMO_ID,
			t1.product_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.start_dt,
			t1.end_dt,
			mean(t2.TRP) as mean_trp
		from
			public.promo_ml as t1 
		left join
			casuser.media as t2
		on
			t1.PROMO_GROUP_ID = t2.PROMO_GROUP_ID and
			datepart(t2.period_dt) <= t1.end_dt and
			datepart(t2.period_dt) >= t1.start_dt
		group by
			t1.PROMO_ID,
			t1.product_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.start_dt,
			t1.end_dt
	;
quit;

/* Расшиваем по дням */
data public.promo_ml_trp_expand;
	set public.promo_ml_trp;
	do sales_dt=start_dt to end_dt;
		output;
	end;
run;

/* Суммируем TRP от всех промо */
proc fedsql sessref=casauto;
	create table public.promo_ml_trp2{options replace=true} as
		select
			product_LEAF_ID,
			PBO_LEAF_ID,
			sales_dt,
			sum(mean_trp) as sum_trp
		from
			public.promo_ml_trp_expand as t1
		group by
			product_LEAF_ID,
			PBO_LEAF_ID,
			sales_dt			
	;
quit;

/* Добавляем неделю и усредняем по неделям */
proc fedsql sessref=casauto;
	create table public.promo_ml_trp3{options replace=true} as 
		select
			t1.PRODUCT_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.week,
			mean(t1.sum_trp) as sum_trp
		from (
			select
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
				cast(intnx('week.2',t1.sales_dt,0,'b') as date)  as week,
				t1.sum_trp
			from
				public.promo_ml_trp2 as t1
		) as t1
		group by
			t1.PRODUCT_LEAF_ID,
			t1.PBO_LEAF_ID,
			t1.week			
	;
quit;

proc fedsql sessref=casauto;
	create table public.new_product_abt10{options replace=true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
			month(t1.week) as month,
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.CPI,
			t1.GPD,
			t1.RDI,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.pairs,
			t1.product_gift,
			t1.support,
			t1.PRECIPITATION,
			t1.TEMPERATURE,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			coalesce(t2.sum_trp, 0) as sum_trp
	from
		public.new_product_abt9 as t1
	left join
		public.promo_ml_trp3 as t2
	on
		t1.product_id = t2.product_leaf_id and	
		t1.pbo_location_id = t2.pbo_leaf_id and
		t1.week = t2.week
	;
quit;

proc casutil;
	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
	droptable casdata="promo_ml_trp" incaslib="public" quiet;
	droptable casdata="promo_ml_trp_expand" incaslib="public" quiet;
	droptable casdata="sum_trp2" incaslib="public" quiet;
	droptable casdata="sum_trp3" incaslib="public" quiet;
	droptable casdata="IA_media" incaslib="public" quiet;
	droptable casdata="IA_promo" incaslib="public" quiet;
	droptable casdata="ia_promo_x_product" incaslib="public" quiet;
	droptable casdata="ia_promo_x_pbo" incaslib="public" quiet;
	droptable casdata="new_product_abt9" incaslib="public" quiet;

/* 	promote casdata="new_product_abt10" incaslib="public" outcaslib="public"; */
run;

/****** 14. Добавляем информацию о доступной истории продаж товара ******/
proc casutil;
	droptable casdata="new_product_abt11" incaslib="public" quiet;
run;

data public.new_product_abt11;
	set public.new_product_abt10;
	by PRODUCT_ID PBO_LOCATION_ID CHANNEL_CD week;
	array all_row_values{512} _temporary_;
	retain i;
	drop i l_sum_qty;
	if first.CHANNEL_CD then do; /*первое наблюдение, сбрасываем массив и счетчик массива*/
		call missing(of all_row_values{*});
		i=1;
	end;
	l_sum_qty=lag(sum_qty); /*мы считаем агрегат на шаге i по предыдущим известным значениям без текущего*/
	if i=1 then l_sum_qty=.;
	all_row_values{i}=l_sum_qty;
	avg=mean(of all_row_values{*});
	std=std(of all_row_values{*});
	med=median(of all_row_values{*});
	pcnt10=pctl(10,of all_row_values{*});
	pcnt25=pctl(25,of all_row_values{*});
	pcnt75=pctl(75,of all_row_values{*});
	pcnt90=pctl(90,of all_row_values{*});
	min=min(of all_row_values{*});
	max=max(of all_row_values{*});
	range=max-min;
	cv=std/avg;
	i+1;
run;

proc casutil;
	droptable casdata="new_product_abt10" incaslib="public" quiet;
/* 	promote casdata="new_product_abt11" incaslib="public" outcaslib="public"; */
run;

/****** 15. Добавление цены ******/
proc casutil;
  droptable casdata="price_ml" incaslib="public" quiet;
  droptable casdata="price_ml2" incaslib="public" quiet;
  droptable casdata="price_ml3" incaslib="public" quiet;
  droptable casdata="new_product_abt12" incaslib="public" quiet;
run;

data CASUSER.price (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set etl_ia.price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;
	
/* Расшиваем по дням */
data public.price_ml2;
	set casuser.price;
	drop start_dt end_dt;
	do sales_dt=start_dt to end_dt;
		output;
	end;
run;

/* Агрегируем по неделям */
proc fedsql sessref=casauto;
	create table public.price_ml3{options replace=true} as
		select
			t1.pbo_location_id,
			t1.product_id,
			t1.week,
			mean(t1.gross_price_amt) as mean_price
		from (
			select
				*,
				cast(intnx('week.2',sales_dt,0,'b') as date)  as week
			from
				public.price_ml2
		) as t1
		group by
			t1.pbo_location_id,
			t1.product_id,
			t1.week
	;
quit;

/* Добавляем к продажам цены */
proc fedsql sessref=casauto; 
	create table public.new_product_abt12{options replace=true} as 
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
			t1.month,
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.CPI,
			t1.GPD,
			t1.RDI,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.pairs,
			t1.product_gift,
			t1.support,
			t1.PRECIPITATION,
			t1.TEMPERATURE,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.avg,
			t1.std,
			t1.med,
			t1.pcnt10,
			t1.pcnt25,
			t1.pcnt75,
			t1.pcnt90,
			t1.min,
			t1.max,
			t1.range,
			t1.cv,
			t2.mean_price
		from
			public.new_product_abt11 as t1
		left join
			public.price_ml3 as t2
		on
			t1.pbo_location_id = t2.pbo_location_id and
			t1.product_id = t2.product_id and
			t1.week = t2.week
	;
quit;

proc casutil;
  droptable casdata="price_ml" incaslib="public" quiet;
  droptable casdata="price_ml2" incaslib="public" quiet;
  droptable casdata="price_ml3" incaslib="public" quiet;
  droptable casdata="ia_price_history" incaslib="public" quiet;
  droptable casdata="ia_price" incaslib="public" quiet;
  droptable casdata="new_product_abt11" incaslib="public" quiet;

run;


/****** 16. Добавляем рецептуру ******/
proc casutil;
*	load data=&inlib..ia_ingridients casout='ia_ingridients' outcaslib='public' replace;
	droptable casdata="ingridients_transposed" incaslib="public" quiet;
	droptable casdata="ingridients_transposed2" incaslib="public" quiet;
	droptable casdata="ingridients_pca" incaslib="public" quiet;
	droptable casdata="ingridients_pca2" incaslib="public" quiet;
	droptable casdata="ingridients_pca3" incaslib="public" quiet;
	droptable casdata="new_product_abt13" incaslib="public" quiet;
run;

/* Транспонируем рецептуру */
proc cas;
transpose.transpose /
	table = {
		name="ingridients",
		caslib="casuser",
		groupby={"MONTH_DT", "PRODUCT_ID"}
	}
	prefix="item_"
	transpose={"FACTOR"} 
	id={"ASMB_ITEM_ID"} 
	casout={name="ingridients_transposed", caslib="public", replace=true};
quit;

/* Заменяем пропуски нулями */
data public.ingridients_transposed2;
	set public. ingridients_transposed;
	array _xxx_ _numeric_;
	do i=1 to dim( _xxx_);
		if missing(_xxx_[i]) then _xxx_[i]=0;
	end;
	drop i _name_ _label_;
run;

/* Создаем список переменных (за исключением ID) */
proc contents data=public.ingridients_transposed2 noprint out=_contents_;
run;

/* Сохраняем список переменных в макропременные */
proc sql noprint;
    select name into :names separated by ' ' from _contents_ where upcase(name) not in ('PRODUCT_ID', 'MONTH_DT');
    select name into :sql_names separated by ',' from _contents_ where upcase(name) not in ('PRODUCT_ID', 'MONTH_DT');
quit;

ods _all_ close;
/* Выполняем проекцию на меньшее подпространство */
proc pca data=public.ingridients_transposed2 n=30 plots=none;
	var &names.;
	output out=public.ingridients_pca copyvars=(month_dt product_id);
run;

/* Меняем тип datetime на date */
proc fedsql	sessref=casauto;
	create table public.ingridients_pca2{options replace=true} as
		select
			datepart(t2.month_dt) as month_dt,
			t2.product_id,
			t2.Score1,
			t2.Score2,
			t2.Score3,
			t2.Score4,
			t2.Score5,
			t2.Score6,
			t2.Score7,
			t2.Score8,
			t2.Score9,
			t2.Score10,
			t2.Score11,
			t2.Score12,
			t2.Score13,
			t2.Score14,
			t2.Score15,
			t2.Score16,
			t2.Score17,
			t2.Score18,
			t2.Score19,
			t2.Score20,
			t2.Score21,
			t2.Score22,
			t2.Score23,
			t2.Score24,
			t2.Score25,
			t2.Score26,
			t2.Score27,
			t2.Score28,
			t2.Score29,
			t2.Score30
		from
			public.ingridients_pca as t2
	;
quit;

/* Протягиваем рецептру на два года вперед */
proc cas;
timeData.timeSeries result =r /
	series={
		{name="Score1", setmiss="PREV"},
		{name="Score2", setmiss="PREV"},
		{name="Score3", setmiss="PREV"},
		{name="Score4", setmiss="PREV"},
		{name="Score5", setmiss="PREV"},
		{name="Score6", setmiss="PREV"},
		{name="Score7", setmiss="PREV"},
		{name="Score8", setmiss="PREV"},
		{name="Score9", setmiss="PREV"},
		{name="Score10", setmiss="PREV"},
		{name="Score11", setmiss="PREV"},
		{name="Score12", setmiss="PREV"},
		{name="Score13", setmiss="PREV"},
		{name="Score14", setmiss="PREV"},
		{name="Score15", setmiss="PREV"},
		{name="Score16", setmiss="PREV"},
		{name="Score17", setmiss="PREV"},
		{name="Score18", setmiss="PREV"},
		{name="Score19", setmiss="PREV"},
		{name="Score20", setmiss="PREV"},
		{name="Score21", setmiss="PREV"},
		{name="Score22", setmiss="PREV"},
		{name="Score23", setmiss="PREV"},
		{name="Score24", setmiss="PREV"},
		{name="Score25", setmiss="PREV"},
		{name="Score26", setmiss="PREV"},
		{name="Score27", setmiss="PREV"},
		{name="Score28", setmiss="PREV"},
		{name="Score29", setmiss="PREV"},
		{name="Score30", setmiss="PREV"}
	}
	tEnd= "&FC_END"
	table={
		caslib="public",
		name="ingridients_pca2",
		groupby={"PRODUCT_ID"}
	}
	timeId="month_dt"
	trimId="LEFT"
	interval="month"
	casOut={caslib="public", name="ingridients_pca3", replace=True}
	;
run;
quit;

/* Добавляем рецептуру к витрине */
proc fedsql sessref=casauto;
	create table public.new_product_abt13{options replace = true} as
		select
			t1.channel_cd,
			t1.pbo_location_id,
			t1.product_id,
			t1.start_week,
			t1.week,
			t1.weeks_from_start,
			t1.count_sales_dt,
			t1.sum_qty,	
			t1.sum_qty_corrected,
			t1.month,
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
			t1.prod_lvl4_id, 
			t1.prod_lvl3_id,
			t1.prod_lvl2_id,
			t1.hero,
			t1.item_size,
			t1.offer_type,
			t1.price_tier,
			t1.CPI,
			t1.GPD,
			t1.RDI,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.pairs,
			t1.product_gift,
			t1.support,
			t1.PRECIPITATION,
			t1.TEMPERATURE,
			t1.comp_trp_BK,
			t1.comp_trp_KFC,
			t1.sum_trp,
			t1.avg,
			t1.std,
			t1.med,
			t1.pcnt10,
			t1.pcnt25,
			t1.pcnt75,
			t1.pcnt90,
			t1.min,
			t1.max,
			t1.range,
			t1.cv,
			t1.mean_price,
			t2.Score1,
			t2.Score2,
			t2.Score3,
			t2.Score4,
			t2.Score5,
			t2.Score6,
			t2.Score7,
			t2.Score8,
			t2.Score9,
			t2.Score10
		from
			public.new_product_abt12 as t1
		left join
			public.ingridients_pca3 as t2
		on
			t1.product_id = t2.product_id and
			intnx('month',t1.week,0,'BEGINNING') = t2.month_dt
	;
quit;

proc casutil;
	droptable casdata="ia_ingridients" incaslib="public" quiet;
	droptable casdata="ingridients_transposed" incaslib="public" quiet;
	droptable casdata="ingridients_transposed2" incaslib="public" quiet;
	droptable casdata="ingridients_pca" incaslib="public" quiet;
	droptable casdata="ingridients_pca2" incaslib="public" quiet;
	droptable casdata="ingridients_pca3" incaslib="public" quiet;
	droptable casdata="new_product_abt12" incaslib="public" quiet;
	promote casdata="new_product_abt13" incaslib="public" outcaslib="public";
run;


/****** 17. Разделение на обучение и скоринг ******/
proc fedsql sessref=casauto;
	create table public.npf_scoring{options replace=true} as
		select
			t1.*
		from
			public.new_product_abt13 as t1
		where
			t1.week >= &VF_FC_START_DT.
	;
	create table public.npf_train{options replace=true} as
		select
			t1.*
		from
			public.new_product_abt13 as t1
		where
			t1.week < &VF_FC_START_DT.
	;
quit;

/****** 18. Обучение модели ******/
proc casutil;
	droptable casdata="models_npf" incaslib="public" quiet;
run;

%let pmix_default_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=50
     maxdepth=10
     inbagfraction=0.6
     minleafsize=5
     numbin=50
     printtarget
;

proc forest data=public.npf_train
	&pmix_default_params.;
	input 
		weeks_from_start
		mean_price
		CPI
		GPD
		RDI
		bogo
		discount
		evm_set
		non_product_gift
		other_discount
		other_digital
		pairs
		product_gift
		support
		PRECIPITATION
		TEMPERATURE
		comp_trp_BK
		comp_trp_KFC
		sum_trp
		avg
		std
		med
		pcnt10
		pcnt25
		pcnt75
		pcnt90
		min
		max
		range
		cv
		Score1
		Score2
		Score3
		Score4
		Score5
		Score6
		Score7
		Score8
		Score9
		Score10 / level = interval;
	input
/*		pbo_location_id
		product_id*/
		month
		lvl3_id
		lvl2_id
		agreement_type
		breakfast
		building_type
		company
		delivery
		drive_thru
		mccafe_type
		price_level
		window_type
		prod_lvl4_id
		prod_lvl3_id
		prod_lvl2_id
		hero
		item_size
		offer_type
		price_tier / level = nominal;
	id channel_cd week;
	target sum_qty_corrected / level = interval;
	grow VARIANCE;
	savestate rstore=public.models_npf;
run;

proc casutil;
	promote casdata="models_npf" incaslib="public" outcaslib="public";
run;


/****** 19. Скоринг ******/
proc casutil;
    droptable casdata="npf_scoring_pred" incaslib="public" quiet;
run;
proc astore;
  score data=public.npf_scoring
  copyvars=(pbo_location_id  product_id sum_qty_corrected PROD_LVL2_ID month)
  rstore=public.models_npf
  out=public.npf_scoring_pred;
quit;


/****** 20. Деление прогноза по дням ******/
/* 
	Базовая идея посчитать для каждой пары
		верхнеуровневая категория | месяц 
	распрделение продаж и дальше разбить продажи товаров
 	Если товар не попадает ни под какую категорию товаров, то 
	делим его прогноз пропорционально общему объёму продаж mcd
*/
proc casutil;
	droptable casdata="npf_frame" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile3" incaslib="public" quiet;
	*droptable casdata="npf_weekday_profile4" incaslib="public" quiet;
	droptable casdata="npf_scoring_pred_day" incaslib="public" quiet;
run;

/* Делим прогноз по дням */
proc fedsql sessref=casauto;
	create table public.npf_scoring_pred_day{options replace=true} as
		select
			t1.channel_cd,
			t1.PRODUCT_ID,
			t1.PBO_LOCATION_ID,
			t1.PROD_LVL2_ID,
			t1.WEEK,
			t2.weekday,
			(case
				when t2.weekday = 1 then cast((t1.week + 6) as date)
				else cast((t1.week + t2.weekday - 2) as date)
			end) as sales_dt,
			t1.MONTH,
			t1.sum_qty_corrected as week_sum_qty,
			t1.P_sum_qty_corrected as week_p_sum_qty,
			t1.p_sum_qty_corrected * t2.weekday_profile as p_sum_qty
		from
			public.npf_scoring_pred as t1
		left join
			public.npf_weekday_profile4 as t2
		on
			t1.month = t2.month and
			t1.PROD_LVL2_ID = t2.PROD_LVL2_ID 
	;
quit;

/* proc casutil; */
/* 	droptable casdata="npf_weekday_profile4" incaslib="public" quiet; */
/* 	promote casdata="npf_scoring_pred_day" incaslib="public" outcaslib="public"; */
/* 	promote casdata="npf_scoring_pred" incaslib="public" outcaslib="public"; */
/* run; */


/****** 21. Пересекаем с ассортиментной матрицей и product chain ******/
proc casutil;
*	load data=&inlib..ia_assort_matrix casout='ia_assort_matrix' outcaslib='public' replace;
	droptable casdata="npf_prediction" incaslib="public" quiet;
run;

/* Оставляем прогноз на товар,
 если есть прогноз он есть в АМ или есть запись в product chain с lifecycle=N */
proc fedsql sessref=casauto;
	create table public.npf_prediction{options replace=true} as
		select
			t1.channel_cd,
			t1.PRODUCT_ID,
			t1.PBO_LOCATION_ID,
			t1.PROD_LVL2_ID,
			t1.WEEK,
			t1.weekday,
			t1.sales_dt,
			t1.MONTH,
			t1.week_sum_qty,
			t1.week_p_sum_qty,
			t1.p_sum_qty
		from
			public.npf_scoring_pred_day as t1
		left join
			casuser.assort_matrix as t2
		on
			t1.product_id = t2.product_id and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt <= datepart(end_dt) and
			t1.sales_dt >= datepart(start_dt)
		left join
			public.future_product_scoring as t3
		on
			t1.product_id = t3.product_id and
			t1.pbo_location_id = t3.pbo_location_id and
			t1.sales_dt = t3.sales_dt
		where
			t2.product_id is not missing or t3.product_id is not missing
	;
quit;

/* Делим прогноз по дням */
*proc fedsql sessref=casauto;
*	create table public.npf_scoring_pred_day{options replace=true} as
		select
			t1.channel_cd,
			t1.PRODUCT_ID,
			t1.PBO_LOCATION_ID,
			t1.PROD_LVL2_ID,
			t1.WEEK,
			t2.weekday,
			cast(t1.week + 
				case when t2.weekday>1 then
				  t2.weekday - 2
				  else 6 end
				as date) as sales_dt,
			t1.MONTH,
			t1.SUM_QTY as week_sum_qty,
			t1.P_SUM_QTY as week_p_sum_qty,
			t1.p_sum_qty * t2.weekday_profile as p_sum_qty
		from
			public.npf_scoring_pred as t1
		left join
			public.npf_weekday_profile4 as t2
		on
			t1.month = t2.month and
			t1.PROD_LVL2_ID = t2.PROD_LVL2_ID
	;
*quit;

proc casutil;
	droptable casdata="npf_frame" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile" incaslib="public" quiet;
	droptable casdata="npf_weekday_mean2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile2" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile3" incaslib="public" quiet;
	droptable casdata="npf_weekday_profile4" incaslib="public" quiet;
	*promote casdata="npf_scoring_pred_day" incaslib="public" outcaslib="public";
	*promote casdata="npf_scoring_pred" incaslib="public" outcaslib="public";
	promote casdata="npf_prediction" incaslib="public" outcaslib="public";
run;

%mend vf_new_product_old;