/* 
	Цель этой программы применить измененную методику рассчета
	списка сопутсвующих/каннибализируемых товаров.
*/

/****** 0. Объявим макропеременные ******/
options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
/*Создать cas-сессию, если её нет*/
%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto SESSOPTS=(TIMEOUT=31536000);
 caslib _all_ assign;
%end;
%mend;
%assign

libname ETL_STG "/data2/etl_stg_28_08_2020";
libname tmp "/data2/TMP";
%let inlib=ETL_STG;
%let filter = t1.channel_cd = 'ALL'; 
%let hist_start_dt = date '2017-01-02';
%let hist_end_dt =  date '2020-07-26';

/****** Загрузим промо ******/
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
  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
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
		select distinct
			pbo_location_id as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl3{options replace=true} as 
		select distinct
			LVL3_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl2{options replace=true} as 
		select distinct
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
		select distinct
			product_id as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl4{options replace=true} as 
		select distinct
			LVL4_ID as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl3{options replace=true} as 
		select distinct
			LVL3_ID as product_id,
			product_id as product_leaf_id
		from
			public.product_hier_flat
	;
	create table public.lvl2{options replace=true} as 
		select distinct
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
		where
			t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table public.ia_promo_x_product_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t2.product_LEAF_ID
		from
			public.ia_promo_x_product as t1,
			public.product_lvl_all as t2
		where
			t1.product_id = t2.product_id
	;
	create table public.promo_ml{options replace = true} as 
		select
			t1.PROMO_ID,
			t3.product_LEAF_ID as product_id,
			t2.PBO_LEAF_ID as pbo_location_id,
			t1.PROMO_NM,
/* 			t1.PROMO_PRICE_AMT, */
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			t1.CHANNEL_CD,
/* 			t1.NP_GIFT_PRICE_AMT, */
/* 			t1.PROMO_MECHANICS, */
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
				when t1.PROMO_MECHANICS = '1+1%' then 'one_plus_one'
			end) as promo_mechanics_name
/* 			1 as promo_flag		 */
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
		where
			t1.channel_cd = 'ALL'
	;
quit;

/* Оставляем только промо случившиеся c 2019 по 2020 с механикой discount */
proc fedsql sessref=casauto;
	create table public.discount_promo{options replace=true} as
		select
			t1.PROMO_ID,
			t1.product_ID,
			t1.PBO_location_ID,
			t1.PROMO_NM,
			t1.start_dt,
			t1.end_dt,
			t1.CHANNEL_CD,
			t1.promo_mechanics_name
		from
			public.promo_ml as t1
		where
			start_dt >= date '2019-01-01' and
			end_dt <= date '2020-09-30' and
			promo_mechanics_name = 'discount' and
			promo_id ^= 745 /* убираем промо, действующее в интеравале времени */
	;
quit;

proc casutil;
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
  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
run;

/* Выгружаем из cas  таблицу с промо */
data work.discount_promo;
	set public.discount_promo;
run;

/* Меняем ID ресторнов */
proc sql;
	create table work.discount_promo_2 as 
		select
			t1.PROMO_ID,
			t1.product_ID,
			input(t2.PBO_LOC_ATTR_VALUE, best32.) as pbo_location_id,
			t1.PROMO_NM,
			t1.start_dt,
			t1.end_dt,
			t1.CHANNEL_CD,
			t1.promo_mechanics_name
		from
			work.discount_promo as t1
		inner join (
			select distinct
				PBO_LOCATION_ID,
				PBO_LOC_ATTR_VALUE
			from
				etl_stg.ia_pbo_loc_attributes
			where
				PBO_LOC_ATTR_NM='STORE_ID'
		) as t2
		on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
	;
quit;

/* Считаем агрегаты продаж */
proc sql;
	create table work.pmix_aggr as
		select
			t1.product_id,
			t1.sales_dt,
			sum(t1.sales_qty) as sum_qty
		from (
			select
				t1.product_id,
				datepart(t1.sales_dt) as sales_dt format date9.,
				t1.pbo_location_id,
				sum(t1.sales_qty, t1.sales_qty_promo) as sales_qty	
			from
				etl_stg.ia_pmix_sales_history as t1
			where
				t1.channel_cd = 'ALL'
		) as t1
		group by
			t1.product_id,
			t1.sales_dt
	;
quit;


/****** 2. Поиск канибализируемых товаров ******/
%macro calculate_adjacency_matrix(promo);

/* 
	Макрос выдает таблицу c переменными:
		1. ID промо
		2. ID товара
		3. Количество чеков в промо период и товара есть в чеке (только для промо чеков и как бы промо чеков в предшествующий период) (a_in_in)
		4. Количество чеков в промо период и товара нет в чеке (только для промо чеков и как бы промо чеков в предшествующий период) (a_in_out)
		5. Количество чеков не в промо период и товар есть в чеке (только для промо чеков и как бы промо чеков в предшествующий период) (a_out_in)
		6. Количество чеков не в промо период и товара нет в чеке (только для промо чеков и как бы промо чеков в предшествующий период) (a_out_out)
		7. Статистика хи квадрат (только для промо чеков и как бы промо чеков в предшествующий период)
		8. Количество чеков в промо период и товара есть в чеке (только для не промо чеков и как бы не промо чеков в предшествующий период) (b_in_in)
		9. Количество чеков в промо период и товара нет в чеке (только для не промо чеков и как бы не промо чеков в предшествующий период) (b_in_out)
		10. Количество чеков не в промо период и товар есть в чеке (только для не промо чеков и как бы не промо чеков в предшествующий период) (b_out_in)
		11. Количество чеков не в промо период и товара нет в чеке (только для не промо чеков и как бы не промо чеков в предшествующий период) (b_out_out)
		12. Статистика хи квадрат (только для не промо чеков и как бы не промо чеков в предшествующий период)

*/

	/* Стираем временные таблицы */
	proc datasets library=tmp;
		delete discount_promo_filter;
		delete russca_receipt_filter;
		delete russca_receipt_filter_id;
		delete russca_receipt_promo;
		delete russca_receipt_promo2;
		delete russca_kanibal;
		delete russca_kanibal_a;
		delete russca_kanibal_b;
		delete pmix_aggr_filter;
		delete russca_receipt_filter_2019;
		delete russca_receipt_filter_2020;
		delete before_pbo_day_count;
		delete during_pbo_day_count;
		delete pbo_day_count;
	run;
	
	/* Фильтруем промо таблицу */
	proc sql;
		create table tmp.discount_promo_filter as 
			select
				t1.PROMO_ID,
				t1.product_ID,
				t1.pbo_location_id,
				t1.PROMO_NM,
				t1.start_dt,
				t1.end_dt,
				t1.CHANNEL_CD,
				t1.promo_mechanics_name
			from
				work.discount_promo_2 as t1
			where
				promo_id = &promo.
		;
	quit;
	
	/* Получаем даты проведения промо акции + аналогичный период в прошлом */
	proc sql noprint;
		select distinct 
			start_dt,
			end_dt,
			start_dt - (end_dt - start_dt + 1) format date9.,
			start_dt - 1 format date9.
		into :promo_start, :promo_end, :pseudo_promo_start, :pseudo_promo_end
	    from
			tmp.discount_promo_filter;
	quit;
	
	%put promo_start = "&promo_start."d;
	%put promo_end = "&promo_end."d;

	%put pseudo_promo_start = "&pseudo_promo_start."d;
	%put pseudo_promo_end = "&pseudo_promo_end."d;

	/* Меняем формат на числовой */
	%let promo_start_num = %sysfunc(INPUTN(&promo_start., date9.));
	%let promo_end_num = %sysfunc(INPUTN(&promo_end., date9.));
	%let pseudo_promo_start_num = %sysfunc(INPUTN(&pseudo_promo_start., date9.));
	%let pseudo_promo_end_num = %sysfunc(INPUTN(&pseudo_promo_end., date9.));

	/* Фильтруем агрегированные продажи */
	proc sql;
		create table tmp.pmix_aggr_filter as
			select
				t1.product_id,
				sum(t1.sum_qty) as sum_qty
			from
				work.pmix_aggr as t1
			where
				t1.sales_dt <= "&promo_end."d and
				t1.sales_dt >= "&promo_start."d
			group by
				t1.product_id
		;
	quit;
	
	/* Фильтруем чеки 2019 года по датам промо акции */
	%if %eval(&pseudo_promo_start_num. < %sysfunc(INPUTN('1jan2020'd, date9.))) %then %do;
		proc sql;
			create table tmp.russca_receipt_filter_2019 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					tmp.tda_pmx_2019 as t1
				where
					datepart(t1.order_date) <= "&promo_end."d and
					datepart(t1.order_date) >= "&pseudo_promo_start."d 
			;
		quit;
	%end;
	%else %do;
		proc sql;
			create table tmp.russca_receipt_filter_2019 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					tmp.tda_pmx_2019(obs=0) as t1
			;
		quit;
	%end;

	/* Фильтруем чеки 2020 года по датам промо акции */
	%if %eval(&promo_end_num. >= %sysfunc(INPUTN('1jan2020'd, date9.))) %then %do;
		proc sql;
			create table tmp.russca_receipt_filter_2020 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					tmp.tda_pmx_2020 as t1
				where
					datepart(t1.order_date) <= "&promo_end."d and
					datepart(t1.order_date) >= "&pseudo_promo_start."d 
			;
		quit;
	%end;
	%else %do;
		proc sql;
			create table tmp.russca_receipt_filter_2020 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					tmp.tda_pmx_2020(obs=0) as t1
			;
		quit;
	%end;

	/* 	Объединяем результаты, чтобы не пропустить переходящие промо из года в год */
	data tmp.russca_receipt_filter;
		set tmp.russca_receipt_filter_2019 tmp.russca_receipt_filter_2020;
	run;
	
	/* 	Сортируем таблицу */
	proc sort data=tmp.russca_receipt_filter;
	by order_number pbo_location_id sales_dt;
	run;
	
	/* 	Создаем уникальный ID чека */
	data tmp.russca_receipt_filter_id;
		set tmp.russca_receipt_filter;
		by order_number pbo_location_id sales_dt;
		if first.sales_dt then receipt_id+1;
	run;
	
	/* 	Размечаем промо чеки */
	proc sql;
		create table tmp.russca_receipt_promo as
			select
				t1.receipt_id,
				max(
					case 
						when t2.promo_id is not missing then 1
						else 0
					end
				) as promo_flag
			from
				tmp.russca_receipt_filter_id as t1
			left join
				tmp.discount_promo_filter as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id
			group by
				t1.receipt_id
		;
	quit;
	
	/* 	Добавляем промо флаг к чекам */
	proc sql;
		create table tmp.russca_receipt_promo2 as
			select
				t1.receipt_id,
				t1.order_number,
				t1.pbo_location_id,
				t1.product_id,
				t1.sales_dt,
				t1.qty,
				t1.qty_promo,
				t2.promo_flag,
				(case
					when t1.sales_dt >= &promo_start_num. then 1
					else 0
				end) as promo_period_flag
			from
				tmp.russca_receipt_filter_id as t1
			inner join
				tmp.russca_receipt_promo as t2
			on
				t1.receipt_id = t2.receipt_id		
			;
	quit;

	/* Считаем количество ресторано-дней для каждого товара	 */
	proc sql;
		/* Количество ресторано-дней до промо интервала */
		create table tmp.before_pbo_day_count as
			select
				t1.product_id,
				count(1) as count_before
			from (
				select distinct
					product_id,
					pbo_location_id,
					sales_dt
				from
					tmp.russca_receipt_promo2
				where
					promo_period_flag = 0
			) as t1
			group by
				t1.product_id
		;
		/* Количество ресторано-дней во время промо интервала */
		create table tmp.during_pbo_day_count as
			select
				t1.product_id,
				count(1) as count_during
			from (
				select distinct
					product_id,
					pbo_location_id,
					sales_dt
				from
					tmp.russca_receipt_promo2
				where
					promo_period_flag = 1
			) as t1
			group by
				t1.product_id
		;
		/* Соединяем одно с другим */
		create table tmp.pbo_day_count as
			select
				coalesce(t1.product_id, t2.product_id) as product_id,
				coalesce(t1.count_before, 0) as count_before,
				coalesce(t2.count_during, 0) as count_during
			from
				tmp.before_pbo_day_count as t1
			full join
				tmp.during_pbo_day_count as t2
			on
				t1.product_id = t2.product_id
		;
	quit;	

	/* Соберем каркас из товаров для множества А (промо чеки и как бы промо) */
	proc sql;
		create table tmp.russca_kanibal_a as
			select distinct
				product_id
			from
				tmp.russca_receipt_filter_id
		;
	quit;
	
	/* 	Добавляем in_in столбец */
	proc sql;
		create table tmp.russca_kanibal_a as
			select
				t1.product_id,
				coalesce(t2.in_in, 0) as in_in
			from
				tmp.russca_kanibal_a as t1
			left join (
				select
					t1.product_id,
					count(distinct receipt_id) as in_in
				from
					tmp.russca_receipt_promo2 as t1
				where
					promo_flag = 1	and
					promo_period_flag = 1		
				group by
					t1.product_id
			) as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/*	Добавляем out_in столбец */
	proc sql;
		create table tmp.russca_kanibal_a as
			select
				t1.product_id,
				t1.in_in,
				coalesce(t2.out_in, 0) as out_in
			from
				tmp.russca_kanibal_a as t1
			left join (
				select
					t1.product_id,
					count(distinct receipt_id) as out_in
				from
					tmp.russca_receipt_promo2 as t1
				where
					promo_flag = 1 and
					promo_period_flag = 0			
				group by
					t1.product_id
			) as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* 	Считаем количество чеков, где сработала и не сработала акция */
	proc sql noprint;
		select distinct 
			sum(t1.promo_period_flag),
			count(1) - sum(t1.promo_period_flag)
		into :n_promo, :n_non_promo
	    from (
			select distinct
				receipt_id,
				promo_period_flag
			from			
				tmp.russca_receipt_promo2
			where
				promo_flag = 1
		) as t1

		;
	quit;
	
	/* 	Добавляем in_out in out_out */
	proc sql;
		create table tmp.russca_kanibal_a as
			select
				t1.product_id,
				t1.in_in,
				t1.out_in,
				&n_promo. - t1.in_in as in_out,
				&n_non_promo. - t1.out_in as out_out
			from
				tmp.russca_kanibal_a as t1
		;
	quit;
	
	/* Нормируем на число чеко-дней */
	proc sql;
		create table tmp.russca_kanibal_a as
			select
				t1.product_id,
				divide(t1.in_in, t2.count_during) as in_in,
				divide(t1.out_in, t2.count_before) as out_in,
				divide(t1.in_out, t2.count_during) as in_out,
				divide(t1.out_out, t2.count_before) as out_out
			from
				tmp.russca_kanibal_a as t1
			left join
				tmp.pbo_day_count as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* 	Считаем хи квадрат */
	proc sql;
		create table tmp.russca_kanibal_a as
			select
				product_id,
				in_in,
				out_in,
				in_out,
				out_out,
				(in_in+in_out+out_in+out_out) * (in_in*out_out - in_out*out_in)**2/(in_in+in_out)/(in_in+out_in)/(out_in+out_out)/(in_out+out_out) as chi_square
			from
				tmp.russca_kanibal_a
		;
	quit;

	/* Соберем каркас из товаров для множества B (не промо чеки и как бы не промо) */
	proc sql;
		create table tmp.russca_kanibal_b as
			select distinct
				product_id
			from
				tmp.russca_receipt_filter_id
		;
	quit;
	
	/* 	Добавляем in_in столбец */
	proc sql;
		create table tmp.russca_kanibal_b as
			select
				t1.product_id,
				coalesce(t2.in_in, 0) as in_in
			from
				tmp.russca_kanibal_b as t1
			left join (
				select
					t1.product_id,
					count(distinct receipt_id) as in_in
				from
					tmp.russca_receipt_promo2 as t1
				where
					promo_flag = 0	and
					promo_period_flag = 1		
				group by
					t1.product_id
			) as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/*	Добавляем out_in столбец */
	proc sql;
		create table tmp.russca_kanibal_b as
			select
				t1.product_id,
				t1.in_in,
				coalesce(t2.out_in, 0) as out_in
			from
				tmp.russca_kanibal_b as t1
			left join (
				select
					t1.product_id,
					count(distinct receipt_id) as out_in
				from
					tmp.russca_receipt_promo2 as t1
				where
					promo_flag = 0 and
					promo_period_flag = 0			
				group by
					t1.product_id
			) as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/*	Считаем количество чеков, где сработала и не сработала акция */
	proc sql noprint;
		select distinct 
			sum(t1.promo_period_flag),
			count(1) - sum(t1.promo_period_flag)
		into :n_promo, :n_non_promo
	    from (
			select distinct
				receipt_id,
				promo_period_flag
			from			
				tmp.russca_receipt_promo2
			where
				promo_flag = 0
		) as t1

		;
	quit;
	
	/*	Добавляем in_out in out_out */
	proc sql;
		create table tmp.russca_kanibal_b as
			select
				t1.product_id,
				t1.in_in,
				t1.out_in,
				&n_promo. - t1.in_in as in_out,
				&n_non_promo. - t1.out_in as out_out
			from
				tmp.russca_kanibal_b as t1
		;
	quit;

	/* Нормируем покзатели на число чеко-дней */
	proc sql;
		create table tmp.russca_kanibal_b as
			select
				t1.product_id,
				divide(t1.in_in, t2.count_during) as in_in,
				divide(t1.out_in, t2.count_before) as out_in,
				divide(t1.in_out, t2.count_during) as in_out,
				divide(t1.out_out, t2.count_before) as out_out
			from
				tmp.russca_kanibal_b as t1
			left join
				tmp.pbo_day_count as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* Считаем хи квадрат */
	proc sql;
		create table tmp.russca_kanibal_b as
			select
				product_id,
				in_in,
				out_in,
				in_out,
				out_out,
				(in_in+in_out+out_in+out_out) * (in_in*out_out - in_out*out_in)**2/(in_in+in_out)/(in_in+out_in)/(out_in+out_out)/(in_out+out_out) as chi_square
			from
				tmp.russca_kanibal_b
		;
	quit;

	/* Объединяем одну таблицу с другой */
	proc sql;
		create table tmp.russca_kanibal as
			select
				coalesce(t1.product_id, t2.product_id) as product_id,
				t1.in_in as a_in_in,
				t1.out_in as a_out_in,
				t1.in_out as a_in_out,
				t1.out_out as a_out_out,
				t1.chi_square as a_chi_square,
				t2.in_in as b_in_in,
				t2.out_in as b_out_in,
				t2.in_out as b_in_out,
				t2.out_out as b_out_out,
				t2.chi_square as b_chi_square
			from
				tmp.russca_kanibal_a as t1
			full join
				tmp.russca_kanibal_b as t2
			on
				t1.product_id = t2.product_id
		;
	quit;


	/* 	Добавим названия товаров и id promo */
	/* 	inner join, потому что хотим убрать псевдотовары */
	proc sql;
		create table tmp.russca_kanibal as
			select
				&promo. as promo_id,
				t2.product_nm,
				t1.*
			from
				tmp.russca_kanibal as t1
			inner join
				etl_stg.ia_product as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* Убираем промо товары и считаем phi */
	proc sql;
		create table tmp.russca_kanibal as
			select
				t1.promo_id,
				t1.product_nm,
				t1.product_id,
				t1.a_in_in,
				t1.a_out_in,
				t1.a_in_out,
				t1.a_out_out,
				t1.a_chi_square,
				t1.b_in_in,
				t1.b_out_in,
				t1.b_in_out,
				t1.b_out_out,
				t1.b_chi_square,
				sqrt(t1.a_chi_square / (t1.a_in_in+t1.a_in_out+t1.a_out_in+t1.a_out_out)) as a_phi,
				sqrt(t1.b_chi_square / (t1.b_in_in+t1.b_in_out+t1.b_out_in+t1.b_out_out)) as b_phi
			from
				tmp.russca_kanibal as t1
			left join (
				select distinct
					product_id
				from
					tmp.discount_promo_filter
			) as t2
			on
				t1.product_id = t2.product_id
			where
				t2.product_id is missing
		;
	quit;
	
/* 	Считаем x-1 */
/* 	proc sort data=tmp.russca_kanibal; */
/* 	by descending phi ; */
/* 	run; */
/* 	 */
/* 	data tmp.russca_kanibal; */
/* 		set tmp.russca_kanibal; */
/* 	prev = lag(phi); */
/* 	run; */
/* 	 */
/* 	Считаем x+1 */
/* 	proc sort data=tmp.russca_kanibal; */
/* 	by phi ; */
/* 	run; */
/* 	 */
/* 	data tmp.russca_kanibal; */
/* 		set tmp.russca_kanibal; */
/* 	next = lag(phi); */
/* 	run; */
/* 	 */
/* 	proc sort data=tmp.russca_kanibal; */
/* 	by descending phi ; */
/* 	run; */
/* 	 */
/* 	Считаем коэффициент кривизны */
/* 	data tmp.russca_kanibal; */
/* 		set tmp.russca_kanibal; */
/* 		k = (prev + next - 2*phi) / ((1 + (next - phi)**2)**(3/2)); */
/* 	run; */
/* 	 */
/* 	Считаем максимальный k */
/* 	proc sql; */
/* 		create table tmp.russca_kanibal as */
/* 			select */
/* 				t1.promo_id, */
/* 				t1.product_nm, */
/* 				t1.product_id, */
/* 				t1.in_in, */
/* 				t1.out_in, */
/* 				t1.in_out, */
/* 				t1.out_out, */
/* 				t1.chi_square, */
/* 				t1.phi, */
/* 				t1.k, */
/* 				t2.max_k */
/* 			from */
/* 				tmp.russca_kanibal as t1, */
/* 			( */
/* 				select */
/* 					max(t1.k) as max_k */
/* 				from */
/* 					tmp.russca_kanibal as t1 */
/* 			) as t2 */
/* 		; */
/* 	run; */
/* 	 */
/* 	Находим argmax */
/* 	data tmp.russca_kanibal; */
/* 	set tmp.russca_kanibal; */
/* 		if k = max_k then tmp = _N_; else tmp = 0; */
/* 	run; */
/* 	 */
/* 	Запоминаем argmax */
/* 	proc sql noprint; */
/* 		select distinct  */
/* 			max(tmp) */
/* 		into :max_tmp */
/* 		from */
/* 			tmp.russca_kanibal */
/* 		; */
/* 	quit; */
/* 	 */
/* 	Помечаем сопутсвующие/канибализируемые товары */
/* 	data tmp.russca_kanibal; */
/* 		drop next prev max_k tmp; */
/* 		set tmp.russca_kanibal; */
/* 		if _N_ < &max_tmp. then flag = 1; else flag = 0; */
/* 	run; */

	/* Добавляем суммарные продажи товаров в этот период */
	proc sql;
		create table tmp.russca_kanibal as
			select
				t1.*,
				coalesce(t2.sum_qty, 0) as sum_qty
			from
				tmp.russca_kanibal as t1
			left join
				tmp.pmix_aggr_filter as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* Добавляем результаты подсчетов к результирующей таблице */
	proc append base=tmp.russca_kanibal_result
		data = tmp.russca_kanibal force;
	run;

%mend;


/* Создаем список промо акций */
proc sql;
	create table work.unique_discount_promo as
		select distinct
			PROMO_ID,
			PROMO_NM,
			start_dt,
			end_dt,
			CHANNEL_CD
		from
			work.discount_promo_2
	;
quit;

/* Удаляем таблицу с результатом */
proc datasets library=tmp;
   delete russca_kanibal_result;
run;

/* Идем по списку промо и вызываем макрос */

%calculate_adjacency_matrix(503)
%calculate_adjacency_matrix(525)
%calculate_adjacency_matrix(660)
%calculate_adjacency_matrix(668)
%calculate_adjacency_matrix(981)

/* data _null_; */
/*    set work.unique_discount_promo; */
/*    call execute('%calculate_adjacency_matrix('||promo_id||')'); */
/* run; */




/* подтягиваем навания промо */
proc sql;
	create table tmp.russca_kanibal_result as
		select
			t2.promo_nm,
			t1.*
		from
			tmp.russca_kanibal_result as t1
		inner join
			work.unique_discount_promo as t2
		on
			t1.promo_id = t2.promo_id
	;
quit;
