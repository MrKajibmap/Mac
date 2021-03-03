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
%assign;

*libname ETL_STG "/data2/etl_stg_23_11_2020";
*libname tmp "/data2/TMP";

%let inlib=ETL_STG2;
%let period_start_dt = date '2019-01-01';
%let period_end_dt = date '2020-09-30';
libname nac "/data/MN_CALC";

/* 
	Ключ выборки: ресторан, товар, день. 
	Вопросы:
	1. Временные ряды товар, ресторан без промо на всей истории стоит рассматривать?
		
	2. Модель получается для каждого временного ряда?
		Если regArima это классическая модель временных рядов, то да.
	3. А если если промо акция действует на несколько товарных категорий, то как распихивать
		его N_a по товарным категориям в признаковом пространстве?
	4. А как быть с временным рядом промо товара для какой-нибудь акции? Например, есть акция на 
		чизбургер и мы рассматриваем его временной ряд. Одновременно с этим есть акция на гамбургер.
		Для чизбургера в выборке признак соответствующий N_a в категории гамбургеров будет суммировать 
		два промо: на чизбургер и на гамбургер. Одно промо дает положительный прирост, другое промо дает
		отрицательный прирост. Как быть в такой ситуации? Ведь модель учит всего один коэффициент alpha при
		соответствующием N_a.
	5. Похожий на четвертый кейс:
		Представим ситуацию: временной ряд на чизбрургер был в промо в один момент времени и мы увидели
		рост продаж. Потом случилось промо на гамбургер, которое уменьшило продажи чизбургера. Промо было
		на одну и ту же категорию товаров, но дельта имеет разные знаки. Наша модель RegArima не сможет
		этого понять.
	6. А что делать если товар учавствует в двух промо акциях одновременно? Как учитывать эту информацию
		 в n+1 признаке? Наверное суммировать. Пока в Discount таких ситуаций нет, но если вдруг возникнет,
		то нужно будет не забывать про это.
	7. А может быть стоит разбивать промо акции не товарные категории, а на мастеркоды? Потому что в примере
		с товаром 2010 среди пиков промо в категории SN EDAP мы видим только пару падений продаж товара. 
		Что наводит на мысль, что не все промо товары в категории SN EDAP  
	8. Еще одна проблема: когда у нас есть промо на товар под тем же мастеркодом. В таком случае 
			проседание сильно больше чем от других промо. Хотя он попадает вместе с остальными промо
			в один признак, мешая нормальному обучению. Что делать?
	Ответы:
		1. Ну наверное нет
		4-5. Можно завести для каждого временного ряда отдельный признак промо на сам товар. 
			Это должно решить описанные проблемы.
		8. Либо выносить такие промо в отдельный признак, либо агрегировать продажи до мастеркода и смотреть
			на временные ряды мастеркодов.
	
	Допущения:
		1. Можно для начала избавиться от ресторанов и рассматривать агрегированные продажи
			Так будет нагляднее виден результат работы модели. Плюс рассматриваем только
			промо с механикой discount, действующие на всю сеть.

	Витрина:
		1. Целевая переменная: продажи товара в определенный день во всей сети
		2. Признаки: вектор из n+1 компонент. Где n = число товарных категорий. Каждая
			компонента этого вектора - суммарное N_a от акций, действующих на соот. категорию.
			Плюс дополнительный признак промо на товар. Если рассматриваемый промо товар учавствует
			в акции, то N_a от этой акции пойдет в этот отдельный признак, а не под соот. категорию

	Шаги сборки витрины (упрощенный вариант):
		1. Собираем целевую переменную из таблиц с агрегированными продажами. Агрегируем ряды до уровня
			товара. Ключ: товар, день.
		2. Собираем признаковое пространство.
			a. Оставляем акции с механикой discount в канале ALL, действующие на всю сеть
			b. Из чеков и таблиц с промо собираем таблицу с полями: товар|кат. тов.|день|суммарное N_a
			c. Из таблицы на шаге b. мы должны хитрым образом сформировать признаковое пространство, не
				забыв, что для каждого товара оно будет отличаться из-за призака промо на товар.
		
*/

/****** Собираем целевую переменную ******/
proc casutil;
	load data=&inlib..IA_pmix_sales_history casout='IA_pmix_sales_HISTORY' outcaslib='public' replace;
	droptable casdata="aggr_pmix" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	create table public.aggr_pmix{options replace=true} as
		select
			t1.product_id,
			t1.sales_dt,
			sum(t1.sum_qty) as sum_qty
		from (
			select
				pbo_location_id,
				product_id,
				datepart(sales_dt) as sales_dt,
				sum(sales_qty, sales_qty_promo) as sum_qty
			from
				public.ia_pmix_sales_history as t1
			where
				channel_cd = 'ALL' and	
				datepart(sales_dt) <= &period_end_dt. and
				datepart(sales_dt) >= &period_start_dt.
		) as t1
		group by
			t1.product_id,
			t1.sales_dt
	;
quit;

proc casutil;
	promote casdata="aggr_pmix" incaslib="public" outcaslib="public";
	droptable casdata="ia_pmix_sales_history" incaslib="public" quiet;
run;


/****** 2. Собираем признаковое пространство ******/
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
		select distinct
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
			t2.PBO_LEAF_ID as pbo_location_id
		from
			public.ia_promo_x_pbo as t1,
			public.pbo_lvl_all as t2
		where
			t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
	create table public.ia_promo_x_product_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t1.OPTION_NUMBER,
			t1.PRODUCT_QTY,
			t2.product_LEAF_ID as product_id
		from
			public.ia_promo_x_product as t1,
			public.product_lvl_all as t2
		where
			t1.product_id = t2.product_id
	;
	create table public.promo_ml{options replace = true} as 
		select
			t1.PROMO_ID,
			datepart(t1.start_dt) as start_dt,
			datepart(t1.end_dt) as end_dt,
			t1.promo_mechanics,
			t3.product_id,
			t3.option_number,
			t2.pbo_location_id
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

/* Выгрузим из cas */
options casdatalimit=10G;
data work.promo_ml;
	set public.promo_ml;
run;

/* Отавляем только Discount И добавляем id store */
proc sql;
	create table work.promo_ml2 as
		select
			t1.promo_id,
			t1.start_dt,
			t1.end_dt,
			t1.promo_mechanics,
			t1.product_id,
			t1.option_number,
			t1.pbo_location_id,
			input(t2.PBO_LOC_ATTR_VALUE, best32.) as store_id 
		from
			work.promo_ml as t1
		inner join (
			select distinct
				PBO_LOCATION_ID,
				PBO_LOC_ATTR_VALUE
			from
				&inlib..ia_pbo_loc_attributes
			where
				PBO_LOC_ATTR_NM='STORE_ID'
		) as t2
		on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
	;
quit;

/* Считаем средние продажи товара в ресторане */
proc sql;
	create table work.mean_pmix as
		select
			t1.product_id,
			mean(t1.sum_qty) as mean_sum_qty
		from (
			select
				pbo_location_id,
				product_id,
				sales_dt,
				sum(sum(sales_qty), sum(sales_qty_promo)) as sum_qty
			from
				&inlib..ia_pmix_sales_history as t1
			where
				channel_cd = 'ALL'
			group by
				pbo_location_id,
				product_id,
				sales_dt
		) as t1
		group by
			t1.product_id
	;
quit;

/* Считаем доли товара в рамках option number */
proc sql;
	create table work.option_mean as
		select
			t1.promo_id,
			t1.option_number,
			t1.product_id,
			t2.mean_sum_qty
		from (
			select distinct
				promo_id,
				option_number,
				product_id
			from
				work.promo_ml2
		) as t1
		inner join
			work.mean_pmix as t2
		on
			t1.product_id = t2.product_id
	;

	create table work.option_freq as
		select
			t1.promo_id,
			t1.option_number,
			t1.product_id,
			divide(t1.mean_sum_qty, t2.option_sum) as freq
		from
			work.option_mean as t1
		inner join (
			select
				promo_id,
				option_number,
				sum(mean_sum_qty) as option_sum	
			from
				work.option_mean
			group by
				promo_id,
				option_number
		) as t2			
		on
			t1.promo_id = t2.promo_id and
			t1.option_number = t2.option_number
	;
quit;

/* Добавляем посчитанные N_a */
proc sql;
	create table work.promo_ml3 as
		select
			t2.promo_id,
			t1.option_number,
			t1.product_id,
			t2.pbo_location_id,
			t2.sales_dt,
			t2.n_a,
			t2.n_a * t3.freq as n_a_modified
		from
			work.promo_ml2 as t1
		inner join
			nac.russca_na_calculation_result4 as t2
		on
			t1.promo_id = t2.promo_id and
			t1.store_id = t2.pbo_location_id
		inner join 
			work.option_freq as t3
		on
			t1.promo_id = t3.promo_id and
			t1.option_number = t3.option_number and
			t1.product_id = t3.product_id
	;
quit;

/* Суммируем промо эффект по товарам */
proc sql;
	create table work.discount_receipt_sum as
		select
			product_id,
			sales_dt,
			sum(n_a_modified) as sum_qty
		from
			work.promo_ml3
		group by
			product_id,
			sales_dt
	;
quit;

/* Добавляем категории товаров */
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
		select 
			t1.product_id, 
			t2.product_id  as LVL4_ID,
			t3.product_id  as LVL3_ID,
			t3.PARENT_product_id as LVL2_ID, 
			1 as LVL1_ID
		from 
			(select * from public.ia_product_hierarchy where product_lvl=5) as t1
		left join 
			(select * from public.ia_product_hierarchy where product_lvl=4) as t2
		on 
			t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
		left join 
			(select * from public.ia_product_hierarchy where product_lvl=3) as t3
		on
			t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
 	;
quit;

proc fedsql sessref=casauto;
	create table public.product_dictionary_ml{options replace=true} as
		select 
			t1.product_id, 
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
		from
			public.product_hier_flat as t1
		left join
			public.attr_transposed as t3
		on
			t1.product_id = t3.product_id
		left join
			PUBLIC.IA_product as t15
		on
			t1.product_id = t15.product_id
		left join
			PUBLIC.IA_product as t14
		on
			t1.lvl4_id = t14.product_id
		left join
			PUBLIC.IA_product as t13
		on
			t1.lvl3_id = t13.product_id
		left join
			PUBLIC.IA_product as t12
		on
			t1.lvl2_id = t12.product_id
	;
quit;

/* Выгружаем из cas */
data nac.product_dictionary_ml;
	set public.product_dictionary_ml;
	category_name = translate(trim(prod_lvl2_nm),'_',' ', '_', '&', '_', '-');
run;

proc sql;
	create table work.discount_receipt_sum_cat as
		select
			t1.product_id,
			t1.sales_dt,
			t1.sum_qty,
			t2.prod_lvl2_id,
			t2.category_name
		from
			work.discount_receipt_sum as t1
		inner join
			nac.product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
	;
quit;

/* Создадим процедуру формирования признакового пространства */
%macro feature_space_creation(product_id);
	
	/* Удалим промежуточные таблицы */
	proc datasets library=work;
		delete product_promo;
		delete other_product_promo;
		delete other_product_promo_zero;
		delete other_product_promo_zero_t;
		delete product_promo_together;
		delete mastercode_promo;
		delete same_mastercode_products;
	run;	

	proc casutil;
		droptable casdata="other_product_promo_zero" incaslib="public" quiet;
	run;

	/* Выделяем промо на товар */
	proc sql;
		create table work.product_promo as
			select
				product_id,
				sales_dt,
				sum_qty as positive_promo_na
			from
				work.discount_receipt_sum_cat
			where
				product_id = &product_id.
		;
	quit;
	
	/* Выделяем промо на товары под тем же мастеркодом */
	proc sql;
		create table work.same_mastercode_products as 
			select
				t1.product_id
			from
				nac.product_dictionary_ml as t1
			inner join (
				select distinct
					PROD_LVL4_ID
				from
					nac.product_dictionary_ml
				where
					product_id = &product_id.
			) as t2
			on
				t1.PROD_LVL4_ID = t2.PROD_LVL4_ID
		;
	quit;
	
	/* Посчитаем по ним суммарный N_a */
	proc sql;
		create table work.mastercode_promo as
			select
				t1.sales_dt,
				sum(t1.sum_qty) as mastercode_promo_na
			from
				work.discount_receipt_sum_cat as t1
			inner join 
				(select * from work.same_mastercode_products where product_id ^= &product_id.) as t2
			on
				t1.product_id = t2.product_id
			group by
				t1.sales_dt
		;
	quit;

	/* Выделяем промо на другие товары */
	proc sql;
		create table work.other_product_promo as
			select
				category_name,
				sales_dt,
				sum(sum_qty) as promo_na
			from
				work.discount_receipt_sum_cat as t1
			left join
				work.same_mastercode_products as t2
			on
				t1.product_id = t2.product_id
			where
				t2.product_id is missing
			group by
				category_name,
				sales_dt
		;
	quit;

	/* Добавляем нули */
	proc sql;
		create table work.other_product_promo_zero as
			select
				t1.category_name,
				t1.sales_dt,
				coalesce(t2.promo_na, 0) as promo_na
			from (
				select
					t1.sales_dt,
					t2.category_name
				from
					(select distinct sales_dt from work.other_product_promo) as t1,
					(select distinct category_name from nac.product_dictionary_ml) as t2					
			) as t1
			left join
				work.other_product_promo as t2
			on
				t1.category_name = t2.category_name and
				t1.sales_dt = t2.sales_dt
		;
	quit;
	
	/* Транспонируем промо на другие товары */
	data public.other_product_promo_zero;
		set work.other_product_promo_zero;
	run;

	proc cas;
	transpose.transpose /
	   table={name="other_product_promo_zero", caslib="public", groupby={"sales_dt"}} 
	   transpose={"promo_na"} 
	   id={"category_name"} 
	   casout={name="other_product_promo_zero_t", caslib="public", replace=true};
	quit;
	
	data work.other_product_promo_zero_t;
		set public.other_product_promo_zero_t;
	run;
	
	/* Соединяем промо на товар с промо на другие категории */
	proc sql;
		create table work.product_promo_together as	
			select
				&product_id. as product_id,
				coalesce(t1.sales_dt, t2.sales_dt) as sales_dt format date9.,
				coalesce(t1.positive_promo_na, 0) as positive_promo_na,
				coalesce(t2.Undefined_Product_Group, 0) as Undefined_Product_Group,
				coalesce(t2.Cold_Drinks, 0) as Cold_Drinks,
				coalesce(t2.Hot_Drinks, 0) as Hot_Drinks,
				coalesce(t2.Breakfast, 0) as Breakfast,
				coalesce(t2.Condiments, 0) as Condiments,
				coalesce(t2.Desserts, 0) as Desserts,
				coalesce(t2.Fries, 0) as Fries,
				coalesce(t2.Starters___Salad, 0) as Starters___Salad,
				coalesce(t2.SN_CORE, 0) as SN_CORE,
				coalesce(t2.McCafe, 0) as McCafe,
				coalesce(t2.Non_product, 0) as Non_product,
				coalesce(t2.SN_EDAP, 0) as SN_EDAP,
				coalesce(t2.SN_PREMIUM, 0) as SN_PREMIUM,
				coalesce(t2.Value_Meal, 0) as Value_Meal,
				coalesce(t2.Nuggets, 0) as Nuggets,
				coalesce(t2.Shakes, 0) as Shakes
			from
				work.product_promo as t1
			full join
				work.other_product_promo_zero_t as t2
			on
				t1.sales_dt = t2.sales_dt
			order by
				sales_dt
		;
	quit;

	/* Соеднияем с промо на мастеркод */
	proc sql;
		create table work.product_promo_together as	
			select
				&product_id. as product_id,
				coalesce(t1.sales_dt, t2.sales_dt) as sales_dt format date9.,
				coalesce(t1.positive_promo_na, 0) as positive_promo_na,
				coalesce(t2.mastercode_promo_na, 0) as mastercode_promo_na,
				coalesce(t1.Undefined_Product_Group, 0) as Undefined_Product_Group,
				coalesce(t1.Cold_Drinks, 0) as Cold_Drinks,
				coalesce(t1.Hot_Drinks, 0) as Hot_Drinks,
				coalesce(t1.Breakfast, 0) as Breakfast,
				coalesce(t1.Condiments, 0) as Condiments,
				coalesce(t1.Desserts, 0) as Desserts,
				coalesce(t1.Fries, 0) as Fries,
				coalesce(t1.Starters___Salad, 0) as Starters___Salad,
				coalesce(t1.SN_CORE, 0) as SN_CORE,
				coalesce(t1.McCafe, 0) as McCafe,
				coalesce(t1.Non_product, 0) as Non_product,
				coalesce(t1.SN_EDAP, 0) as SN_EDAP,
				coalesce(t1.SN_PREMIUM, 0) as SN_PREMIUM,
				coalesce(t1.Value_Meal, 0) as Value_Meal,
				coalesce(t1.Nuggets, 0) as Nuggets,
				coalesce(t1.Shakes, 0) as Shakes
			from
				work.product_promo_together as t1
			full join
				work.mastercode_promo as t2
			on
				t1.sales_dt = t2.sales_dt
			order by
				sales_dt
		;		
	quit;		

	/* Добавим результат к витрине */
	proc append base=nac.promo_effect_feature_space
		data = work.product_promo_together force;
	run;

%mend;


/* Удаляем таблицу с результатом */
proc datasets library=nac;
   delete promo_effect_feature_space;
run;

/* Создадим уникальный список товаров учавствовавших в промо */
proc sql;
	create table work.promo_product_list as
		select distinct
			product_id
		from
			work.discount_receipt_sum_cat
	;
quit;

/* Пройдем в цикле по товарам и будем вызывать макрос */
/* %feature_space_creation(2010) */

data _null_;
   set work.promo_product_list;
   call execute('%feature_space_creation('||put(product_id,10.)||')');
run;


/* Соеденим с витриной */
data public.promo_effect_feature_space;
	set nac.promo_effect_feature_space;
run;

proc casutil;
	droptable casdata="promo_effect_abt" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	create table public.promo_effect_abt{options replace=true} as
		select
			t1.product_id,
			t1.sales_dt,
			t1.sum_qty,
			coalesce(t2.positive_promo_na, 0) as positive_promo_na,
			coalesce(t2.mastercode_promo_na, 0) as mastercode_promo_na,
			coalesce(t2.Undefined_Product_Group, 0) as Undefined_Product_Group,
			coalesce(t2.Cold_Drinks, 0) as Cold_Drinks,
			coalesce(t2.Hot_Drinks, 0) as Hot_Drinks,
			coalesce(t2.Breakfast, 0) as Breakfast,
			coalesce(t2.Condiments, 0) as Condiments,
			coalesce(t2.Desserts, 0) as Desserts,
			coalesce(t2.Fries, 0) as Fries,
			coalesce(t2.Starters___Salad, 0) as Starters___Salad,
			coalesce(t2.SN_CORE, 0) as SN_CORE,
			coalesce(t2.McCafe, 0) as McCafe,
			coalesce(t2.Non_product, 0) as Non_product,
			coalesce(t2.SN_EDAP, 0) as SN_EDAP,
			coalesce(t2.SN_PREMIUM, 0) as SN_PREMIUM,
			coalesce(t2.Value_Meal, 0) as Value_Meal,
			coalesce(t2.Nuggets, 0) as Nuggets,
			coalesce(t2.Shakes, 0) as Shakes			
		from
			public.aggr_pmix as t1
		left join
			public.promo_effect_feature_space as t2
		on
			t1.product_id = t2.product_id and
			t1.sales_dt = t2.sales_dt
		inner join
			(select distinct product_id from public.promo_effect_feature_space) as t3
		on
			t1.product_id = t3.product_id
	;
quit;

data nac.promo_effect_abt;
	set public.promo_effect_abt;
run;

proc sort data=nac.promo_effect_abt ;
	by sales_dt;
run;

data public.promo_effect_abt;
	set nac.promo_effect_abt;
run;

/* Возьмем один бургер */
proc fedsql sessref=casauto;
	create table public.one_burger{options replace=true} as
		select
			t1.product_id,
			t1.sales_dt,
			t1.sum_qty,
			t1.positive_promo_na,
			t1.mastercode_promo_na,
			t1.Undefined_Product_Group,
			t1.Cold_Drinks,
			t1.Hot_Drinks,
			t1.Breakfast,
			t1.Condiments,
			t1.Desserts,
			t1.Fries,
			t1.Starters___Salad,
			t1.SN_CORE,
			t1.McCafe,
			t1.Non_product,
			t1.SN_EDAP,
			t1.SN_PREMIUM,
			t1.Value_Meal,
			t1.Nuggets,
			t1.Shakes		
		from
			public.promo_effect_abt as t1
		where
			t1.product_id = 2010
	;
quit;

/* Прогнозируем ВР */
proc cas;
   timeData.forecast /
		table={name='one_burger', caslib="public", groupBy={{name='product_id'}}},
		timeId={name='sales_dt'},
		interval='day',
		mode='DYNAMIC',
		trimId = 'LEFT',
		dependents={{name='sum_qty'}},
		predictors={
			{name='positive_promo_na'},
			{name='mastercode_promo_na'},
			{name='hot_drinks'},
			{name='desserts'},
			{name='starters___salad'},
			{name='sn_core'},
			{name='sn_edap'},
			{name='nuggets'}
		},
		lead=52*2,
		seasonality=7,
		forOut={name='salefor', caslib="public", replace=True},
		infoOut={name='saleinfo', caslib="public", replace=True},
		indepOut={name='saleindep', caslib="public", replace=True},
		selectOut={name='saleselect', caslib="public", replace=True},
		specOut={name='salespec', caslib="public", replace=True};
   run;
   table.fetch /
      table = {name='saleselect', caslib="public"};
   table.fetch /
      table = {name='salefor', caslib="public"};
   run;
quit;






/* ods select UnitRootTestModel */
/*            UnitRootTest */
/*            AutoChoiceModel */
/*            Best5Model */
/*            AutomaticModelChoice */
/*            InitialModelChoice */
/*            FinalModelChecks */
/*            FinalModelChoice */
/*            AutomdlNote; */



/* proc x13 data=tmp.promo_effect_abt date=sales_dt; */
/*    var sum_qty; */
/*    transform function=log; */
/*    REGRESSION USERVAR= */
/* 		positive_promo_na */
/* 		Undefined_Product_Group */
/* 		Cold_Drinks */
/* 		Hot_Drinks */
/* 		Breakfast */
/* 		Condiments */
/* 		Desserts */
/* 		Fries */
/* 		Starters___Salad */
/* 		SN_CORE */
/* 		McCafe */
/* 		Non_product */
/* 		SN_EDAP */
/* 		SN_PREMIUM */
/* 		Value_Meal */
/* 		Nuggets */
/* 		Shakes; */
/*    automdl maxorder=(1,1) */
/*            print=all; */
/*    estimate; */
/*    x11; */
/*    output out=out a1 a2 a6 b1 c17 c20 d1 d7 d8 d9 d10 */
/*                   d11 d12 d13 d16 d18; */
/* run; */
































