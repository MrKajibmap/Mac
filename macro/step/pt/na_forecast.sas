/*
	0. Объявление макропеременных
		1. Сборка витрины для прогноза N_a
		1.1 Вычисление каркаса таблицы промо акций: промо, ПБО, товара, интервал, механика
		1.2 One hot кодировка механики промо акции
		1.3 Количество товаров, участвующих в промо (количество уникальных product_id),
			количество позиций (количество уникальных option_number), 
			количество единиц товара, необходимое для покупки 
		1.4 Пускай у нас имеется k товарных категорий, тогда создадим вектор размерности k.
			Каждая компонента этого вектора описывает количество товаров данной категории участвующих в промо.
		1.5 Атрибуты ПБО
		1.6 Календарные признаки и праздники
		1.7 Признаки описывающие трафик ресторана (количество чеков)
		1.8 Признаки описывающие продажи промо товаров
	
	2. Подсчет целевой переменной
	
	3. Прогнозирование, анализ результатов
*/
options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
/*Создать cas-сессию, если её нет*/
%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign;

*libname ETL_STG "/data2/etl_stg_23_11_2020";
*libname tmp "/data2/TMP";
%let inlib=ETL_STG2;
%let hist_start_dt = date '2019-01-01';
libname nac "/data/MN_CALC";
/* 
	Конец истории нужен для проведения оценки качества на тестовой выборке.
	В дальнейшем конец истории будет выставлять в последний доступный день.
*/
%let hist_end_dt =  date '2020-08-28';

/* Интервал в котором формируем календарные признаки */
%let first_date = '01jan2017'd;
%let last_date = '31dec2023'd;


/****** 1.1 Вычисление каркаса таблицы промо акций ******/
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
  	droptable casdata="promo_skelet" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet;
  	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet;
  	droptable casdata="na_abt1" incaslib="public" quiet;
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
	create table public.promo_skelet{options replace = true} as 
		select
			t1.PROMO_ID,
			t2.pbo_location_id,
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			(datepart(t1.END_DT) - datepart(t1.START_DT)) as promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
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
/* 				when t1.PROMO_MECHANICS = '1+1%' then 'one_plus_one' */
			end) as promo_mechanics_name,
			1 as promo_flag		
		from
			public.ia_promo as t1
		left join
			public.ia_promo_x_pbo_leaf as t2
		on 
			t1.PROMO_ID = t2.PROMO_ID
		where
			t1.channel_cd = 'ALL' and
			datepart(t1.start_dt) >= &hist_start_dt.
	;
quit;

/* Расшиваем интервалы по дням */
data public.na_abt1;
	set public.promo_skelet;
	format sales_dt DATE9.;
	do sales_dt=start_dt to end_dt;
		output;
	end;
run;

proc casutil;
	droptable casdata="ia_pbo_loc_hierarchy" incaslib="public" quiet;
	droptable casdata="ia_product_hierarchy" incaslib="public" quiet;
/* 	droptable casdata="ia_promo" incaslib="public" quiet; */
	droptable casdata="ia_promo_x_pbo" incaslib="public" quiet;
	droptable casdata="ia_promo_x_product" incaslib="public" quiet;

	droptable casdata="pbo_hier_flat" incaslib="public" quiet;
	droptable casdata="product_hier_flat" incaslib="public" quiet;
	droptable casdata="lvl5" incaslib="public" quiet;
	droptable casdata="lvl4" incaslib="public" quiet;
	droptable casdata="lvl3" incaslib="public" quiet;
	droptable casdata="lvl2" incaslib="public" quiet;
	droptable casdata="lvl1" incaslib="public" quiet;
	droptable casdata="pbo_lvl_all" incaslib="public" quiet;
	droptable casdata="product_lvl_all" incaslib="public" quiet;
/*   	droptable casdata="ia_promo_x_pbo_leaf" incaslib="public" quiet; */
run;


/****** 1.2 One hot кодировка механики промо акции ******/
proc casutil;
	droptable casdata="na_abt2" incaslib="public" quiet;
	droptable casdata="promo_mechanics" incaslib="public" quiet;
	droptable casdata="promo_mechanics_one_hot" incaslib="public" quiet;
run;

/* Определяем механики промо акций */
proc fedsql sessref=casauto;
	create table public.promo_mechanics{options replace=true} as
		select distinct
			promo_id,
			promo_mechanics_name,
			promo_flag
		from
			public.na_abt1
	;
quit;

/* Транспонируем механику промо в вектор */
proc cas;
transpose.transpose /
	table = {
		name="promo_mechanics",
		caslib="public",
		groupby={"promo_id"}}
	transpose={"promo_flag"} 
	id={"promo_mechanics_name"} 
	casout={name="promo_mechanics_one_hot", caslib="public", replace=true};
quit;

/* Добавляем переменные к витрине */
proc fedsql sessref=casauto;
	create table public.na_abt2{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.sales_dt,
			coalesce(t2.bogo, 0) as bogo,
			coalesce(t2.discount, 0) as discount,
			coalesce(t2.evm_set, 0) as evm_set,
			coalesce(t2.non_product_gift, 0) as non_product_gift,
			coalesce(t2.pairs, 0) as pairs,
			coalesce(t2.product_gift, 0) as product_gift,
			coalesce(t2.other_discount, 0) as other_discount,
			coalesce(t2.other_digital, 0) as other_digital,
			coalesce(t2.support, 0) as support
/* 			coalesce(t2.one_plus_one, 0) as one_plus_one */
		from
			public.na_abt1 as t1
		left join
			public.promo_mechanics_one_hot as t2
		on
			t1.promo_id = t2.promo_id
	;
quit;

proc casutil;
	droptable casdata="na_abt1" incaslib="public" quiet;
	droptable casdata="promo_mechanics" incaslib="public" quiet;
	droptable casdata="promo_mechanics_one_hot" incaslib="public" quiet;
run;


/****** 
	1.3 Количество товаров, участвующих в промо (количество уникальных product_id),
		количество позиций (количество уникальных option_number), 
		количество единиц товара, необходимое для покупки 
******/
proc casutil;
	droptable casdata="na_abt3" incaslib="public" quiet;
	droptable casdata="product_characteristics" incaslib="public" quiet;
	droptable casdata="product_characteristics2" incaslib="public" quiet;
run;

proc fedsql sessref=casauto;
	/* Количество товаров, позиций участвующих в промо */
	create table public.product_characteristics{options replace=true} as
		select
			promo_id,
			max(option_number) as number_of_options,
			count(distinct product_id) as number_of_products
		from
			public.ia_promo_x_product_leaf
		group by
			promo_id
	;
	/* Количество единиц товара, необходимое для покупки */
	create table public.product_characteristics2{options replace=true} as
		select
			t1.promo_id,
			sum(product_qty) as necessary_amount
		from (
			select distinct
				promo_id,
				option_number,
				PRODUCT_QTY
			from
				public.ia_promo_x_product_leaf
		) as t1
		group by
			t1.promo_id
	;
quit;

/* Добавляем признаки в витрину */
proc fedsql sessref=casauto;
	create table public.na_abt3{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
/* 			t1.one_plus_one, */
			t2.number_of_options,
			t2.number_of_products,
			t3.necessary_amount
		from
			public.na_abt2 as t1
		left join
			public.product_characteristics as t2
		on
			t1.promo_id = t2.promo_id
		left join
			public.product_characteristics2 as t3
		on
			t1.promo_id = t3.promo_id	
	;
quit;

proc casutil;
	droptable casdata="na_abt2" incaslib="public" quiet;
	droptable casdata="product_characteristics" incaslib="public" quiet;
	droptable casdata="product_characteristics2" incaslib="public" quiet;
run;


/****** 
	1.4 Пускай у нас имеется k товарных категорий,
	 тогда создадим вектор размерности k. Каждая компонента этого
	 вектора описывает количество товаров данной категории участвующих в промо. 
******/
proc casutil;
	droptable casdata="na_abt4" incaslib="public" quiet;
	droptable casdata="product_dictionary_ml" incaslib="public" quiet;
	droptable casdata="promo_category" incaslib="public" quiet;
	droptable casdata="promo_category_transposed" incaslib="public" quiet;
	droptable casdata="attr_transposed" incaslib="public" quiet;
	droptable casdata="product_hier_flat" incaslib="public" quiet;
	
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

/* Заменяем пробел на нижнее подчеркивание */
data work.product_dictionary_ml;
	set public.product_dictionary_ml;
	prod_lvl2_nm2 = translate(trim(prod_lvl2_nm),'_',' ');
run;

/* Возвращаем таблицу в cas */
data public.product_dictionary_ml;
	set work.product_dictionary_ml;
run;

/* Считаем количество товаров в категории */
proc fedsql sessref=casauto;
	create table public.promo_category{options replace=true} as
		select
			t1.promo_id,
			t2.prod_lvl2_nm2,
			count(distinct t1.product_id) as count_promo
		from
			public.ia_promo_x_product_leaf as t1
		inner join
			public.product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
		group by
			t1.promo_id,
			t2.prod_lvl2_nm2
	;
quit;

/* Транспонируем таблицу */
proc cas;
transpose.transpose /
   table={name="promo_category", caslib="public", groupby={"promo_id"}} 
   transpose={"count_promo"} 
   id={"prod_lvl2_nm2"} 
   casout={name="promo_category_transposed", caslib="public", replace=true};
quit;

/* Добавляем признаки в витрину */
proc fedsql sessref=casauto;
	create table public.na_abt4{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
/* 			t1.one_plus_one, */
			t1.number_of_options,
			t1.number_of_products,
			t1.necessary_amount,
			coalesce(t2.McCafe, 0) as McCafe, 
			coalesce(t2.SN_CORE, 0) as SN_CORE,
			coalesce(t2.Shakes, 0) as Shakes,
			coalesce(t2.Cold_Drinks, 0) as Cold_Drinks,
			coalesce(t2.Breakfast, 0) as Breakfast_food,
			coalesce(t2.Nuggets, 0) as Nuggets,
			coalesce(t2."Starters_&_Salad", 0) as Starters_Salad,
			coalesce(t2.Desserts, 0) as Desserts,
			coalesce(t2."Non-product", 0) as Non_product,
			coalesce(t2.Value_Meal, 0) as Value_Meal,
			coalesce(t2.Fries, 0) as Fries,
			coalesce(t2.SN_EDAP, 0) as SN_EDAP,
			coalesce(t2.Undefined_Product_Group, 0) as Undefined_Product_Group,
			coalesce(t2.Condiments, 0) as Condiments,
			coalesce(t2.Hot_Drinks, 0) as Hot_Drinks,
			coalesce(t2.SN_PREMIUM, 0) as SN_PREMIU
		from
			public.na_abt3 as t1
		left join
			public.promo_category_transposed as t2
		on
			t1.promo_id = t2.promo_id
	;
quit;

proc casutil;
	droptable casdata="promo_category" incaslib="public" quiet;
	droptable casdata="promo_category_transposed" incaslib="public" quiet;
	droptable casdata="attr_transposed" incaslib="public" quiet;
	droptable casdata="product_hier_flat" incaslib="public" quiet;
	droptable casdata="IA_product" incaslib="public" quiet;
	droptable casdata="IA_product_HIERARCHY" incaslib="public" quiet;
	droptable casdata="IA_product_ATTRIBUTES" incaslib="public" quiet;
	droptable casdata="na_abt3" incaslib="public" quiet;
/* 	droptable casdata="product_dictionary_ml" incaslib="public" quiet; */
/* 	droptable casdata="ia_promo_x_product_leaf" incaslib="public" quiet; */
run;


/****** 1.5 Атрибуты ПБО ******/
proc casutil;
	droptable casdata="na_abt5" incaslib="public" quiet;
	droptable casdata="attr_transposed" incaslib="public" quiet;
	droptable casdata="pbo_hier_flat" incaslib="public" quiet;
	droptable casdata="pbo_dictionary_ml" incaslib="public" quiet;
	
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
	create table public.na_abt5{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
/* 			t1.one_plus_one, */
			t1.number_of_options,
			t1.number_of_products,
			t1.necessary_amount,
			t1.McCafe,
			t1.SN_CORE,
			t1.Shakes,
			t1.Cold_Drinks,
			t1.Breakfast_food,
			t1.Nuggets,
			t1.Starters_Salad,
			t1.Desserts,
			t1.Non_product,
			t1.Value_Meal,
			t1.Fries,
			t1.SN_EDAP,
			t1.Undefined_Product_Group,
			t1.Condiments,
			t1.Hot_Drinks,
			t1.SN_PREMIU,
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
			public.na_abt4 as t1
		left join
			public.pbo_dictionary_ml as t2
		on
			t1.pbo_location_id = t2.pbo_location_id
	;
quit;

proc casutil;
	droptable casdata="na_abt4" incaslib="public" quiet;
	droptable casdata="attr_transposed" incaslib="public" quiet;
	droptable casdata="pbo_hier_flat" incaslib="public" quiet;
	droptable casdata="pbo_dictionary_ml" incaslib="public" quiet;
	droptable casdata='ia_pbo_location' incaslib='public' quiet;
	droptable casdata='IA_PBO_LOC_HIERARCHY' incaslib='public' quiet;
	droptable casdata='IA_PBO_LOC_ATTRIBUTES' incaslib='public' quiet;
run;


/****** 1.6 Календарные признаки и праздники ******/
proc casutil;
	droptable casdata="na_abt6" incaslib="public" quiet;
	droptable casdata="russia_weekend_transposed" incaslib="public" quiet;
	droptable casdata="cldr_prep_features2" incaslib="public" quiet;
	droptable casdata="russia_weekend" incaslib="public" quiet;
run;

data work.cldr_prep;
	retain date &first_date.;
	do while(date <= &last_date.);
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
			year(date) as year,
			(case
				when weekday(date) in (1, 7) then 1
				else 0
			end) as weekend_flag
		from
			work.cldr_prep
	;
quit;

/* Список выходных дней в РФ с 2017 по 2023 */
data work.russia_weekend;
input date :yymmdd10. weekend_name $64.;
format date yymmddd10.;
datalines;
2017-01-01 New_year
2017-01-02 Day_After_New_Year
2017-01-07 Christmas
2017-02-23 Defendence_of_the_Fatherland
2017-03-08 International_Womens_Day
2017-05-09 Victory_Day
2017-06-12 National_Day
2017-11-04 Day_of_Unity
2017-05-01 Labour_Day
2017-01-02 New_Year_shift
2017-12-25 Christmas_Day
2018-01-01 New_year
2018-01-02 Day_After_New_Year
2018-01-07 Christmas
2018-02-23 Defendence_of_the_Fatherland
2018-03-08 International_Womens_Day
2018-05-09 Victory_Day
2018-06-12 National_Day
2018-11-04 Day_of_Unity
2018-05-01 Labour_Day
2018-12-25 Christmas_Day
2019-01-01 New_year
2019-01-02 Day_After_New_Year
2019-01-07 Christmas
2019-02-23 Defendence_of_the_Fatherland
2019-03-08 International_Womens_Day
2019-05-09 Victory_Day
2019-06-12 National_Day
2019-11-04 Day_of_Unity
2019-05-01 Labour_Day
2019-12-25 Christmas_Day
2020-01-01 New_year
2020-01-02 Day_After_New_Year
2020-01-07 Christmas
2020-02-23 Defendence_of_the_Fatherland
2020-03-08 International_Womens_Day
2020-05-09 Victory_Day
2020-06-12 National_Day
2020-11-04 Day_of_Unity
2020-05-01 Labour_Day
2020-12-25 Christmas_Day
2021-01-01 New_year
2021-01-02 Day_After_New_Year
2021-01-07 Christmas
2021-02-23 Defendence_of_the_Fatherland
2021-03-08 International_Womens_Day
2021-05-09 Victory_Day
2021-06-12 National_Day
2021-11-04 Day_of_Unity
2021-05-01 Labour_Day
2021-12-25 Christmas_Day
2022-01-01 New_year
2022-01-02 Day_After_New_Year
2022-01-07 Christmas
2022-02-23 Defendence_of_the_Fatherland
2022-03-08 International_Womens_Day
2022-05-09 Victory_Day
2022-06-12 National_Day
2022-11-04 Day_of_Unity
2022-05-01 Labour_Day
2022-01-03 New_Year_shift
2022-12-25 Christmas_Day
2023-01-01 New_year
2023-01-02 Day_After_New_Year
2023-01-07 Christmas
2023-02-23 Defendence_of_the_Fatherland
2023-03-08 International_Womens_Day
2023-05-09 Victory_Day
2023-06-12 National_Day
2023-11-04 Day_of_Unity
2023-05-01 Labour_Day
2023-01-02 New_Year_shift
2023-12-25 Christmas_Day
;
run;

/* загружаем в cas */
data public.russia_weekend;
set work.russia_weekend;
weekend_flag=1;
run;

/* транспонируем russia_weekend */
proc cas;
transpose.transpose /
   table={name="russia_weekend", caslib="public", groupby={"date"}} 
   transpose={"weekend_flag"} 
   id={"weekend_name"} 
   casout={name="russia_weekend_transposed", caslib="public", replace=true};
quit;

/* Объединяем государственные выходные с субботой и воскресеньем */
proc sql;
	create table work.cldr_prep_features2 as 
		select
			t1.date,
			t1.week,
			t1.weekday,
			t1.month,
			t1.year,
			t1.weekend_flag as regular_weekend_flag,
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

/* Загружаем в cas */
data public.cldr_prep_features2;
	set work.cldr_prep_features2;
run;

/* Добавляем к витрине */
proc fedsql sessref = casauto;
	create table public.na_abt6{options replace = true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
/* 			t1.one_plus_one, */
			t1.number_of_options,
			t1.number_of_products,
			t1.necessary_amount,
			t1.McCafe,
			t1.SN_CORE,
			t1.Shakes,
			t1.Cold_Drinks,
			t1.Breakfast_food,
			t1.Nuggets,
			t1.Starters_Salad,
			t1.Desserts,
			t1.Non_product,
			t1.Value_Meal,
			t1.Fries,
			t1.SN_EDAP,
			t1.Undefined_Product_Group,
			t1.Condiments,
			t1.Hot_Drinks,
			t1.SN_PREMIU,
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
			t2.week,
			t2.weekday,
			t2.month,
			t2.year,
			t2.regular_weekend_flag,
			t2.weekend_flag,
			coalesce(t3.Christmas, 0) as Christmas,
			coalesce(t3.Christmas_Day, 0) as Christmas_Day,
			coalesce(t3.Day_After_New_Year, 0) as Day_After_New_Year,
			coalesce(t3.Day_of_Unity, 0) as Day_of_Unity,
			coalesce(t3.Defendence_of_the_Fatherland, 0) as Defendence_of_the_Fatherland,
			coalesce(t3.International_Womens_Day, 0) as International_Womens_Day,
			coalesce(t3.Labour_Day, 0) as Labour_Day,
			coalesce(t3.National_Day, 0) as National_Day,
			coalesce(t3.New_Year_shift, 0) as New_Year_shift,
			coalesce(t3.New_year, 0) as New_year,
			coalesce(t3.Victory_Day, 0) as Victory_Day 
		from
			public.na_abt5 as t1
		left join
			public.cldr_prep_features2 as t2
		on
			t1.sales_dt = t2.date
		left join
			public.russia_weekend_transposed as t3
		on
			t1.sales_dt = t3.date
	;
quit;

proc casutil;
	droptable casdata="na_abt5" incaslib="public" quiet;
	droptable casdata="russia_weekend_transposed" incaslib="public" quiet;
	droptable casdata="cldr_prep_features2" incaslib="public" quiet;
	droptable casdata="russia_weekend" incaslib="public" quiet;
run;


/****** 1.7 Признаки описывающие трафик ресторана ******/
proc casutil;
	load data=&inlib..ia_pbo_sales_history casout='ia_pbo_sales_history' outcaslib='public' replace;
	droptable casdata="gc_aggr_smart" incaslib="public" quiet;
	droptable casdata="gc_aggr_dump" incaslib="public" quiet;
	droptable casdata="na_abt7" incaslib="public" quiet;
run;

/* Агрегируем чеки до ПБО, год, месяц, день недели */
proc fedsql sessref=casauto;
	create table public.gc_aggr_smart{options replace=true} as
		select
			t1.pbo_location_id,
			t1.year,
			t1.month,
			t1.weekday,
			mean(t1.receipt_qty) as mean_receipt_qty,
			std(t1.receipt_qty) as std_receipt_qty
		from (
			select
				pbo_location_id,
				year(datepart(sales_dt)) as year,
				month(datepart(sales_dt)) as month,
				weekday(datepart(sales_dt)) as weekday,
				receipt_qty
			from
				public.ia_pbo_sales_history
			where
				channel_cd = 'ALL'
		) as t1
		group by
			t1.pbo_location_id,
			t1.year,
			t1.month,
			t1.weekday			
	;
quit;

/* Агрегируем чеки до год, месяц, день недели */
proc fedsql sessref=casauto;
	create table public.gc_aggr_dump{options replace=true} as
		select
			t1.year,
			t1.month,
			t1.weekday,
			mean(t1.receipt_qty) as mean_receipt_qty,
			std(t1.receipt_qty) as std_receipt_qty
		from (
			select
				year(datepart(sales_dt)) as year,
				month(datepart(sales_dt)) as month,
				weekday(datepart(sales_dt)) as weekday,
				receipt_qty
			from
				public.ia_pbo_sales_history
			where
				channel_cd = 'ALL'
		) as t1
		group by
			t1.year,
			t1.month,
			t1.weekday			
	;
quit;

/* Добавляем к витрине характеристики трафика ресторана */
proc fedsql sessref=casauto;
	create table public.na_abt7{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
/* 			t1.one_plus_one, */
			t1.number_of_options,
			t1.number_of_products,
			t1.necessary_amount,
			t1.McCafe,
			t1.SN_CORE,
			t1.Shakes,
			t1.Cold_Drinks,
			t1.Breakfast_food,
			t1.Nuggets,
			t1.Starters_Salad,
			t1.Desserts,
			t1.Non_product,
			t1.Value_Meal,
			t1.Fries,
			t1.SN_EDAP,
			t1.Undefined_Product_Group,
			t1.Condiments,
			t1.Hot_Drinks,
			t1.SN_PREMIU,
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
			t1.week,
			t1.weekday,
			t1.month,
			t1.year,
			t1.regular_weekend_flag,
			t1.weekend_flag,
			t1.Christmas,
			t1.Christmas_Day,
			t1.Day_After_New_Year,
			t1.Day_of_Unity,
			t1.Defendence_of_the_Fatherland,
			t1.International_Womens_Day,
			t1.Labour_Day,
			t1.National_Day,
			t1.New_Year_shift,
			t1.New_year,
			t1.Victory_Day,
			coalesce(t2.mean_receipt_qty, t3.mean_receipt_qty) as mean_receipt_qty,
			coalesce(t2.std_receipt_qty, t3.std_receipt_qty) as std_receipt_qty	
		from
			public.na_abt6 as t1
		left join
			public.gc_aggr_smart as t2
		on
			(t1.year - 1) = t2.year and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.month = t2.month and
			t1.weekday = t2.weekday
		left join
			public.gc_aggr_dump as t3
		on
			(t1.year - 1) = t3.year and
			t1.month = t3.month and
			t1.weekday = t3.weekday
	;
quit;

proc casutil;
	droptable casdata="gc_aggr_smart" incaslib="public" quiet;
	droptable casdata="gc_aggr_dump" incaslib="public" quiet;
	droptable casdata="na_abt6" incaslib="public" quiet;
run;


/****** 1.8 Признаки описывающие продажи промо товаров ******/
proc casutil;
	load data=&inlib..ia_pmix_sales_history casout='ia_pmix_sales_history' outcaslib='public' replace;
	
	droptable casdata="na_abt8" incaslib="public" quiet;
	droptable casdata="pmix_mastercode_sum" incaslib="public" quiet;
	droptable casdata="promo_ml" incaslib="public" quiet;
	droptable casdata="promo_ml2" incaslib="public" quiet;
	droptable casdata="promo_ml3" incaslib="public" quiet;
	droptable casdata="pmix_aggr_smart" incaslib="public" quiet;
	droptable casdata="pmix_aggr_dump" incaslib="public" quiet;
run;

/* Создаем временные ряды продаж мастеркодов */
proc fedsql sessref=casauto;
	create table public.pmix_mastercode_sum{options replace=true} as
		select
			t1.pbo_location_id,
			t1.PROD_LVL4_ID,
			t1.sales_dt,
			sum(t1.sales_qty) as sales_qty
		from (
			select
				t2.PROD_LVL4_ID,
				datepart(t1.sales_dt) as sales_dt,
				t1.pbo_location_id,
				sum(t1.sales_qty, t1.sales_qty_promo) as sales_qty	
			from
				public.ia_pmix_sales_history as t1
			inner join
				public.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
			where
				t1.channel_cd = 'ALL'
		) as t1
		group by
			t1.pbo_location_id,
			t1.PROD_LVL4_ID,
			t1.sales_dt
	;
quit;

/* Снова создадим таблицу с промо акциями */
proc fedsql sessref=casauto;
	create table public.promo_ml{options replace = true} as 
		select
			t1.PROMO_ID,
			datepart(t1.start_dt) as start_dt,
			datepart(t1.end_dt) as end_dt,
			t1.promo_mechanics,
			t3.product_id,
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

/* Меняем товары на мастеркоды  */
proc fedsql sessref=casauto;
	create table public.promo_ml2{options replace = true} as 
		select distinct
			t1.PROMO_ID,
			t2.PROD_LVL4_ID,
			t1.pbo_location_id
		from
			public.promo_ml as t1
		inner join
			public.product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
	;
quit;

/* Соединяем продажи с промо */
proc fedsql sessref=casauto;
	create table public.promo_ml3{options replace = true} as 
		select
			t1.promo_id,
			t1.pbo_location_id,
			t2.sales_dt,
			mean(t2.sales_qty) as mean_sales_qty
		from
			public.promo_ml2 as t1
		left join
			public.pmix_mastercode_sum as t2
		on
			t1.PROD_LVL4_ID = t2.PROD_LVL4_ID and
			t1.pbo_location_id = t2.pbo_location_id
		group by
			t1.promo_id,
			t1.pbo_location_id,
			t2.sales_dt			
	;
quit;

/* Считаем агрегаты Промо, ПБО, год, месяц, день недели */
proc fedsql sessref=casauto;
	create table public.pmix_aggr_smart{options replace=true} as
		select
			t1.promo_id,
			t1.pbo_location_id,
			t1.year,
			t1.month,
			t1.weekday,
			mean(t1.mean_sales_qty) as mean_sales_qty,
			std(t1.mean_sales_qty) as std_sales_qty
		from (
			select
				t1.promo_id,
				t1.pbo_location_id,
				year(t1.sales_dt) as year,
				month(t1.sales_dt) as month,
				weekday(t1.sales_dt) as weekday,
				t1.mean_sales_qty
			from
				public.promo_ml3 as t1
		) as t1
		group by
			t1.promo_id,
			t1.pbo_location_id,
			t1.year,
			t1.month,
			t1.weekday
	;
quit;

/* Считаем агрегаты Промо, год, месяц, день недели */
proc fedsql sessref=casauto;
	create table public.pmix_aggr_dump{options replace=true} as
		select
			t1.promo_id,
			t1.year,
			t1.month,
			t1.weekday,
			mean(t1.mean_sales_qty) as mean_sales_qty,
			std(t1.mean_sales_qty) as std_sales_qty
		from (
			select
				t1.promo_id,
				year(t1.sales_dt) as year,
				month(t1.sales_dt) as month,
				weekday(t1.sales_dt) as weekday,
				t1.mean_sales_qty
			from
				public.promo_ml3 as t1
		) as t1
		group by
			t1.promo_id,
			t1.year,
			t1.month,
			t1.weekday
	;
quit;

/* Добавляем к витрине характеристики трафика ресторана */
proc fedsql sessref=casauto;
	create table public.na_abt8{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
			t1.number_of_options,
			t1.number_of_products,
			t1.necessary_amount,
			t1.McCafe,
			t1.SN_CORE,
			t1.Shakes,
			t1.Cold_Drinks,
			t1.Breakfast_food,
			t1.Nuggets,
			t1.Starters_Salad,
			t1.Desserts,
			t1.Non_product,
			t1.Value_Meal,
			t1.Fries,
			t1.SN_EDAP,
			t1.Undefined_Product_Group,
			t1.Condiments,
			t1.Hot_Drinks,
			t1.SN_PREMIU,
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
			t1.week,
			t1.weekday,
			t1.month,
			t1.year,
			t1.regular_weekend_flag,
			t1.weekend_flag,
			t1.Christmas,
			t1.Christmas_Day,
			t1.Day_After_New_Year,
			t1.Day_of_Unity,
			t1.Defendence_of_the_Fatherland,
			t1.International_Womens_Day,
			t1.Labour_Day,
			t1.National_Day,
			t1.New_Year_shift,
			t1.New_year,
			t1.Victory_Day,
			t1.mean_receipt_qty,
			t1.std_receipt_qty,
			coalesce(t2.mean_sales_qty, t3.mean_sales_qty) as mean_sales_qty,
			coalesce(t2.std_sales_qty, t3.std_sales_qty) as std_sales_qty
		from
			public.na_abt7 as t1
		left join
			public.pmix_aggr_smart as t2
		on
			t1.promo_id = t2.promo_id and
			(t1.year - 1) = t2.year and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.month = t2.month and
			t1.weekday = t2.weekday
		left join
			public.pmix_aggr_dump as t3
		on
			t1.promo_id = t3.promo_id and
			(t1.year - 1) = t3.year and
			t1.month = t3.month and
			t1.weekday = t3.weekday
	;
quit;

proc casutil;
	droptable casdata="ia_pmix_sales_history" incaslib="public" quiet;
	droptable casdata="pmix_mastercode_sum" incaslib="public" quiet;
	droptable casdata="na_abt7" incaslib="public" quiet;
	droptable casdata="promo_ml2" incaslib="public" quiet;
	droptable casdata="promo_ml3" incaslib="public" quiet;
	droptable casdata="pmix_aggr_smart" incaslib="public" quiet;
	droptable casdata="pmix_aggr_dump" incaslib="public" quiet;
run;


/****** 2. Подсчет целевой переменной ******/
proc casutil;
	droptable casdata="na_abt9" incaslib="public" quiet;
	droptable casdata="russca_na_calculation_result" incaslib="public" quiet;
run;

/* Меняем ID */
proc sql;
	create table work.russca_na_calculation_result as
		select
			t1.promo_id,
			t2.pbo_location_id,
			t1.sales_dt,
			t1.n_a
		from
			nac.russca_na_calculation_result4 as t1
		inner join (
			select distinct
				PBO_LOCATION_ID,
				input(PBO_LOC_ATTR_VALUE, best32.) as store_id
			from
				&inlib..ia_pbo_loc_attributes
			where
				PBO_LOC_ATTR_NM='STORE_ID'
		) as t2
		on
			t1.PBO_LOCATION_ID = t2.store_id
	;
quit;

/* Выгружаем в cas таблицу */
data public.russca_na_calculation_result;
	set work.russca_na_calculation_result;
run;

proc fedsql sessref=casauto;
	create table public.na_abt9{options replace=true} as
		select
			t1.PROMO_ID,
			t1.pbo_location_id,
			t1.sales_dt,
			t1.promo_lifetime,
			t1.CHANNEL_CD,
			t1.NP_GIFT_PRICE_AMT,
			t1.PROMO_GROUP_ID,
			t1.bogo,
			t1.discount,
			t1.evm_set,
			t1.non_product_gift,
			t1.pairs,
			t1.product_gift,
			t1.other_discount,
			t1.other_digital,
			t1.support,
			t1.number_of_options,
			t1.number_of_products,
			t1.necessary_amount,
			t1.McCafe,
			t1.SN_CORE,
			t1.Shakes,
			t1.Cold_Drinks,
			t1.Breakfast_food,
			t1.Nuggets,
			t1.Starters_Salad,
			t1.Desserts,
			t1.Non_product,
			t1.Value_Meal,
			t1.Fries,
			t1.SN_EDAP,
			t1.Undefined_Product_Group,
			t1.Condiments,
			t1.Hot_Drinks,
			t1.SN_PREMIU,
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
			t1.week,
			t1.weekday,
			t1.month,
			t1.year,
			t1.regular_weekend_flag,
			t1.weekend_flag,
			t1.Christmas,
			t1.Christmas_Day,
			t1.Day_After_New_Year,
			t1.Day_of_Unity,
			t1.Defendence_of_the_Fatherland,
			t1.International_Womens_Day,
			t1.Labour_Day,
			t1.National_Day,
			t1.New_Year_shift,
			t1.New_year,
			t1.Victory_Day,
			t1.mean_receipt_qty,
			t1.std_receipt_qty,
			t1.mean_sales_qty,
			t1.std_sales_qty,
			t2.n_a		
		from
			public.na_abt8 as t1
		inner join
			public.russca_na_calculation_result as t2
		on
			t1.promo_id = t2.promo_id and
			t1.pbo_location_id = t2.pbo_location_id and
			t1.sales_dt = t2.sales_dt
		where /* Уберем последний день, вдруг там не все чеки */
			t1.sales_dt < &hist_end_dt. 
	;
quit;

proc casutil;
	droptable casdata="na_abt8" incaslib="public" quiet;	
	promote casdata="na_abt9" incaslib="public" outcaslib="public";
	save casdata="na_abt9" incaslib="public" outcaslib="public" replace;
run;


/* proc fedsql sessref=casauto; */
/* 	select  */
/* 		count(1) as na_abt8  */
/* 	from  */
/* 		public.na_abt8 */
/* 	; */
/* 	select distinct */
/* 		channel_cd */
/* 	from */
/* 		public.na_abt8 */
/* 	; */
/* 	select */
/* 		count(distinct promo_id) */
/* 	from */
/* 		public.na_abt8 */
/* 	; */
/* 	select  */
/* 		count(1) as na_abt9  */
/* 	from  */
/* 		public.na_abt9 */
/* 	; */
/* 	select */
/* 		count(distinct promo_id) */
/* 	from */
/* 		public.na_abt9 */
/* 	; */
/* quit; */
/*  */
/* proc sql; */
/* 	select */
/* 		count(1) */
/* 	from */
/* 		nac.russca_na_calculation_result4 */
/* 	; */
/* 	select */
/* 		count(distinct promo_id) */
/* 	from */
/* 		nac.russca_na_calculation_result4; */
/* 	; */
/* 	select distinct */
/* 		t1.* */
/* 	from */
/* 		etl_stg.ia_promo as t1 */
/* 	inner join */
/* 		(select distinct promo_id from nac.russca_na_calculation_result4) as t2 */
/* 	on */
/* 		t1.promo_id = t2.promo_id */
/* 	group by */
/* 		t1.PROMO_MECHANICS */
/* 	; */
/* quit; */


/* куда делось 100к записей? */
/* proc fedsql sessref=casauto; */
/* 	select count(1) as abt8_cnt from public.na_abt8; */
/* 	select count(1) as n_a_calc from public.russca_na_calculation_result; */
/* 	select */
/* 		count(1) as cnt, */
/* 		nmiss(t1.promo_id) as t1_miss, */
/* 		nmiss(t2.promo_id) as t2_miss */
/* 	from */
/* 		public.na_abt8 as t1 */
/* 	full join */
/* 		public.russca_na_calculation_result as t2 */
/* 	on */
/* 		t1.promo_id = t2.promo_id and */
/* 		t1.pbo_location_id = t2.pbo_location_id and */
/* 		t1.sales_dt = t2.sales_dt */
/* 	; */
/* 	select */
/* 		count(1) as cnt */
/* 	from */
/* 		public.na_abt8 as t1 */
/* 	inner join */
/* 		public.russca_na_calculation_result as t2 */
/* 	on */
/* 		t1.promo_id = t2.promo_id and */
/* 		t1.pbo_location_id = t2.pbo_location_id and */
/* 		t1.sales_dt = t2.sales_dt */
/* 	; */
/* 	create table public.strange_promo{option replace=true} as */
/* 		select */
/* 			t2.* */
/* 		from */
/* 			public.na_abt8 as t1 */
/* 		full join */
/* 			public.russca_na_calculation_result as t2 */
/* 		on */
/* 			t1.promo_id = t2.promo_id and */
/* 			t1.pbo_location_id = t2.pbo_location_id and */
/* 			t1.sales_dt = t2.sales_dt */
/* 		where */
/* 			t1.promo_id is missing */
/* 		; */
/* quit; */



/* proc casutil; */
/* 	droptable casdata="na_abt9" incaslib="public" quiet; */
/* 	droptable casdata="discount_receipt" incaslib="public" quiet; */
/* run; */
/*  */
/* Выгружаем таблицу с промо из cas */
/* options casdatalimit=10G; */
/* data work.promo_ml; */
/* 	set public.promo_ml; */
/* run; */
/*  */
/* Отавляем только Discount И добавляем id store */
/* proc sql; */
/* 	create table work.promo_discount as */
/* 		select */
/* 			t1.PROMO_ID, */
/* 			t1.start_dt, */
/* 			t1.end_dt, */
/* 			t1.promo_mechanics, */
/* 			t1.product_id, */
/* 			t1.pbo_location_id, */
/* 			input(t2.PBO_LOC_ATTR_VALUE, best32.) as store_id  */
/* 		from */
/* 			work.promo_ml as t1 */
/* 		inner join ( */
/* 			select distinct */
/* 				PBO_LOCATION_ID, */
/* 				PBO_LOC_ATTR_VALUE */
/* 			from */
/* 				etl_stg.ia_pbo_loc_attributes */
/* 			where */
/* 				PBO_LOC_ATTR_NM='STORE_ID' */
/* 		) as t2 */
/* 		on */
/* 			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID */
/* 		where	 */
/* 			t1.promo_mechanics = 'Discount' */
/* 	; */
/* quit; */
/*  */
/*  */
/* proc sql; */
/* 	create table work.discount_receipt_2020 as */
/* 		select */
/* 			t1.promo_id, */
/* 			t1.sales_dt, */
/* 			t1.pbo_location_id, */
/* 			sum(t1.sum_qty) as sum_qty */
/* 		from ( */
/* 			select */
/* 				datepart(t1.order_date) as sales_dt format DATE9., */
/* 				t2.pbo_location_id, */
/* 				t1.menu_code as product_id, */
/* 				sum(t1.qty, t1.qty_promo) as sum_qty, */
/* 				t2.promo_id */
/* 			from  */
/* 				nac.tda_pmx_2020 as t1 */
/* 			inner join */
/* 				work.promo_discount as t2 */
/* 			on */
/* 				t1.menu_code = t2.product_id and */
/* 				t1.store_id = t2.store_id and */
/* 				datepart(t1.order_date) <= t2.end_dt and */
/* 				datepart(t1.order_date) >= t2.start_dt */
/* 		) as t1 */
/* 		group by */
/* 			t1.promo_id, */
/* 			t1.sales_dt, */
/* 			t1.pbo_location_id */
/* 	; */
/* quit; */
/*  */
/* Добавим 2019 год */
/* proc sql; */
/* 	create table work.discount_receipt_2019 as */
/* 		select */
/* 			t1.promo_id, */
/* 			t1.sales_dt, */
/* 			t1.pbo_location_id, */
/* 			sum(t1.sum_qty) as sum_qty */
/* 		from ( */
/* 			select */
/* 				datepart(t1.order_date) as sales_dt format DATE9., */
/* 				t2.pbo_location_id, */
/* 				t1.menu_code as product_id, */
/* 				sum(t1.qty, t1.qty_promo) as sum_qty, */
/* 				t2.promo_id */
/* 			from  */
/* 				nac.tda_pmx_2019 as t1 */
/* 			inner join */
/* 				work.promo_discount as t2 */
/* 			on */
/* 				t1.menu_code = t2.product_id and */
/* 				t1.store_id = t2.store_id and */
/* 				datepart(t1.order_date) <= t2.end_dt and */
/* 				datepart(t1.order_date) >= t2.start_dt */
/* 		) as t1 */
/* 		group by */
/* 			t1.promo_id, */
/* 			t1.sales_dt, */
/* 			t1.pbo_location_id */
/* 	; */
/* quit; */
/*  */
/* Объеденим таблицы */
/* data work.discount_receipt; */
/* 	set work.discount_receipt_2019 work.discount_receipt_2020; */
/* run; */
/*  */
/* Загружаем данные обратно в cas */
/* data public.discount_receipt; */
/* 	set work.discount_receipt; */
/* run; */
/*  */
/* Оставим в витрине только механику скидки */
/* proc fedsql sessref=casauto; */
/* 	create table public.na_abt8_discount{options replace=true} as */
/* 		select */
/* 			t1.* */
/* 		from */
/* 			public.na_abt8 as t1 */
/* 		inner join ( */
/* 			select distinct */
/* 				t1.promo_id */
/* 			from */
/* 				public.promo_ml as t1 */
/* 			where */
/* 				t1.promo_mechanics = 'Discount' */
/* 		) as t2 */
/* 		on */
/* 			t1.promo_id = t2.promo_id */
/* 	; */
/* quit;	 */
/*  */
/* Добавляем целевую переменную к витрине */
/* proc fedsql sessref=casauto; */
/* 	create table public.na_abt9{options replace=true} as */
/* 		select */
/* 			t1.PROMO_ID, */
/* 			t1.pbo_location_id, */
/* 			t1.sales_dt, */
/* 			t1.promo_lifetime, */
/* 			t1.CHANNEL_CD, */
/* 			t1.NP_GIFT_PRICE_AMT, */
/* 			t1.PROMO_GROUP_ID, */
/* 			t1.bogo, */
/* 			t1.discount, */
/* 			t1.evm_set, */
/* 			t1.non_product_gift, */
/* 			t1.pairs, */
/* 			t1.product_gift, */
/* 			t1.other_discount, */
/* 			t1.other_digital, */
/* 			t1.support, */
/* 			t1.number_of_options, */
/* 			t1.number_of_products, */
/* 			t1.necessary_amount, */
/* 			t1.McCafe, */
/* 			t1.SN_CORE, */
/* 			t1.Shakes, */
/* 			t1.Cold_Drinks, */
/* 			t1.Breakfast_food, */
/* 			t1.Nuggets, */
/* 			t1.Starters_Salad, */
/* 			t1.Desserts, */
/* 			t1.Non_product, */
/* 			t1.Value_Meal, */
/* 			t1.Fries, */
/* 			t1.SN_EDAP, */
/* 			t1.Undefined_Product_Group, */
/* 			t1.Condiments, */
/* 			t1.Hot_Drinks, */
/* 			t1.SN_PREMIU, */
/* 			t1.lvl3_id, */
/* 			t1.lvl2_id, */
/* 			t1.agreement_type, */
/* 			t1.breakfast, */
/* 			t1.building_type, */
/* 			t1.company, */
/* 			t1.delivery, */
/* 			t1.drive_thru, */
/* 			t1.mccafe_type, */
/* 			t1.price_level, */
/* 			t1.window_type, */
/* 			t1.week, */
/* 			t1.weekday, */
/* 			t1.month, */
/* 			t1.year, */
/* 			t1.regular_weekend_flag, */
/* 			t1.weekend_flag, */
/* 			t1.Christmas, */
/* 			t1.Christmas_Day, */
/* 			t1.Day_After_New_Year, */
/* 			t1.Day_of_Unity, */
/* 			t1.Defendence_of_the_Fatherland, */
/* 			t1.International_Womens_Day, */
/* 			t1.Labour_Day, */
/* 			t1.National_Day, */
/* 			t1.New_Year_shift, */
/* 			t1.New_year, */
/* 			t1.Victory_Day, */
/* 			t1.mean_receipt_qty, */
/* 			t1.std_receipt_qty, */
/* 			t1.mean_sales_qty, */
/* 			t1.std_sales_qty, */
/* 			coalesce(t2.sum_qty, 0) as sum_qty */
/* 		from */
/* 			public.na_abt8_discount as t1 */
/* 		left join */
/* 			public.discount_receipt as t2 */
/* 		on */
/* 			t1.promo_id = t2.promo_id and */
/* 			t1.pbo_location_id = t2.pbo_location_id and */
/* 			t1.sales_dt = t2.sales_dt */
/* 	; */
/* quit; */
/*  */
/* proc casutil; */
/* 	promote casdata="na_abt9" incaslib="public" outcaslib="public"; */
/*  */
/* 	droptable casdata="promo_ml" incaslib="public" quiet; */
/* 	droptable casdata="na_abt8" incaslib="public" quiet; */
/* 	droptable casdata="discount_receipt" incaslib="public" quiet; */
/* run; */

/* TODO: по хорошему нужно отфильтровать временно закрытые магазины */
















/*
********************** Прогноз GC при помощи ESM **********************
*/

/* proc casutil; */
/* 	load data=&inlib..ia_pbo_sales_history casout='ia_pbo_sales_history' outcaslib='public' replace; */
/* run; */
/*  */
/* Считаем суммарное количество чеков в день */
/* proc fedsql sessref=casauto; */
/* 	create table public.russca_gc_weekday_sum{options replace=True} as */
/* 		select */
/* 			t1.pbo_location_id, */
/* 			t1.weekday, */
/* 			mean(t1.RECEIPT_QTY) as receipt_qty */
/* 		from ( */
/* 			select */
/* 				pbo_location_id, */
/* 				weekday(datepart(sales_dt)) as weekday, */
/* 				RECEIPT_QTY */
/* 			from */
/* 				public.ia_pbo_sales_history */
/* 			where */
/* 				channel_cd = 'ALL' and */
/* 				datepart(sales_dt) < date'2020-03-01' /*избавляемся от короновируса */
/* 		) as t1 */
/* 		group by */
/* 			t1.pbo_location_id, */
/* 			t1.weekday */
/* 	; */
/* quit; */
/*  */
/* Считаем недельный профиль на всей истории ресторана */
/* proc fedsql sessref=casauto; */
/* 	create table public.russca_gc_weekday_profile{options replace=True} as */
/* 		select */
/* 			t1.pbo_location_id, */
/* 			t1.weekday, */
/* 			t1.receipt_qty / t2.receipt_qty as week_profile */
/* 		from */
/* 			public.russca_gc_weekday_sum as t1 */
/* 		inner join ( */
/* 			select */
/* 				pbo_location_id, */
/* 				sum(receipt_qty) as receipt_qty */
/* 			from */
/* 				public.russca_gc_weekday_sum */
/* 			group by */
/* 				pbo_location_id */
/* 		) as t2 */
/* 		on */
/* 			t1.pbo_location_id = t2.pbo_location_id */
/* 	; */
/* quit; */
/*  */
/* Суммируем число чеков ресторана в неделю */
/* proc fedsql sessref=casauto; */
/* 	create table public.russca_gc_week{options replace=true} as */
/* 		select */
/* 			t1.pbo_location_id, */
/* 			t1.week, */
/* 			sum(RECEIPT_QTY) as RECEIPT_QTY, */
/* 			log(sum(RECEIPT_QTY) * divide(1, sum(week_profile))) as receipt_qty_corr */
/* 			sum(week_profile) as week_profile, */
/* 			count(1) as cnt */
/* 		from ( */
/* 			select */
/* 				t1.pbo_location_id, */
/* 				cast(intnx('week.2',datepart(t1.sales_dt),0,'b') as date)  as week, */
/* 				weekday(datepart(t1.sales_dt)) as weekday, */
/* 				t1.RECEIPT_QTY, */
/* 				t2.week_profile */
/* 			from */
/* 				public.ia_pbo_sales_history as t1 */
/* 			left join */
/* 				public.russca_gc_weekday_profile as t2 */
/* 			on */
/* 				t1.pbo_location_id = t2.pbo_location_id and */
/* 				weekday(datepart(t1.sales_dt)) = t2.weekday */
/* 			where */
/* 				channel_cd = 'ALL' and */
/* 				datepart(sales_dt) < date'2020-03-02' and /*избавляемся от короновируса */
/* 				datepart(sales_dt) >=date'2017-01-02' /*первая дата в истории понедельник */
/* 		) as t1 */
/* 		group by */
/* 			t1.pbo_location_id, */
/* 			t1.week */
/* 	; */
/* quit; */
/*  */
/* Попробуем вставить миссинги в целевую перменную */
/* proc fedsql sessref=casauto; */
/* 	create table public.unique_pbo{options replace=True} as */
/* 		select distinct */
/* 			pbo_location_id */
/* 		from */
/* 			public.russca_gc_week */
/* 	; */
/* 	create table public.unique_week{options replace=True} as */
/* 		select distinct */
/* 			week */
/* 		from */
/* 			public.russca_gc_week */
/* 	; */
/* 	create table public.pbo_week{options replace=True} as */
/* 		select */
/* 			t1.pbo_location_id, */
/* 			t2.week */
/* 		from */
/* 			public.unique_pbo as t1, */
/* 			public.unique_week as t2 */
/* 	; */
/* 	create table public.russca_gc_week{options replace=true} as */
/* 		select */
/* 			t1.pbo_location_id, */
/* 			t1.week, */
/* 			t2.RECEIPT_QTY as RECEIPT_QTY, */
/* 			t2.receipt_qty_corr as receipt_qty_corr */
/* 		from */
/* 			public.pbo_week as t1 */
/* 		left join */
/* 			public.russca_gc_week as t2 */
/* 		on */
/* 			t1.pbo_location_id = t2.pbo_location_id and */
/* 			t1.week = t2.week */
/* 	; */
/* quit; */
/*  */
/* Прогнозируем ВР */
/* proc cas; */
/*    timeData.forecast / */
/*       table={name='russca_gc_week', caslib="public", groupBy={{name='pbo_location_id'}}}, */
/*       timeId={name='week'}, */
/*       interval='week1.2', */
/* 	  mode='SIMPLE', */
/* 	  trimId = 'LEFT', */
/*       dependents={{name='receipt_qty_corr'}}, */
/*       lead=52*2, */
/* 	  seasonality=52, */
/*       forOut={name='salefor', caslib="public", replace=True}, */
/*       infoOut={name='saleinfo', caslib="public", replace=True}, */
/*       indepOut={name='saleindep', caslib="public", replace=True}, */
/*       selectOut={name='saleselect', caslib="public", replace=True}, */
/*       specOut={name='salespec', caslib="public", replace=True}; */
/*    run; */
/*    table.fetch / */
/*       table = {name='saleselect', caslib="public"}; */
/*    table.fetch / */
/*       table = {name='salefor', caslib="public"}; */
/*    run; */
/* quit; */
/*  */
/* Используя недельный профиль разбиваем недельный прогноз */
/* proc fedsql sessref=casauto; */
/* 	create table public.russca_gc_prediction{options replace=True} as */
/* 		select */
/* 			(case */
/* 				when t2.weekday = 1 then cast((t1.week + 6) as date) */
/* 				else cast((t1.week + t2.weekday - 2) as date) */
/* 			end) as sales_dt, */
/* 			t1.PBO_LOCATION_ID, */
/* 			exp(t1.PREDICT) * t2.week_profile as predict, */
/* 			t1.week, */
/* 			t2.weekday, */
/* 			t2.week_profile */
/* 		from */
/* 			public.salefor as t1 */
/* 		left join */
/* 			public.russca_gc_weekday_profile as t2 */
/* 		on */
/* 			t1.pbo_location_id = t2.pbo_location_id */
/* 	; */
/* quit; */
/*  */
/* options casdatalimit=20G; */
/* data nac.russca_gc_prediction; */
/* set public.russca_gc_prediction; */
/* run; */
