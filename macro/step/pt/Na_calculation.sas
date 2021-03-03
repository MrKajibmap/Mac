/*Расчет N_a на истории - нерегламентный процесс*/
/****** 0. Объявим макропеременные ******/
%include '/opt/sas/mcd_config/config/initialize_global.sas';
options mprint nomprintnest nomlogic nomlogicnest symbolgen mcompilenote=all mreplace;
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
options casdatalimit=10G;
%let filter = t1.channel_cd = 'ALL'; 
%let hist_start_dt = date '2019-01-01';
%let hist_end_dt =  date '2020-09-30';

libname nac "/data/MN_CALC";

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
			t1.option_number,
			t1.product_qty,
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
			t3.option_number,
			t3.product_qty,
			t3.product_LEAF_ID as product_id,
			t2.PBO_LEAF_ID as pbo_location_id,
			t1.PROMO_NM,
/* 			t1.PROMO_PRICE_AMT, */
			datepart(t1.START_DT) as start_dt,
			datepart(t1.END_DT) as end_dt,
			t1.CHANNEL_CD,
/* 			t1.NP_GIFT_PRICE_AMT, */
			t1.PROMO_MECHANICS
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
	create table public.promo_ml_filter{options replace=true} as
		select
			t1.PROMO_ID,
			t1.option_number,
			t1.product_qty,
			t1.product_ID,
			t1.PBO_location_ID,
			t1.PROMO_NM,
			t1.start_dt,
			t1.end_dt,
			t1.CHANNEL_CD,
			t1.PROMO_MECHANICS
		from
			public.promo_ml as t1
		where
			start_dt >= &hist_start_dt. and
			end_dt <= &hist_end_dt. and
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
data work.promo_ml_filter;
	set public.promo_ml_filter;
run;

/* Меняем ID ресторнов */
proc sql;
	create table work.promo_ml_filter2 as 
		select
			t1.PROMO_ID,
			t1.option_number,
			t1.product_qty,
			t1.product_ID,
			input(t2.PBO_LOC_ATTR_VALUE, best32.) as pbo_location_id,
			t1.PROMO_NM,
			t1.start_dt,
			t1.end_dt,
			t1.CHANNEL_CD,
			t1.PROMO_MECHANICS
		from
			work.promo_ml_filter as t1
		inner join (
			select distinct
				PBO_LOCATION_ID,
				PBO_LOC_ATTR_VALUE
			from
				etl_stg2.ia_pbo_loc_attributes
			where
				PBO_LOC_ATTR_NM='STORE_ID'
		) as t2
		on
			t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
	;
quit;


%macro prepare_receipt_data(promo, promo_start, promo_end);

/* 
	Макрос подготоваливает таблицы с чеками. На вход поступает ID промо акции.
		1. Фильтруем чековые данные в интервале действия промо.
		2. Создаем нормальный ID чека
		3. Объединяем чеки 2019 и 2020 годов в одну табицу

	Задача макроса облегчить работу с чековыми данными для подсчета Na.

*/

	/* Стираем временные таблицы */
	proc datasets library=nac;
		delete discount_promo_filter;
		delete russca_receipt_filter_2019;
		delete russca_receipt_filter_2020;
		delete russca_receipt_filter;
		delete russca_receipt_filter_id;
	run;
	
	/* Фильтруем промо таблицу */
	proc sql;
		create table nac.discount_promo_filter as 
			select
				t1.PROMO_ID,
				t1.product_ID,
				t1.pbo_location_id,
				t1.PROMO_NM,
				t1.start_dt,
				t1.end_dt,
				t1.CHANNEL_CD,
				t1.PROMO_MECHANICS
			from
				work.promo_ml_filter2 as t1
			where
				promo_id = &promo.
		;
	quit;
	
	/* Получаем даты проведения промо акции + аналогичный период в прошлом */
/* 	proc sql noprint; */
/* 		select distinct  */
/* 			start_dt, */
/* 			end_dt */
/* 		into :promo_start, :promo_end */
/* 	    from */
/* 			nac.discount_promo_filter; */
/* 	quit; */
	
/* 	%put promo_start = "&promo_start."d; */
/* 	%put promo_end = "&promo_end."d; */

	/* Меняем формат на числовой */
/* 	%let promo_start_num = %sysfunc(INPUTN(&promo_start., date9.)); */
/* 	%let promo_end_num = %sysfunc(INPUTN(&promo_end., date9.)); */

	
	/* Фильтруем чеки 2019 года по датам промо акции */
	%if %eval(&promo_start. < %sysfunc(INPUTN('1jan2020'd, date9.))) %then %do;
		proc sql;
			create table nac.russca_receipt_filter_2019 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					etl_stg2.tda_pmx_2019 as t1
				where
					datepart(t1.order_date) <= &promo_end. and
					datepart(t1.order_date) >= &promo_start. 
			;
		quit;
	%end;
	%else %do;
		proc sql;
			create table nac.russca_receipt_filter_2019 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					etl_stg2.tda_pmx_2019(obs=0) as t1
			;
		quit;
	%end;

	/* Фильтруем чеки 2020 года по датам промо акции */
	%if %eval(&promo_end. >= %sysfunc(INPUTN('1jan2020'd, date9.))) %then %do;
		proc sql;
			create table nac.russca_receipt_filter_2020 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					etl_stg2.tda_pmx_2020 as t1
				where
					datepart(t1.order_date) <= &promo_end. and
					datepart(t1.order_date) >= &promo_start. 
			;
		quit;
	%end;
	%else %do;
		proc sql;
			create table nac.russca_receipt_filter_2020 as
				select
					t1.order_number,
					t1.STORE_ID as pbo_location_id,
					t1.menu_code as product_id,
					datepart(t1.order_date) as sales_dt format date9.,
					t1.qty,
					t1.qty_promo
				from
					etl_stg2.tda_pmx_2020(obs=0) as t1
			;
		quit;
	%end;

	/* 	Объединяем результаты, чтобы не пропустить переходящие промо из года в год */
	data nac.russca_receipt_filter;
		set nac.russca_receipt_filter_2019 nac.russca_receipt_filter_2020;
	run;
	
	/* 	Сортируем таблицу */
	proc sort data=nac.russca_receipt_filter;
		by order_number pbo_location_id sales_dt;
	run;
	
	/* 	Создаем уникальный ID чека */
	data nac.russca_receipt_filter_id;
		set nac.russca_receipt_filter;
		by order_number pbo_location_id sales_dt;
		if first.sales_dt then receipt_id+1;
	run;

	/* Стираем временные таблицы */
	proc datasets library=nac;
		delete discount_promo_filter;
		delete russca_receipt_filter_2019;
		delete russca_receipt_filter_2020;
		delete russca_receipt_filter;
	run;
	
%mend;


%macro evm(promo, option_number);
	/*
		Макрос, который считает N_a для механики промо EVM/Set
		Алгоритм:
			1. Фильтруем таблицу с промо
			2. Пересекаем чеки с промо таблицей, считая сумму в рамках каждой позиции
			3. Если число поцизий в чеке = число позиций в промо, то N_a = min(среди всех позиций чека)
	*/
	proc datasets library=work;
		delete one_promo;
		delete promo_receipt;
		delete receipt_options;
		delete evm_na;
	run;
	
	/* Фильтруем чеки */
	proc sql;
		create table work.one_promo as
			select
				promo_id,
				option_number,
				product_qty,
				product_id,
				pbo_location_id,
				promo_nm,
				start_dt,
				end_dt,
				channel_cd,
				promo_mechanics
			from
				work.promo_ml_filter2
			where
				promo_id = &promo.
		;
	quit;

	/* Считаем необходимое количество позиций в промо */
/* 	proc sql noprint; */
/* 		select  */
/* 			max(option_number) */
/* 		into :option_number */
/* 	    from */
/* 			work.one_promo */
/* 		; */
/* 	quit; */
	
	%put option_number = &option_number.;

	/* Пересекаем с чеками */
	proc sql;
		create table work.promo_receipt as
			select
				t1.receipt_id,
				t1.sales_dt,
				t1.pbo_location_id,
				t2.option_number,
				t2.product_qty,
				sum(sum(t1.qty), sum(t1.qty_promo)) as sum_qty
			from
				nac.russca_receipt_filter_id as t1
			inner join
				work.one_promo as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id
			group by
				t1.receipt_id,
				t1.sales_dt,
				t1.pbo_location_id,
				t2.option_number,
				t2.product_qty
		;
	quit;

	/* Считаем число позиций в чеке и минимальное число товара в позиции */
	proc sql;
		create table work.receipt_options as
			select
				t1.receipt_id,
				count(distinct t1.option_number) as number_of_options,
				min(divide(t1.sum_qty, t1.product_qty)) as n_a
			from
				work.promo_receipt as t1
			where
				divide(t1.sum_qty, t1.product_qty) >= 1 /* Убираем позиции, где было куплено недостаточно товара */
			group by
				t1.receipt_id
		;	
	quit;
	
	/* Считаем N_a */
	proc sql;
		create table work.evm_na as 
			select
				&promo. as promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				sum(t2.n_a) as n_a
			from
				work.promo_receipt as t1
			inner join
				(select * from work.receipt_options where number_of_options = &option_number.) as t2
			on
				t1.receipt_id = t2.receipt_id
			group by
				t1.pbo_location_id,
				t1.sales_dt
		;
	quit;

	/* 	Добавляем результат к итоговой таблице */
	proc append base=nac.russca_na_calculation_result4
		data = work.evm_na force;
	run;
	
%mend;

/* Стираем итоговую таблицу */
proc datasets library=nac;
	delete russca_na_calculation_result4;
run;

/* Создаем список промо акций, которые описываются скриптом evm */
proc sql;
	create table work.unique_evm_like_promo as
		select
			put(PROMO_ID, 8.) as promo_id,
			PROMO_NM,
			put(start_dt,8.) as start_dt,
			put(end_dt,8.) as end_dt,
			CHANNEL_CD,
			PROMO_MECHANICS,
			put(max(option_number),8.) as max_option_number
		from
			work.promo_ml_filter2
		where
			promo_mechanics in (
				'BOGO / 1+1',
				'Discount',
				'EVM/Set',
				'NP Promo Support',
				'Non-Product Gift',
				'Pairs'
			)
		group by
			PROMO_ID,
			PROMO_NM,
			start_dt,
			end_dt,
			CHANNEL_CD,
			PROMO_MECHANICS

	;
quit;


data _null_;
    set work.unique_evm_like_promo;
    call execute('%prepare_receipt_data('||promo_id||','||start_dt||','||end_dt||')');
    call execute('%evm('||promo_id||','||max_option_number||')');
run;





/* 
	Общий алгоритм такой:
		В цикле по всем промо в канале ALL:
			1. Фильтруем чековые данные в интервале действия промо (макрос готов)
			2. В зависимости от механики промо вызываем соответсвующий макрос,
				который возвращает таблицу promo_id|sales_dt|N_a
			3. Складываем результат в итоговую таблицу
*/


/*
	Карина, наша с тобой задача написать макросы по подсчету N_a,
	приведу шаблон такого макроса. Всего будет макросов столько, сколько 
	имеется промо механик
*/

/* %macro discount_promo(promo);  называем скрипт по типу механики */
/*
	Скрипт принимает на вход ID промо акции (предполагается, что это правильный ID,
	т.е. для данному скрипту будут приходить только ID акции с механикой discount).	
	Предполагается, что до вызова этого макроса будет вызван макрос prepare_receipt_data с
	тем же ID промо. Таким образом, мы будем работать с таблицей tmp.russca_receipt_filter_id
	в которой есть поля:
		* order_number - ненужное поле
		* pbo_location_id - id Ресторана
		* product_id - id товара
		* sales_dt - дата
		* qty - число купленного товара
		* qty_promo - число купленного товара в промо (по-идее мы должны суммировать qty и qty_promo)
		* receipt_id - id чека
	И в нашем распоряжении есть таблица work.promo_ml_filter2,
		которая описывает на какие товары/ресторны распространяется промо акция.
	
	Наша задача получить на выходе таблицу:
		promo_id|sales_dt|N_a
	
	На что стоит обратить внимание:
		* Механики промо в чеке работают по разному. Как понять правило срабатывания промо акции
		в чеке? Посмотреть по таблицам промо ia_promo, ia_promo_x_product названия промо акции (обычно
		название промо много говорит о механике), список товаров на которые действует промо,
		сколько позиций должно быть в чеке, чтобы сработало промо. Если будут вопросы спрашивай у меня или
		Димы правила срабатывания промо акций
		* N_a - это число срабатываний промо акции. Т.е. в механике discount это промо сумма купленного промо товара.
		Но если это EVM, то это будет количество купленных наборов товаров в чеке и это разные вещи.


%mend; */
