/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в cas (public) для последующих расчетов в рамках 
*		сквозного процесса для оперпрогноза
*
*  ПАРАМЕТРЫ:
*	  Нет
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
*    %rtp_load_data;
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
****************************************************************************/

/*
	Схема программы:
	
	0. Объявление макропеременных: первый и последний день в истории, фильтрация по категориям
		товаров и каналам.
	1. Сбор "каркаса" из таблиц ia_pmix и ia_pmix_history с учетом фильтра.
	2. Добавление цен.
	3. Протягиваем временные ряды на 91 день с последнего дня в истории. Целевую переменную
		заполняем пропусками, цены заполняем предыдущим значением.
	4. Фильтрация:
		* Убираем временные закрытия ПБО.
		* Убираем закрытые магазины из справочника ПБО.
		* Убираем из истории пропуски в продажах (появившиеся после протяжки временных рядов).
		* PLM, убираем выведенные товары (пока на паузе).
		* Пересекаем с ассортиментной матрицей скоринговую витрину.
	5. Подсчет лагов.
	6. Добавление промо:
		* Бинарные флаги промо по промо механикам.
		* Флаг side_promo для регулярного товара вне промо.
		* Скидка?
	7. Добавляем мароэкономику (проверить заполненность на будущее).
	8. Добавляем погоду (проверить заполненность на будещее).
	9. Добавляем trp конкурентов.
	10. Добавляем медиаподдержку.
	11. Добавим атрибуты товаров. Дополнительно перекодируем текстовые переменные.
	12. Добавляем атрибуты ПБО. Дополнительно перекодируем текстовые переменные.
	13. Добавляем календарные признаки.
	14. Добавляем события. Можно забить на те события, которые нам передают сейчас, а
		просто взять и использовать, те события, которые Лиза считает существенными.
		*** Взять день города! ***
	15. Добавим ценовые ранги.
	16. Перекодируем channel_cd (текстовое поле на число). Делаем это в последнюю очередб
		 для удобства соединения таблиц.
	17. Разделяем витрину на обучение и скоринг. 

	Что еще хочется видеть в витрине?
	* Закодировать ID ПБО и товаров через агрегаты целевой переменной
	* Придумать признаков по промо. 
*/
%macro rtp_load_data;
	/****** 0. Объявление макропеременных ******/
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
	 cas casauto;
	 caslib _all_ assign;
	%end;
	 /* Объявление макропеременных */
	%local lmvInLib;
	%let lmvInLib=ETL_IA;
	%let lmvFilter = t1.channel_cd = 'ALL'; 
	%let lmvHistStartDt = date'2017-01-02';
	%let lmvHistEndDt =  date'2020-05-24';

	/****** 1. Сбор "каркаса" из таблиц ia_pmix и ia_pmix_history с учетом фильтра ******/
	/* Сначала собираем справочник товаров для того, чтобы создать фильтр */
	proc casutil;
	  droptable casdata="product_dictionary_ml" incaslib="public" quiet;
	run;
	
	data CASUSER.product (replace=yes);
		set &lmvInLib..product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_HIERARCHY (replace=yes);
		set &lmvInLib..product_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_ATTRIBUTES (replace=yes);
		set &lmvInLib..product_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc cas;
	transpose.transpose /
	   table={name="product_attributes", caslib="casuser", groupby={"product_id"}} 
	   attributes={{name="product_id"}} 
	   transpose={"PRODUCT_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PRODUCT_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto;
	   create table casuser.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from casuser.product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
			;
	quit;

	proc fedsql sessref=casauto;
	   create table casuser.product_dictionary_ml{options replace=true} as
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
	   from casuser.product_hier_flat t1
	   left join casuser.attr_transposed t3
		on t1.product_id=t3.product_id
	   left join casuser.product t15
		on t1.product_id=t15.product_id
	   left join casuser.product t14
		on t1.lvl4_id=t14.product_id
	   left join casuser.product t13
		on t1.lvl3_id=t13.product_id
	   left join casuser.product t12
		on t1.lvl2_id=t12.product_id;
	quit;

	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_hero);
	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_item_size);
	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_offer_type);
	%text_encoding(mpTable=casuser.product_dictionary_ml, mpVariable=a_price_tier);

	proc casutil;
	  droptable casdata='product' incaslib='casuser' quiet;
	  /* droptable casdata='product_HIERARCHY' incaslib='casuser' quiet; */
	  droptable casdata='product_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='product_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;

	/* Подготовка таблицы с продажами */
	data CASUSER.product (replace=yes);
		set &lmvInLib..pmix_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto; 
		create table casuser.abt1_ml{options replace=true} as
		select 
			t1.PBO_LOCATION_ID,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			t1.sum_qty
		from (
			select 
				coalesce(t1.PBO_LOCATION_ID, t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
				coalesce(t1.PRODUCT_ID, t2.PRODUCT_ID) as PRODUCT_ID,
				coalesce(t1.CHANNEL_CD, t2.CHANNEL_CD) as CHANNEL_CD,
				coalesce(t1.SALES_D, t2.SALES_D) as SALES_DT,
				coalesce(t1.SALES_QTY, t2.SALES_QTY, 0) + coalesce(t1.SALES_QTY_PROMO, t2.SALES_QTY_PROMO, 0) as sum_qty
			from (
				select *
				from casuser.pmix_sales 
			) t1
		left join
			 casuser.product_dictionary_ml as t2 
		on
			t1.product_id = t2.product_id
		where
			&lmvFilter and
			t1.SALES_DT >= &lmvHistStartDt and
			t1.SALES_DT <= &lmvHistEndDt 
	;
	quit;

	/****** 2. Добавление цен ******/
	/* proc casutil;
	  droptable casdata="price_ml" incaslib="public" quiet;
	  droptable casdata="abt2_ml" incaslib="public" quiet;
	run; */

	data CASUSER.price (replace=yes);
		set &lmvInLib..price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto; 
		/* Объединяем историю с актуальными данными */
		create table casuser.price_ml{options replace=true} as
			select 
				t1.PBO_LOCATION_ID
				,t1.PRODUCT_ID
				,t1.start_dt
				,t1.end_dt
				,t1.GROSS_PRICE_AMT
				,t1.NET_PRICE_AMT
				,t1.PRICE_TYPE
			from 
				casuser.price as t1
		;
	quit;

	/* Добавляем к продажам цены */
	proc fedsql sessref=casauto; 
		create table casuser.abt2_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				max(t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT
			from
				casuser.abt1_ml as t1
				left join casuser.price_ml as t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= t2.end_dt and   
				t1.SALES_DT >= t2.start_dt
			group by 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty
			;
	quit;

	proc casutil;
	  droptable casdata="price_ml" incaslib="casuser" quiet;
	  droptable casdata="price" incaslib="casuser" quiet;
	  droptable casdata="abt1_ml" incaslib="casuser" quiet;
	/*   promote casdata="abt2_ml" incaslib="public" outcaslib="public"; */
	run;


	/****** 3. Протяжка временных рядов ******/
	proc casutil;
	  droptable casdata="abt3_ml" incaslib="casuser" quiet;
	run;

	%let fc_end_sas=%sysfunc(inputn(%scan(%bquote(&lmvHistEndDt),2,%bquote(' )),yymmdd10.));
	%let fc_end= %sysfunc(intnx(day,&fc_end_sas, 91),yymmddd10.);

	proc cas;
	timeData.timeSeries result =r /
		series={
			{name="sum_qty", setmiss="MISSING"},
			{name="GROSS_PRICE_AMT", setmiss="PREV"}
		}
		tEnd= "&fc_end"
		table={
			caslib="casuser",
			name="abt2_ml",
			groupby={"PBO_LOCATION_ID","PRODUCT_ID", "CHANNEL_CD"}
		}
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="casuser", name="abt3_ml", replace=True}
		;
	run;
	quit;

	proc casutil;
		  droptable casdata="abt2_ml" incaslib="casuser" quiet;
	/*   promote casdata="abt3_ml" incaslib="casuser" outcaslib="public"; */
	run;


	/****** 4. Фильтрация ******/

	/* Проверки. Аня писала, что могут буть дубли. Вставил дополнительную проверку */
	%macro check(table, name);
		proc fedsql sessref=casauto;
			select
				count(1) as &name.
			from
				(
				select
					pbo_location_id,
					product_id,
					sales_dt,
					channel_cd,
					count(sales_dt) as cnt
				from
					casuser.&table.
				group by
					pbo_location_id,
					product_id,
					sales_dt,
					channel_cd
				) as t1
			where
				t1.cnt > 1
			;
		quit;
	%mend;

	/* 4.1 Убираем временные закрытия ПБО */
	proc casutil;
		droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
		droptable casdata="abt4_ml" incaslib="casuser" quiet;
	run;
	
	data CASUSER.pbo_close_period (replace=yes);
		set &lmvInLib..pbo_close_period(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	/* заполняем пропуски в end_dt */
	proc fedsql sessref=casauto;
		create table casuser.pbo_closed_ml {options replace=true} as
			select 
				CHANNEL_CD,
				PBO_LOCATION_ID,
				datepart(start_dt) as start_dt,
				coalesce(datepart(end_dt), date '2100-01-01') as end_dt,
				CLOSE_PERIOD_DESC
			from
				casuser.pbo_close_period
		;
	quit;

	/* Удалаем даты закрытия pbo из abt */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt3_ml as t1
			left join
				casuser.pbo_closed_ml as t2
			on
				t1.sales_dt >= t2.start_dt and
				t1.sales_dt <= t2.end_dt and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.channel_cd = t2.channel_cd
			where
				t2.pbo_location_id is missing
		;
	quit;

	%check(abt4_ml, check4_1);

	/* 4.2 Убираем закрытые насовсем магазины */
	proc casutil;
		droptable casdata="closed_pbo" incaslib="casuser" quiet;
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc cas;
	transpose.transpose /
	   table={name="PBO_LOC_ATTRIBUTES", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	/* Преобразовываем даты открытия и закрытия магазинов */
	proc fedsql sessref=casauto;
		create table casuser.closed_pbo{options replace=true} as 
			select distinct
				pbo_location_id,
				cast(inputn(A_OPEN_DATE,'ddmmyy10.') as date) as OPEN_DATE,
				coalesce(
					cast(inputn(A_CLOSE_DATE,'ddmmyy10.') as date),
					date '2100-01-01'
				) as CLOSE_DATE
			from casuser.attr_transposed
		;
	quit;

	/* Удаляем закрытые насовсем магазины  */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt4_ml as t1
			left join
				casuser.closed_pbo as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt >= t2.OPEN_DATE and
				t1.sales_dt <= t2.CLOSE_DATE
			where
				t2.pbo_location_id is not missing
		;
	quit;

	%check(abt4_ml, check4_2);

	/* 4.3 Убираем из истории пропуски в продажах */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.abt4_ml as t1
			where 
				(t1.sum_qty is not missing and t1.sales_dt <= &lmvHistEndDt.) or
				(t1.sales_dt > &lmvHistEndDt.)
		;
	quit;

	%check(abt4_ml, check4_3);

	/* 4.4 Пересекаем с ассортиментной матрицей скоринговую витрину */
	data CASUSER.assort_matrix (replace=yes);
		set &lmvInLib..assort_matrix(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml {options replace = true} as	
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT					
			from
				casuser.abt4_ml as t1
			left join
				casuser.assort_matrix  t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= datepart(t2.end_dt) and 
				t1.SALES_DT >= datepart(t2.start_dt)
			where	
				(t1.SALES_DT <= &lmvHistEndDt) or 
				(t2.PBO_LOCATION_ID is not missing)
		;
	quit;

	%check(abt4_ml, check4_4);

	proc casutil;
		droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
		droptable casdata="closed_pbo" incaslib="casuser" quiet;
		droptable casdata="assort_matrix" incaslib="casuser" quiet;
		droptable casdata="pbo_close_period" incaslib="casuser" quiet;
	/*	droptable casdata="PBO_LOC_ATTRIBUTES" incaslib="casuser" quiet;*/
		droptable casdata="attr_transposed" incaslib="casuser" quiet;
		droptable casdata="abt3_ml" incaslib="casuser" quiet;
	/* 	promote casdata="abt4_ml" incaslib="public" outcaslib="public"; */
	run;


	/****** 5. Подсчет лагов ******/
	proc casutil;
	  droptable casdata='lag_abt1' incaslib='casuser' quiet;
	  droptable casdata='lag_abt2' incaslib='casuser' quiet;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata='abt5_ml' incaslib='casuser' quiet;
	run;

	/* считаем медиану и среднее арифметическое */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
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
			  %let intnm=%day_interval_name(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._avg[t]=mean(%transform_var_to_array_dlm(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
				lag_&intnm._med[t]=median(%transform_var_to_array_dlm(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
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
			name ='abt4_ml',
			caslib = 'casuser', 
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
			  %let intnm=%day_interval_name(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._std[t]=std(%transform_var_to_array_dlm(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
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
				name ='abt4_ml',
				caslib = 'casuser', 
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
				  %let intnm=%day_interval_name(&window);        /*название интервала окна; 7->week итд */
				  %let intnm=%sysfunc(strip(&intnm.));
				  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
					lag_&intnm._pct10[t]=pctl(10,%transform_var_to_array_dlm(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
					lag_&intnm._pct90[t]=pctl(90,%transform_var_to_array_dlm(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
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
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				casuser.abt4_ml as t1,
				casuser.lag_abt1 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				casuser.abt5_ml as t1,
				casuser.lag_abt2 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				casuser.abt5_ml as t1,
				casuser.lag_abt3 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc casutil;
	/*   promote casdata="abt5_ml" incaslib="casuser" outcaslib="public"; */
	  droptable casdata='lag_abt1' incaslib='casuser' quiet;
	  droptable casdata='lag_abt2' incaslib='casuser' quiet;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata="abt4_ml" incaslib="casuser" quiet;
	run;


	/****** 6. Добавление промо ******/
	proc casutil;
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="pbo_lvl_all" incaslib="public" quiet;
		droptable casdata="product_lvl_all" incaslib="public" quiet;
		droptable casdata="promo_ml" incaslib="casuser" quiet;
		droptable casdata="promo_transposed" incaslib="casuser" quiet;
		droptable casdata="abt6_ml" incaslib="casuser" quiet;
		droptable casdata="promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_x_pbo_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_ml_main_code" incaslib="casuser" quiet;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
	run;
	
	data CASUSER.pbo_loc_hierarchy (replace=yes);
		set &lmvInLib..pbo_loc_hierarchy(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.promo (replace=yes);
		set &lmvInLib..promo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.promo_x_pbo (replace=yes);
		set &lmvInLib..promo_x_pbo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.promo_x_product (replace=yes);
		set &lmvInLib..promo_x_product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table casuser.pbo_hier_flat{options replace=true} as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from casuser.pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
				(select * from casuser.pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
		create table casuser.lvl4{options replace=true} as 
			select 
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select 
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select 
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
	quit;

	/* Соединяем в единый справочник ПБО */
	data casuser.pbo_lvl_all;
		set casuser.lvl4
			casuser.lvl3 
			casuser.lvl2 
			casuser.lvl1
		;
	run;

	/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
	   create table casuser.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from casuser.product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
		;
		create table casuser.lvl5{options replace=true} as 
			select 
				product_id as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl4{options replace=true} as 
			select 
				LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select 
				LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select 
				LVL2_ID as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
		create table casuser.lvl1{options replace=true} as 
			select 
				1 as product_id,
				product_id as product_leaf_id
			from
				casuser.product_hier_flat
		;
	quit;

	/* Соединяем в единый справочник ПБО */
	data casuser.product_lvl_all;
		set casuser.lvl5
			casuser.lvl4 
			casuser.lvl3 
			casuser.lvl2 
			casuser.lvl1
		;
	run;

	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table casuser.promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				casuser.promo_x_pbo as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table casuser.promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.product_LEAF_ID
			from
				casuser.promo_x_product as t1,
				casuser.product_lvl_all as t2
			where t1.product_id = t2.product_id
		;
		create table casuser.promo_ml{options replace = true} as 
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
				(case
					when t1.PROMO_MECHANICS = 'BOGO / 1+1' then 'bogo'
					when t1.PROMO_MECHANICS = 'Discount' then 'discount'
					when t1.PROMO_MECHANICS = 'EVM/Set' then 'evm_set'
					when t1.PROMO_MECHANICS = 'Non-Product Gift' then 'non_product_gift'
					when t1.PROMO_MECHANICS = 'Pairs' then 'pairs'
					when t1.PROMO_MECHANICS = 'Product Gift' then 'product_gift'
					when t1.PROMO_MECHANICS = 'Other: Discount for volume' then 'other_promo'
					when t1.PROMO_MECHANICS = 'NP Promo Support' then 'support'
				end) as promo_mechanics_name,
				1 as promo_flag		
			from
				casuser.promo as t1 
			left join
				casuser.promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID 
		;
	quit;
		
	proc fedsql sessref=casauto;
		create table casuser.promo_ml_main_code{options replace = true} as 
			select distinct
				(MOD(t2.LVL4_ID, 10000)) AS product_MAIN_CODE,
				t1.PBO_LEAF_ID,
				datepart(t1.START_DT) as start_dt,
				datepart(t1.END_DT) as end_dt,
				t1.CHANNEL_CD,
				case
					when product_LEAF_ID = MOD(t2.LVL4_ID, 10000) then 0
					else 1
				end as side_promo_flag
					
			from
				casuser.promo_ml as t1 
			left join
				casuser.product_hier_flat as t2
			on 
				t1.product_LEAF_ID = t2.product_id
		;
	quit;

	/* транспонируем таблицу с промо по типам промо механк */
	proc cas;
	transpose.transpose /
		table = {
			name="promo_ml",
			caslib="casuser",
			groupby={"promo_id", "product_LEAF_ID", "PBO_LEAF_ID", "CHANNEL_CD", "START_DT", "END_DT"}}
		transpose={"promo_flag"} 
		id={"promo_mechanics_name"} 
		casout={name="promo_transposed", caslib="casuser", replace=true};
	quit;

	/* Соединяем с витриной */
	proc fedsql sessref = casauto;
		/* Подготоваливаем таблицу для джойна с витриной */
		create table casuser.abt_promo{options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				max(coalesce(t2.other_promo, 0)) as other_promo,  
				max(coalesce(t2.support, 0)) as support,
				max(coalesce(t2.bogo, 0)) as bogo,
				max(coalesce(t2.discount, 0)) as discount,
				max(coalesce(t2.evm_set, 0)) as evm_set,
				max(coalesce(t2.non_product_gift, 0)) as non_product_gift,
				max(coalesce(t2.pairs, 0)) as pairs,
				max(coalesce(t2.product_gift, 0)) as product_gift,
				max(coalesce(t3.side_promo_flag, 0)) as side_promo_flag
			from
				casuser.abt5_ml as t1
			left join
				casuser.promo_transposed as t2
			on
				t1.product_id = t2.product_LEAF_ID and
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.SALES_DT <= t2.END_DT and
				t1.SALES_DT >= t2.START_DT
			left join
				casuser.promo_ml_main_code as t3
			on
				t1.product_id = t3.product_MAIN_CODE and
				t1.pbo_location_id = t3.PBO_LEAF_ID and
				t1.SALES_DT <= t3.END_DT and
				t1.SALES_DT >= t3.START_DT
			group by
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT
		;
		/* Добавляем промо к витрине */
		create table casuser.abt6_ml{options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t2.other_promo,  
				t2.support,
				t2.bogo,
				t2.discount,
				t2.evm_set,
				t2.non_product_gift,
				t2.pairs,
				t2.product_gift,
				t2.side_promo_flag 
			from
				casuser.abt5_ml as t1
			left join
				casuser.abt_promo as t2
			on
				t1.product_id = t2.product_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.SALES_DT = t2.SALES_DT and
				t1.CHANNEL_CD = t2.CHANNEL_CD
		;
	quit;

	proc casutil;
	/* 	promote casdata="abt6_ml" incaslib="casuser" outcaslib="public"; */
		promote casdata="pbo_lvl_all" incaslib="casuser" outcaslib="public";
		promote casdata="product_lvl_all" incaslib="casuser" outcaslib="public";

		droptable casdata="pbo_hier_flat" incaslib="public" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="public" quiet;
		droptable casdata="lvl4" incaslib="public" quiet;
		droptable casdata="lvl3" incaslib="public" quiet;
		droptable casdata="lvl2" incaslib="public" quiet;
		droptable casdata="lvl1" incaslib="public" quiet;
		droptable casdata="product_hierarchy" incaslib="casuser" quiet;
		droptable casdata="promo_ml" incaslib="public" quiet;
		droptable casdata="promo_transposed" incaslib="casuser" quiet;
		droptable casdata="promo_x_product_leaf" incaslib="public" quiet;
		/* droptable casdata="promo_x_pbo_leaf" incaslib="public" quiet; */
		droptable casdata="promo_ml_main_code" incaslib="public" quiet;
		droptable casdata="abt_promo" incaslib="public" quiet;
		droptable casdata="abt5_ml" incaslib="public" quiet;
	run;



	/****** 7. Добавляем мароэкономику ******/
	proc casutil;
	  droptable casdata="macro_ml" incaslib="public" quiet;
	  droptable casdata="macro2_ml" incaslib="public" quiet;
	  droptable casdata="macro_transposed_ml" incaslib="public" quiet;
	  droptable casdata="abt7_ml" incaslib="public" quiet;
	  load data=&lmvInLib..macro_factor casout='macro' outcaslib='public' replace;
	run;
	
	data CASUSER.macro (replace=yes);
		set &lmvInLib..macro_factor(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	proc fedsql sessref=casauto;
		create table casuser.macro_ml{options replace=true} as 
			select 
				factor_cd,
				datepart(cast(REPORT_DT as timestamp)) as period_dt,
				FACTOR_CHNG_PCT
			from casuser.macro;
	quit;

	data casuser.macro2_ml;
	  format period_dt date9.;
	  drop pdt;
	  set casuser.macro_ml(rename=(period_dt=pdt));
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
	   table={name="macro2_ml", caslib="casuser", groupby={"period_dt"}} 
	   attributes={{name="period_dt"}} 
	   transpose={"FACTOR_CHNG_PCT"} 
	   prefix="A_" 
	   id={"factor_cd"} 
	   casout={name="macro_transposed_ml", caslib="casuser", replace=true};
	quit;

	/* Соединяем с ABT */
	proc fedsql sessref = casauto;
		create table casuser.abt7_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t2.A_CPI,
				t2.A_GPD,
				t2.A_RDI
			from
				casuser.abt6_ml as t1 left join 
				casuser.macro_transposed_ml as t2
			on
				t1.sales_dt = t2.period_dt
		;
	quit;

	proc casutil;
	  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
	  droptable casdata="macro2_ml" incaslib="casuser" quiet;
	  droptable casdata="macro" incaslib="casuser" quiet;
	  droptable casdata="macro_ml" incaslib="casuser" quiet;
	  droptable casdata="abt6_ml" incaslib="casuser" quiet;
	/*   promote casdata="abt7_ml" incaslib="casuser" outcaslib="public"; */
	run;


	/***** 8. Добавляем погоду. *****/
	proc casutil;
	  droptable casdata = "abt8_ml" incaslib = "public" quiet;
	run;
	
	data CASUSER.weather (replace=yes);
		set &lmvInLib..weather(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc fedsql sessref =casauto;
		create table casuser.abt8_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t2.TEMPERATURE,
				t2.PRECIPITATION
			from
				casuser.abt7_ml as t1
			left join
				casuser.weather as t2
			on 
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = datepart(t2.REPORT_DT)
		;
	quit;

	proc casutil;
	  droptable casdata="weather" incaslib="public" quiet;
	  droptable casdata="abt7_ml" incaslib="public" quiet;
	/*   promote casdata="abt8_ml" incaslib="casuser" outcaslib="public"; */
	run;


	/***** 9. Добавляем trp конкурентов *****/
	proc casutil;
		/*droptable casdata="comp_media_ml" incaslib="public" quiet;*/
		/*droptable casdata="abt9_ml" incaslib="public" quiet;*/
	run;
	
	data CASUSER.comp_media (replace=yes);
		set &lmvInLib..comp_media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto;
		create table casuser.comp_media_ml{options replace=true} as 
			select
				COMPETITOR_CD,
				TRP,
				datepart(cast(report_dt as timestamp)) as report_dt
			from 
				public.comp_media
		;
	quit;

	/* Транспонируем таблицу */
	proc cas;
	transpose.transpose /
	   table={name="comp_media_ml", caslib="casuser", groupby={"REPORT_DT"}} 
	   transpose={"TRP"} 
	   prefix="comp_trp_" 
	   id={"COMPETITOR_CD"} 
	   casout={name="comp_transposed_ml", caslib="casuser", replace=true};
	quit;

	/* Протягиваем trp на всю неделю вперед */
	data casuser.comp_transposed_ml_expand;
		set casuser.comp_transposed_ml;
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
		create table casuser.comp_transposed_ml_expand{options replace=true} as
			select
				REPORT_DT,
				mean(comp_trp_BK) as comp_trp_BK,
				mean(comp_trp_KFC) as comp_trp_KFC
			from
				casuser.comp_transposed_ml_expand
			group by report_dt
		;
	quit;

	/* Соединяем с ABT */
	proc fedsql sessref = casauto;
		create table casuser.abt9_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t2.comp_trp_BK,
				t2.comp_trp_KFC
			from
				casuser.abt8_ml as t1
			left join
				casuser.comp_transposed_ml_expand as t2
			on
				t1.sales_dt = t2.REPORT_DT
		;
	quit;

	proc casutil;
		droptable casdata='comp_media' incaslib='casuser' quiet;
		droptable casdata='comp_media_ml' incaslib='casuser' quiet;
		droptable casdata='comp_transposed_ml' incaslib='casuser' quiet;
		droptable casdata='comp_transposed_ml_expand' incaslib='casuser' quiet;
		droptable casdata="abt8_ml" incaslib="casuser" quiet;
	/* 	promote casdata="abt9_ml" incaslib="public" outcaslib="public"; */
	run;


	/***** 10. Добавляем медиаподдержку *****/
	proc casutil;
	  droptable casdata="media_ml" incaslib="public" quiet;
	  droptable casdata="abt10_ml" incaslib="public" quiet;
	run;
	
	data CASUSER.media (replace=yes);
		set &lmvInLib..media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc fedsql sessref=casauto;
	/*	
		create table casuser.promo_x_pbo_leaf{options replace = true} as 
			select
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				casuser.promo_x_pbo as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		Уже создана строка 936
	*/
		create table casuser.promo_x_product_leaf{options replace = true} as 
			select
				t1.promo_id,
				t2.product_LEAF_ID
			from
				casuser.promo_x_product as t1,
				casuser.product_lvl_all as t2
			where t1.product_id = t2.product_id
		;
		create table casuser.promo_ml_trp{options replace = true} as 
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
				casuser.promo as t1 
			left join
				casuser.promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID
			left join
				casuser.media as t4
			on
				t1.PROMO_GROUP_ID = t4.PROMO_GROUP_ID
		;
	quit;

	data casuser.promo_ml_trp_expand;
		set casuser.promo_ml_trp;
		do i = 1 to 7;
			output;
			REPORT_DT + 1;
		end;
	run;

	proc fedsql sessref=casauto;
		create table casuser.sum_trp{options replace=true} as 
			select
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
				t1.REPORT_DT,
				sum(t1.trp) as sum_trp
			from
				casuser.promo_ml_trp_expand as t1
			group by
				t1.PRODUCT_LEAF_ID,
				t1.PBO_LEAF_ID,
				t1.report_dt
		;
		create table casuser.abt10_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t2.sum_trp
			from
				casuser.abt9_ml as t1
			left join
				casuser.sum_trp as t2
			on 
				t1.product_id = t2.PRODUCT_LEAF_ID and
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.sales_dt = t2.report_dt
		;
	quit;

	proc casutil;
	  droptable casdata="media" incaslib="casuser" quiet;
	  droptable casdata="promo" incaslib="casuser" quiet;
	  droptable casdata="promo_x_product" incaslib="casuser" quiet;
	  droptable casdata="promo_x_pbo" incaslib="casuser" quiet;
	  droptable casdata="promo_x_pbo_leaf" incaslib="casuser" quiet;
	  droptable casdata="promo_x_product_leaf" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
	  droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
	  droptable casdata="sum_trp" incaslib="casuser" quiet;
	  droptable casdata="abt9_ml" incaslib="casuser" quiet;
	/*   promote casdata="abt10_ml" incaslib="casuser" outcaslib="public"; */
	run;


	/****** 11. Добавляем атрибуты товаров ******/
	proc casutil;
	  droptable casdata="abt11_ml" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.abt11_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t2.prod_lvl4_id, 
				t2.prod_lvl3_id,
				t2.prod_lvl2_id,
				t2.a_hero_id as hero,
				t2.a_item_size_id as item_size,
				t2.a_offer_type_id as offer_type,
				t2.a_price_tier_id as price_tier
		from
			casuser.abt10_ml as t1
		left join
			casuser.product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
		;
	quit;
	 
	proc casutil;
	/*   promote casdata="abt11_ml" incaslib="casuser" outcaslib="public"; */
		promote casdata="product_dictionary_ml" incaslib="casuser" outcaslib="public";
		droptable casdata="abt10_ml" incaslib="casuser" quiet;
	run;


	/******	12. Добавим атрибуты ПБО ******/
	proc casutil;
	  droptable casdata="abt12_ml" incaslib="public" quiet;
	  load data=&lmvInLib..pbo_location casout='pbo_location' outcaslib='public' replace;
	run;

	data CASUSER.pbo_location (replace=yes);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc cas;
	transpose.transpose /
	   table={name="PBO_LOC_ATTRIBUTES", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto;
	   create table casuser.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.pbo_dictionary_ml{options replace=true} as
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
				casuser.pbo_hier_flat t2
			left join
				casuser.attr_transposed t3
			on
				t2.pbo_location_id=t3.pbo_location_id
			left join
				casuser.pbo_location t14
			on 
				t2.pbo_location_id=t14.pbo_location_id
			left join
				casuser.pbo_location t13
			on 
				t2.lvl3_id=t13.pbo_location_id
			left join
				casuser.pbo_location t12
			on
				t2.lvl2_id=t12.pbo_location_id;
	quit;

	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_AGREEMENT_TYPE);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_BREAKFAST);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_BUILDING_TYPE);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_COMPANY);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_DELIVERY);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_MCCAFE_TYPE);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_PRICE_LEVEL);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_DRIVE_THRU);
	%text_encoding(mpTable=casuser.pbo_dictionary_ml, mpVariable=A_WINDOW_TYPE);

	proc fedsql sessref=casauto;
		create table casuser.abt12_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
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
				casuser.abt11_ml as t1
			left join
				casuser.pbo_dictionary_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	proc casutil;
	  droptable casdata='pbo_location' incaslib='casuser' quiet;
	  droptable casdata='pbo_loc_hierarchy' incaslib='casuser' quiet;
	  droptable casdata='PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	  droptable casdata='pbo_dictionary_ml' incaslib='casuser' quiet;
	/*   promote casdata="abt12_ml" incaslib="casuser" outcaslib="public"; */
	  droptable casdata="abt11_ml" incaslib="casuser" quiet;
	run;



	/****** 13. Добавляем календарные признаки *******/
	proc casutil;
	  droptable casdata="abt13_ml" incaslib="casuser" quiet;
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
	  load data=work.cldr_prep_features2 casout='cldr_prep_features' outcaslib='casuser' replace;
	run;

	/* Добавляем к витрине */
	proc fedsql sessref = casauto;
		create table casuser.abt13_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
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
				t2.weekend_flag
			from
				casuser.abt12_ml as t1
			left join
				casuser.cldr_prep_features as t2
			on
				t1.sales_dt = t2.date
		;
	quit;


	/******  14. Добавим события ******/
	proc casutil;
		droptable casdata="russia_event" incaslib="casuser" quiet;
		droptable casdata="russia_event2" incaslib="casuser" quiet;
		droptable casdata="russia_event_t" incaslib="casuser" quiet;
		droptable casdata="abt14_ml" incaslib="public" quiet;
	/* 	promote casdata="abt13_ml" incaslib="casuser" outcaslib="public"; */
		droptable casdata="abt12_ml" incaslib="casuser" quiet;
	run;

	data work.russia_event;
	input date :yymmdd10. event_nm $32.;
	format date yymmddd10.;
	datalines;
	2017-01-01 new_year
	2017-01-02 new_year
	2017-01-03 new_year
	2017-01-04 new_year
	2017-01-05 new_year
	2017-01-06 new_year
	2017-01-07 new_year
	2017-01-08 new_year
	2017-01-25 student_day
	2017-02-14 valentine_day
	2017-02-23 defender_day
	2017-02-24 defender_day
	2017-03-08 female_day
	2017-04-29 may_holiday
	2017-04-30 may_holiday
	2017-05-01 may_holiday
	2017-05-02 may_holiday
	2017-05-03 may_holiday
	2017-05-04 may_holiday
	2017-05-05 may_holiday
	2017-05-06 may_holiday
	2017-05-07 may_holiday
	2017-05-08 may_holiday
	2017-05-09 may_holiday
	2017-06-01 summer_start
	2017-06-12 russia_day
	2017-09-01 school_start
	2017-12-31 new_year
	2018-01-01 new_year
	2018-01-02 new_year
	2018-01-03 new_year
	2018-01-04 new_year
	2018-01-05 new_year
	2018-01-08 new_year
	2018-01-25 student_day
	2018-02-14 valentine_day
	2018-02-23 defender_day
	2018-03-08 female_day
	2018-03-09 female_day
	2018-04-30 may_holiday
	2018-05-01 may_holiday
	2018-05-02 may_holiday
	2018-05-03 may_holiday
	2018-05-04 may_holiday
	2018-05-05 may_holiday
	2018-05-06 may_holiday
	2018-05-07 may_holiday
	2018-05-08 may_holiday
	2018-05-09 may_holiday
	2018-06-01 summer_start
	2018-06-11 russia_day
	2018-06-12 russia_day
	2018-09-01 school_start
	2018-12-31 new_year
	2019-01-01 new_year
	2019-01-02 new_year
	2019-01-03 new_year
	2019-01-04 new_year
	2019-01-07 new_year
	2019-01-08 new_year
	2019-01-25 student_day
	2019-02-14 valentine_day
	2019-02-23 defender_day
	2019-03-08 female_day
	2019-04-29 may_holiday
	2019-04-30 may_holiday
	2019-05-01 may_holiday
	2019-05-02 may_holiday
	2019-05-03 may_holiday
	2019-05-04 may_holiday
	2019-05-05 may_holiday
	2019-05-06 may_holiday
	2019-05-07 may_holiday
	2019-05-08 may_holiday
	2019-05-09 may_holiday
	2019-05-10 may_holiday
	2019-06-01 summer_start
	2019-06-12 russia_day
	2019-09-01 school_start
	2019-12-31 new_year
	2020-01-01 new_year
	2020-01-02 new_year
	2020-01-03 new_year
	2020-01-06 new_year
	2020-01-07 new_year
	2020-01-08 new_year
	2020-01-25 student_day
	2020-02-14 valentine_day
	2020-02-23 defender_day
	2020-02-24 defender_day
	2020-03-08 female_day
	2020-03-09 female_day
	2020-05-01 may_holiday
	2020-05-02 may_holiday
	2020-05-03 may_holiday
	2020-05-04 may_holiday
	2020-05-05 may_holiday
	2020-05-06 may_holiday
	2020-05-07 may_holiday
	2020-05-08 may_holiday
	2020-05-09 may_holiday 
	2020-05-10 may_holiday
	2020-05-11 may_holiday
	2020-06-01 summer_start
	2020-06-12 russia_day
	2020-09-01 school_start
	2020-12-31 new_year
	2021-01-01 new_year
	2021-01-02 new_year
	2021-01-03 new_year
	2021-01-04 new_year
	2021-01-05 new_year
	2021-01-06 new_year
	2021-01-07 new_year
	2021-01-08 new_year
	2021-01-25 student_day
	2021-02-14 valentine_day
	2021-02-23 defender_day
	2021-03-08 female_day
	2021-05-01 may_holiday
	2021-05-02 may_holiday
	2021-05-03 may_holiday
	2021-05-04 may_holiday
	2021-05-05 may_holiday
	2021-05-06 may_holiday
	2021-05-07 may_holiday
	2021-05-08 may_holiday
	2021-05-09 may_holiday
	2021-05-10 may_holiday
	2021-06-01 summer_start
	2021-06-14 russia_day
	2021-09-01 school_start
	;
	run;

	/* загружаем таблицу в cas */
	proc casutil;
	  load data=work.russia_event casout='russia_event' outcaslib='casuser' replace;
	run;

	/* добваляем константный флаг */
	proc fedsql sessref = casauto;
		create table casuser.russia_event2{options replace=true} as
			select *, 1 as event_flag from casuser.russia_event;
	quit;

	/* транспонируем таблицу */
	proc cas;
	transpose.transpose /
	   table={name="russia_event2", caslib="casuser", groupby={"date"}} 
	   attributes={{name="date"}} 
	   transpose={"event_flag"} 
	   id={"event_nm"} 
	   casout={name="russia_event_t", caslib="casuser", replace=true};
	quit;

	/* добавляем к ваитрине */
	proc fedsql sessref=casauto;
		create table casuser.abt14_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
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
				t1.weekend_flag,
				coalesce(t2.defender_day, 0) as defender_day,
				coalesce(t2.female_day, 0) as female_day,
				coalesce(t2.may_holiday, 0) as may_holiday,
				coalesce(t2.new_year , 0) as new_year,
				coalesce(t2.russia_day, 0) as russia_day,
				coalesce(t2.school_start, 0) as school_start,
				coalesce(t2.student_day, 0) as student_day,
				coalesce(t2.summer_start, 0) as summer_start,
				coalesce(t2.valentine_day, 0) as valentine_day
			from
				casuser.abt13_ml as t1
			left join
				casuser.russia_event_t as t2
			on
				t1.sales_dt = t2.date
		;	
	quit;

	proc casutil;
		droptable casdata="russia_event" incaslib="casuser" quiet;
		droptable casdata="russia_event2" incaslib="casuser" quiet;
		droptable casdata="russia_event_t" incaslib="casuser" quiet;
		
		droptable casdata="abt13_ml" incaslib="casuser" quiet;
	run;


	/******	15. Добавим ценовые ранги ******/
	proc casutil;
		droptable casdata="abt15_ml" incaslib="casuser" quiet;
		droptable casdata="unique_day_price" incaslib="casuser" quiet;
		droptable casdata="sum_count_price" incaslib="casuser" quiet;
		droptable casdata="price_rank" incaslib="casuser" quiet;
		droptable casdata="price_rank2" incaslib="casuser" quiet;
		droptable casdata="price_rank3" incaslib="casuser" quiet;
		droptable casdata="price_feature" incaslib="casuser" quiet;
		
	run;

	/* уникальные ПБО/день/категория товаров/товар/цена */
	proc fedsql sessref = casauto;
		create table casuser.unique_day_price as 
			select distinct
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.product_id,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt14_ml as t1
		;
	quit;

	/* Считаем суммарную цену в групе и количество товаров */
	proc fedsql sessref = casauto;
		create table casuser.sum_count_price{options replace = true} as
			select
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				count(t1.product_id) as count_product,
				sum(t1.GROSS_PRICE_AMT) as sum_gross_price_amt
			from casuser.unique_day_price as t1
			group by
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt
		;
	quit;

	/* считаем позицию товара в отсортированном списке цен */
	data casuser.price_rank;
		set casuser.unique_day_price;
		by pbo_location_id sales_dt PROD_LVL3_ID GROSS_PRICE_AMT ;
		if first.PROD_LVL3_ID then i = 0;
		if GROSS_PRICE_AMT ^= lag(GROSS_PRICE_AMT) then i+1;
	run;

	proc fedsql sessref = casauto;
		create table casuser.price_rank2{options replace=true} as
			select
				t1.pbo_location_id,
				t1.sales_dt,
				t1.PROD_LVL3_ID,
				max(t1.i) as max_i
			from
				casuser.price_rank as t1
			group by
				t1.pbo_location_id,
				t1.sales_dt,
				t1.PROD_LVL3_ID
		; 
	quit;

	/* Соединяем таблицы price_rank, price_rank2 */
	proc fedsql sessref=casauto;
		create table casuser.price_rank3{options replace=true} as
			select
				t1.product_id,
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.GROSS_PRICE_AMT,
				t1.i,
				t2.max_i
			from
				casuser.price_rank as t1
			left join
				casuser.price_rank2 as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
				t1.sales_dt = t2.sales_dt
		;
	quit;

	/* Соединяем таблицы price_rank3 и sum_count_price */
	proc fedsql sessref=casauto;
		create table casuser.price_feature{options replace=true} as
			select
				t1.product_id,
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.GROSS_PRICE_AMT,
				t1.i,
				t1.max_i,
				t2.count_product,
				t2.sum_gross_price_amt,
				divide(t1.i,t1.max_i) as price_rank,
				(
					case
						when t2.sum_gross_price_amt = t1.GROSS_PRICE_AMT then 1
						else divide(t1.GROSS_PRICE_AMT,divide((t2.sum_gross_price_amt - t1.GROSS_PRICE_AMT),(t2.count_product - 1)))
					end
				) as price_index
			from
				casuser.price_rank3 as t1
			left join
				casuser.sum_count_price as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
				t1.sales_dt = t2.sales_dt
		;
	quit;

	/* Добавляем в витрину */
	proc fedsql sessref = casauto;
		create table casuser.abt15_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
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
				t1.weekend_flag,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t2.price_rank,
				t2.price_index
			from
				casuser.abt14_ml as t1
			left join
				casuser.price_feature as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id and
				t1.sales_dt = t2.sales_dt
		;
	quit;

	proc casutil;
		droptable casdata="unique_day_price" incaslib="casuser" quiet;
		droptable casdata="sum_count_price" incaslib="casuser" quiet;
		droptable casdata="price_rank" incaslib="casuser" quiet;
		droptable casdata="price_rank2" incaslib="casuser" quiet;
		droptable casdata="price_rank3" incaslib="casuser" quiet;
		droptable casdata="price_feature" incaslib="casuser" quiet;
		
		promote casdata="abt14_ml" incaslib="casuser" outcaslib="public";
		droptable casdata="abt14_ml" incaslib="casuser" quiet;
	run;

	/******	16. Перекодируем channel_cd  ******/
	proc casutil;
		droptable casdata="abt16_ml" incaslib="public" quiet;
	run;

	%text_encoding(mpTable=casuser.abt15_ml, mpVariable=channel_cd);

	/* Заменяем текстовое поле на числовое */
	proc fedsql sessref = casauto;
		create table casuser.abt16_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD_id as channel_cd,
				t1.SALES_DT,
				t1.sum_qty,
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
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
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
				t1.weekend_flag,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t1.price_rank,
				t1.price_index
			from
				casuser.abt15_ml as t1
		;
	quit;

	/******	17. Разделяем витрину на обучение и скоринг  ******/
	proc casutil;
		droptable casdata="russca_all_ml_train" incaslib="public" quiet;
		droptable casdata="russca_all_ml_scoring" incaslib="public" quiet;
		promote casdata="abt15_ml" incaslib="casuser" outcaslib="public";
		droptable casdata="abt15_ml" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.russca_all_ml_train{options replace = true} as 
			select * from casuser.abt16_ml where sales_dt <= date'2019-12-31';
		create table casuser.russca_all_ml_scoring{options replace = true} as 
			select * from casuser.abt16_ml 
			where
				sales_dt > date'2019-12-31' and
				sales_dt <= date'2020-03-01';
	quit;

	proc casutil;
		promote casdata="abt16_ml" incaslib="casuser" outcaslib="public";
		promote casdata="russca_all_ml_train" incaslib="casuser" outcaslib="public";
		promote casdata="russca_all_ml_scoring" incaslib="casuser" outcaslib="public";
	run;

%mend rtp_load_data;