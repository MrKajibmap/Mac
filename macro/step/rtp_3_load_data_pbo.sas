/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (PBO)
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
*	%macro rtp_3_load_data_pbo(mpMode=S,
*							mpOutTableTrain=dm_abt.pbo_train,
							mpOutTableScore=dm_abt.pbo_score);
*							);
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
****************************************************************************/

%macro rtp_3_load_data_pbo(mpMode=A, 
							mpOutTableTrain=casuser.pbo_train,
							mpOutTableScore=casuser.pbo_score);
	/****** 0. Объявление макропеременных ******/
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;

	%local lmvInLib 
			lmvMode 
			lmvReportDttm
			lmvLibrefOutTrain
			lmvTabNmOutTrain
			lmvLibrefOutScore
			lmvTabNmOutScore
			lmvFcEnd
			;
	%let lmvMode = &mpMode.;
	%if &lmvMode. = S %then %do;
		%let lmvStartDate =%eval(%sysfunc(intnx(year,&etl_current_dt.,-1,s))-91);
		%let lmvEndDate = &etl_current_dt.;
	%end;
	%else %if &lmvMode = T or &lmvMode. = A %then %do;
		/*%let lmvStartDate = %eval(%sysfunc(intnx(year,&etl_current_dt.,-3,s))-91);*/
		%let lmvStartDate = %eval(%sysfunc(intnx(year,&etl_current_dt.,-3,s))-91);
		%let lmvEndDate = &etl_current_dt.;
	%end;
	%let lmvInLib=ETL_IA;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvStartDateScore =%sysfunc(intnx(year,&etl_current_dt.,-1,s));
	%let lmvFcEnd=%sysfunc(putn(&lmvEndDate,yymmdd10.));
	
	%member_names(mpTable=&mpOutTableTrain, mpLibrefNameKey=lmvLibrefOutTrain, mpMemberNameKey=lmvTabNmOutTrain);
	%member_names(mpTable=&mpOutTableScore, mpLibrefNameKey=lmvLibrefOutScore, mpMemberNameKey=lmvTabNmOutScore);
	
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND ( casauto)) = 0 %then %do;
	 cas casauto;
	 caslib _all_ assign;
	%end;

	/* Подготовка таблицы с продажами */
	data CASUSER.pmix_sales (replace=yes);
			set &lmvInLib..pmix_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			and sales_dt<=&lmvEndDate. and sales_dt>=&lmvStartDate.));
	run;
		
	proc casutil;
		%if &lmvMode. = A or &lmvMode = T %then %do;
			droptable casdata="&lmvTabNmOutTrain." incaslib="&lmvLibrefOutTrain." quiet;
		%end;
		%if &lmvMode. = A or &lmvMode = S %then %do;
			droptable casdata="&lmvTabNmOutScore." incaslib="&lmvLibrefOutScore." quiet;
		%end;
	run;
			
	proc fedsql sessref=casauto; 
		create table casuser.pbo_abt1_ml{options replace=true} as
		select 
			t1.PBO_LOCATION_ID,
			t2.lvl2_id,
			t2.lvl3_id,
			t1.PRODUCT_ID,
			t1.CHANNEL_CD,
			t1.SALES_DT,
			(t1.SALES_QTY + t1.SALES_QTY_PROMO) as sum_qty
		from CASUSER.pmix_sales t1
		left join 
			casuser.pbo_dictionary as t2 /* from 1 */
		on
			t1.pbo_location_id = t2.pbo_location_id
	;
	quit;

	/****** 2. Добавление цен ******/
	data CASUSER.price_ml (replace=yes);
			set &lmvInLib..price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			and end_dt<=&lmvEndDate. and start_dt>=&lmvStartDate.));
	run;
	/* Добавляем к продажам цены */
	proc fedsql sessref=casauto; 
		create table casuser.pbo_abt2_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				max(t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT
			from
				casuser.pbo_abt1_ml as t1
				left join casuser.price_ml as t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= t2.end_dt and   
				t1.SALES_DT >= t2.start_dt
			group by 
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty
			;
	quit;
	/****** 3. Протягиваем ВР ******/
	proc casutil;
	  droptable casdata="pbo_abt3_ml" incaslib="casuser" quiet;
	run;

	proc cas;
	timeData.timeSeries result =r /
		series={
			{name="sum_qty", setmiss="MISSING"},
			{name="GROSS_PRICE_AMT", setmiss="PREV"}
		}
		tEnd= "&lmvFcEnd"
		table={
			caslib="casuser",
			name="pbo_abt2_ml",
			groupby={"PBO_LOCATION_ID", "PRODUCT_ID", "CHANNEL_CD", "lvl2_id", "lvl3_id"}
		}
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="casuser", name="pbo_abt3_ml", replace=True}
		;
	run;
	quit;

	/****** 4. Фильтрация ******/

	/* 4.1 Убираем временные закрытия ПБО */
	proc casutil;
		droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
		droptable casdata="pbo_abt4_ml" incaslib="casuser" quiet;
	run;

	data CASUSER.pbo_close_period (replace=yes);
			set &lmvInLib..pbo_close_period(where=(/*valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
				and */end_dt<=&lmvEndDate. and start_dt>=&lmvStartDate.));
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.pbo_closed_ml {options replace=true} as
			select 
				CHANNEL_CD,
				PBO_LOCATION_ID,
				start_dt,
				coalesce(end_dt, date '2100-01-01') as end_dt,
				CLOSE_PERIOD_DESC
			from
				casuser.pbo_close_period
		;
	quit;
	
	/* Удалаем даты закрытия pbo из abt */
	proc fedsql sessref=casauto;
		create table casuser.pbo_abt4_ml{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.pbo_abt3_ml as t1
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
	
	/* Удаляем закрытые насовсем магазины  */
	proc fedsql sessref=casauto;
		create table casuser.pbo_abt4_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.pbo_abt4_ml as t1
			left join
				casuser.closed_pbo as t2 /*из 1_1 */
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt >= t2.OPEN_DATE and
				t1.sales_dt <= t2.CLOSE_DATE
			where
				t2.pbo_location_id is not missing
		;
	quit;

	/* 4.3 Убираем из истории пропуски в продажах */
	proc fedsql sessref=casauto;
		create table casuser.pbo_abt4_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.pbo_abt4_ml as t1
			where 
				t1.sum_qty is not missing 
				/*
				(t1.sum_qty is not missing and t1.sales_dt <= &lmvEndDate.) or
				(t1.sales_dt > &lmvEndDate.) */
		;
	quit;

	/* 4.4 Пересекаем с ассортиментной матрицей скоринговую витрину */
	proc fedsql sessref=casauto;
		create table casuser.pbo_abt4_ml {options replace = true} as	
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT				
			from
				casuser.pbo_abt4_ml as t1
			left join
				casuser.assort_matrix  t2 /* из 1_1 */
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= datepart(t2.end_dt) and 
				t1.SALES_DT >= datepart(t2.start_dt)
			where	
				/*(t1.SALES_DT <= &lmvEndDate) or */
				t2.PBO_LOCATION_ID is not missing
		;
	quit;

	/****** 5. Агрегация ******/
	proc casutil;
		droptable casdata="pbo_abt5_ml" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.pbo_abt5_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				sum(t1.sum_qty) as sum_qty,
				mean(t1.GROSS_PRICE_AMT) as mean_price
			from
				casuser.pbo_abt4_ml as t1
			group by
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT
		;
	quit;

	/****** 6. Добавление независымых переменных ******/
	/* 1.Добавляем мароэкономику */
	/* Соединяем с ABT */
	proc fedsql sessref = casauto;
		create table casuser.pbo_abt6_1_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.mean_price,
				t2.A_CPI,
				t2.A_GPD,
				t2.A_RDI
			from
				casuser.pbo_abt5_ml as t1 left join 
				casuser.macro_transposed_ml as t2 /* из 1_1 */
			on
				t1.sales_dt = t2.period_dt
		;
	quit;
	
	/* 5.2 Добавляем погоду */
	proc casutil;
	  droptable casdata = "pbo_abt6_2_ml" incaslib = "casuser" quiet;
	run;

	proc fedsql sessref =casauto;
		create table casuser.pbo_abt6_2_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.mean_price,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t2.TEMPERATURE,
				t2.PRECIPITATION
			from
				casuser.pbo_abt6_1_ml as t1
			left join
				casuser.weather as t2 /* 1_1 */
			on 
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = datepart(t2.REPORT_DT)
		;
	quit;

	/* 5.3 Добавляем количество товаров в промо */
	proc casutil;
		droptable casdata="pbo_hier_flat" incaslib="casuser" quiet;
		droptable casdata="product_hier_flat" incaslib="casuser" quiet;
		droptable casdata="lvl5" incaslib="casuser" quiet;
		droptable casdata="lvl4" incaslib="casuser" quiet;
		droptable casdata="lvl3" incaslib="casuser" quiet;
		droptable casdata="lvl2" incaslib="casuser" quiet;
		droptable casdata="lvl1" incaslib="casuser" quiet;
		droptable casdata="num_of_promo_prod" incaslib="casuser" quiet;
		droptable casdata="pbo_lvl_all" incaslib="casuser" quiet;
		droptable casdata="product_lvl_all" incaslib="casuser" quiet;
		droptable casdata="promo_ml" incaslib="casuser" quiet;
		droptable casdata="promo_ml2" incaslib="casuser" quiet;
		droptable casdata="pbo_abt6_3_ml" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref = casauto;
	create table casuser.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				t3.product_LEAF_ID,
				t2.PBO_LEAF_ID,
				t1.PROMO_NM,
				t1.PROMO_PRICE_AMT,
				t1.CHANNEL_CD,
				t1.NP_GIFT_PRICE_AMT,
				t1.PROMO_MECHANICS,
				start_dt,
				 end_dt
			from
				casuser.promo as t1  /*1_1 */
			left join
				casuser.promo_x_pbo_leaf as t2 /*1_1 */
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				casuser.promo_x_product_leaf as t3 /*1_1 */
			on
				t1.PROMO_ID = t3.PROMO_ID 
		;
	quit;
	/* Протягиваем интервалы промо */
	data casuser.promo_ml2;
		set casuser.promo_ml;
		format period_dt date9.;
		do period_dt=start_dt to end_dt;
			output;
		end;
	run;
		
	/* Считаем количество товаров в промо */
	proc fedsql sessref=casauto;
		create table casuser.num_of_promo_prod{options replace=true} as 
			select
				t1.CHANNEL_CD,
				t1.PBO_LEAF_ID,
				t1.period_dt,
				count(distinct t1.product_LEAF_ID) as count_promo_product,
				count(distinct t1.PROMO_ID) as nunique_promo
			from
				casuser.promo_ml2 as t1
			group by
				t1.CHANNEL_CD,
				t1.PBO_LEAF_ID,
				t1.period_dt
		;
	quit;	
		
	proc fedsql sessref=casauto;
		create table casuser.pbo_abt6_3_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.mean_price,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				coalesce(t2.count_promo_product, 0) as count_promo_product,
				coalesce(t2.nunique_promo, 0) as nunique_promo
			from
				casuser.pbo_abt6_2_ml as t1
			left join
				casuser.num_of_promo_prod as t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LEAF_ID and
				t1.CHANNEL_CD = t2.CHANNEL_CD  and
				t1.sales_dt = t2.period_dt
		;
	quit;

	proc casutil;
		droptable casdata="num_of_promo_prod" incaslib="casuser" quiet;
		droptable casdata="promo_ml2" incaslib="casuser" quiet;
		droptable casdata="promo_x_product_leaf" incaslib="casuser" quiet;
		droptable casdata="promo_x_pbo_leaf" incaslib="casuser" quiet;
	run;

	/* 5.4 TRP конкурентов */
	proc casutil;
		droptable casdata="comp_media_ml" incaslib="casuser" quiet;
		droptable casdata="pbo_abt6_4_ml" incaslib="casuser" quiet;
	run;

	/*
		Пока в данных есть ошибка, все интевалы report_dt указаны
		с интервалом в неделю, но есть одно наблюдение
		в котором этот порядок рушится 16dec2019 и 22dec2019 (6 Дней)
		Поэтому, пока в таблице есть дубль, который мы убираем путем усреднения
	*/

	/* Соединяем с ABT */
	proc fedsql sessref = casauto;
		create table casuser.pbo_abt6_4_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.mean_price,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.count_promo_product,	
				t1.nunique_promo,		
				t2.comp_trp_BK,
				t2.comp_trp_KFC
			from
				casuser.pbo_abt6_3_ml as t1
			left join
				casuser.comp_transposed_ml_expand as t2
			on
				t1.sales_dt = t2.REPORT_DT
		;
	quit;

	/* 5.5 TRP мака */
	proc fedsql sessref=casauto;
		create table casuser.sum_trp{options replace=true} as 
			select
				t1.PBO_LEAF_ID,
				t1.REPORT_DT,
				count(t1.trp) as count_trp,
				sum(t1.trp) as sum_trp
			from
				casuser.promo_ml_trp_expand as t1
			group by
				t1.PBO_LEAF_ID,
				t1.report_dt
		;
		create table casuser.pbo_abt6_5_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.lvl2_id,
				t1.lvl3_id,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.mean_price,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.count_promo_product,	
				t1.nunique_promo,		
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t2.sum_trp,
				t2.count_trp
			from
				casuser.pbo_abt6_4_ml as t1
			left join
				casuser.sum_trp as t2
			on 
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.sales_dt = t2.report_dt
		;
	quit; 
	
	proc fedsql sessref=casauto;
	 %if &lmvMode. = A or &lmvMode = T %then %do;
		create table casuser.&lmvTabNmOutTrain.{options replace = true} as 
			select
				*
			from
				casuser.pbo_abt6_5_ml
		;
	 %end;
	 %if &lmvMode. = A or &lmvMode = S %then %do;
		create table casuser.&lmvTabNmOutScore.{options replace = true} as 
			select 
				* 
			from 
				casuser.pbo_abt6_5_ml 
			where /* Забираем лишь только 1 год + 91 день ? */
				sales_dt > date %str(%')%sysfunc(putn(&lmvStartDateScore., yymmdd10.))%str(%') /*and
				sales_dt <= date'2020-03-01' */
		;	
	 %end;
	quit;

	proc casutil;
	%if &lmvMode. = A or &lmvMode = T %then %do;
		promote casdata="&lmvTabNmOutTrain." incaslib="casuser" outcaslib="&lmvLibrefOutTrain.";
	%end;
	%if &lmvMode. = A or &lmvMode = S %then %do;
		promote casdata="&lmvTabNmOutScore." incaslib="casuser" outcaslib="&lmvLibrefOutScore.";
	%end;
		droptable casdata="media" incaslib="casuser" quiet;
		/*droptable casdata="promo" incaslib="casuser" quiet; */
		droptable casdata="promo_x_product" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
		droptable casdata="sum_trp" incaslib="casuser" quiet;
		droptable casdata="pbo_abt6_5_ml" incaslib="casuser" quiet;
	run;

%mend rtp_3_load_data_pbo;
