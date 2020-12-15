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
*	%macro rtp_3_load_data_pbo_sep(mpMode=S,
*							mpOutTableTrain=dm_abt.pbo_train,
							mpOutTableScore=dm_abt.pbo_score);
*							);
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
****************************************************************************/

%macro rtp_3_load_data_pbo_sep(mpMode=A, 
							mpOutTableTrain=mn_short.pbo_train,
							mpOutTableScore=mn_short.pbo_score);
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
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	
	/*%if &lmvMode. = S %then %do;
		%let lmvStartDate =%eval(%sysfunc(intnx(year,&etl_current_dt.,-1,s))-91);
		%let lmvEndDate = &VF_HIST_END_DT_SAS.;
		%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	%end;*/
	/*%else %if &lmvMode = T or &lmvMode. = A %then %do;*/
		%let lmvStartDate = %eval(%sysfunc(intnx(year,&etl_current_dt.,-3,s))-91);
		%let lmvEndDate = &VF_HIST_END_DT_SAS.;
		%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	/*%end;*/
	
	%let lmvInLib=ETL_IA;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	/* -(год+91 день) */
	/*%let lmvStartDateScore =%sysfunc(intnx(day,%sysfunc(intnx(year,&etl_current_dt.,-1,s)),-91));*/
	%let lmvStartDateScore =%sysfunc(intnx(year,&etl_current_dt.,-1,s));
	%let lmvFcEnd=%sysfunc(putn(&lmvScoreEndDate,yymmdd10.));
	
	%member_names(mpTable=&mpOutTableTrain, mpLibrefNameKey=lmvLibrefOutTrain, mpMemberNameKey=lmvTabNmOutTrain);
	%member_names(mpTable=&mpOutTableScore, mpLibrefNameKey=lmvLibrefOutScore, mpMemberNameKey=lmvTabNmOutScore);
	
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND ( casauto)) = 0 %then %do;
	 cas casauto sessopts=(metrics=true);
	 caslib _all_ assign;
	%end;
	
	/* Подтягиваем данные из PROMOTOOL */
	%add_promotool_marks(mpIntLibref=casuser,
							mpExtLibref=pt);

	data CASUSER.product (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_HIERARCHY (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.product_ATTRIBUTES (replace=yes  drop=valid_from_dttm valid_to_dttm);
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

proc casutil;
  droptable casdata='product' incaslib='casuser' quiet;
  droptable casdata='product_hier_flat' incaslib='casuser' quiet;
  droptable casdata='attr_transposed' incaslib='casuser' quiet;
run;

/* Cобираем справочник ПБО для того, чтобы создать фильтр */
data CASUSER.pbo_location (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	data CASUSER.pbo_loc_hierarchy (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..pbo_loc_hierarchy(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

proc cas;
transpose.transpose /
   table={name="pbo_loc_attributes", caslib="casuser", groupby={"pbo_location_id"}} 
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
			casuser.PBO_LOCATION t14
		on 
			t2.pbo_location_id=t14.pbo_location_id
		left join
			casuser.PBO_LOCATION t13
		on 
			t2.lvl3_id=t13.pbo_location_id
		left join
			casuser.PBO_LOCATION t12
		on
			t2.lvl2_id=t12.pbo_location_id;
quit;



	/* Подготовка таблицы с продажами */
	data CASUSER.pmix_sales (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..pmix_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			and sales_dt<=&lmvScoreEndDate. and sales_dt>=&lmvStartDate.));
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
			casuser.pbo_dictionary_ml as t2 /* from 1 */
		on
			t1.pbo_location_id = t2.pbo_location_id
	;
	quit;

	/****** 2. Добавление цен ******/
	data CASUSER.price_ml (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			and end_dt<=&lmvScoreEndDate. and start_dt>=&lmvStartDate.));
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
	  droptable casdata="pbo_abt1_ml" incaslib="casuser" quiet;
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
	    droptable casdata="pbo_abt2_ml" incaslib="casuser" quiet;
		droptable casdata="pbo_closed_ml" incaslib="casuser" quiet;
		droptable casdata="pbo_abt4_ml" incaslib="casuser" quiet;
	run;
		
	data CASUSER.pbo_close_period (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..pbo_close_period(where=((valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.) 
				and (end_dt<=&lmvScoreEndDate. and start_dt>=&lmvStartDate.)));
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
	/*Создание closed_pbo */
	proc casutil;
		droptable casdata="closed_pbo" incaslib="casuser" quiet;
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc cas;
	transpose.transpose /
	   table={name="pbo_loc_attributes", caslib="casuser", groupby={"pbo_location_id"}} 
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
				casuser.closed_pbo as t2 
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
				(t1.sum_qty is not missing and t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%')) or
			(t1.SALES_DT > %str(date%')%sysfunc(putn(&lmvEndDate.,yymmdd10.))%str(%'))
				
		;
	quit;

	/* Create assort_matrix */
	/* 4.4 Пересекаем с ассортиментной матрицей скоринговую витрину */
	proc casutil;
		droptable casdata="assort_matrix" incaslib="casuser" quiet;
	run;
	
	data CASUSER.assort_matrix (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..assort_matrix(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
		/*and end_dt<=&lmvEndDate. and start_dt>=&lmvStartDate.*/));
	run;
	
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
				casuser.assort_matrix  t2 
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= datepart(t2.end_dt) and 
				t1.SALES_DT >= datepart(t2.start_dt)
			where	
				(t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%')) or 
				t2.PBO_LOCATION_ID is not missing
		;
	quit;

	/****** 5. Агрегация ******/
	proc casutil;
	    droptable casdata="pbo_abt3_ml" incaslib="casuser" quiet;
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
	
	/******  Добавляем macro_transposed_ml ******/
	proc casutil;
	  droptable casdata="macro_ml" incaslib="casuser" quiet;
	  droptable casdata="macro2_ml" incaslib="casuser" quiet;
	  droptable casdata="macro_transposed_ml" incaslib="casuser" quiet;
	  run;

		data CASUSER.macro (replace=yes  drop=valid_from_dttm valid_to_dttm);
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
				casuser.macro_transposed_ml as t2 
			on
				t1.sales_dt = t2.period_dt
		;
	quit;
	
	/* 5.2 Добавляем погоду */
	proc casutil;
	  droptable casdata="pbo_abt5_ml" incaslib="casuser" quiet;
	  droptable casdata="pbo_abt4_ml" incaslib="casuser" quiet;
	  droptable casdata = "pbo_abt6_2_ml" incaslib = "casuser" quiet;	  
	  droptable casdata = "weather" incaslib = "casuser" quiet;
	run;

	data CASUSER.weather (replace=yes  drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..weather(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
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
				casuser.weather as t2 
			on 
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = datepart(t2.REPORT_DT)
			/* t1.sales_dt = datepart(t2.PERIOD_DT) */
		;
	quit;

	/* 5.3 Добавляем количество товаров в промо */
	proc casutil;
		droptable casdata="pbo_abt6_1_ml" incaslib="casuser" quiet;
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

	data CASUSER.promo (replace=yes);
		/* set &lmvInLib..promo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set CASUSER.promo_enh;
	run;
	
	data CASUSER.promo_x_pbo (replace=yes);
		/* set &lmvInLib..promo_x_pbo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set CASUSER.promo_pbo_enh;
	run;
	
	data CASUSER.promo_x_product (replace=yes);
		/* set &lmvInLib..promo_x_product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set casuser.promo_prod_enh;
	run;

	/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
	   create table casuser.product_hier_flat{options replace=true} as
			select 
				t1.product_id, 
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
		set casuser.lvl5 casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
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
	set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
run;

	data CASUSER.media (replace=yes);
		/* set &lmvInLib..media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set CASUSER.media_enh;
	run;

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
		create table casuser.promo_ml_trp{options replace = true} as 
		select
			t1.PROMO_ID,
			t3.product_LEAF_ID,
			t2.PBO_LEAF_ID,
			t1.PROMO_NM,
			t1.START_DT,
			t1.END_DT,
			t4.REPORT_DT,
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

	/* add comp_transposed_ml_expand */
	%if %sysfunc(exist(casuser.comp_transposed_ml_expand)) eq 0 %then %do;
		proc casutil;
			droptable casdata="comp_transposed_ml_expand" incaslib="casuser" quiet;
			droptable casdata="comp_media_ml" incaslib="casuser" quiet;
		run;

		data CASUSER.comp_media (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..comp_media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;

		proc fedsql sessref=casauto;
			create table casuser.comp_media_ml{options replace=true} as 
				select
					COMPETITOR_CD,
					TRP,
					datepart(cast(report_dt as timestamp)) as report_dt
				from 
					casuser.comp_media
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
	%end;
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
			where sales_dt <= date %str(%')%sysfunc(putn(&lmvEndDate., yymmdd10.))%str(%')
		;
	 %end;
	 %if &lmvMode. = A or &lmvMode = S %then %do;
		create table casuser.&lmvTabNmOutScore.{options replace = true} as 
			select 
				* 
			from 
				casuser.pbo_abt6_5_ml 
			where /* Забираем лишь только 1 год + 91 день ? */
				sales_dt > date %str(%')%sysfunc(putn(&lmvStartDateScore., yymmdd10.))%str(%') and
				sales_dt <= date %str(%')%sysfunc(putn(&lmvScoreEndDate., yymmdd10.))%str(%')
		;	
	 %end;
	quit;
		
	proc casutil;
	%if &lmvMode. = A or &lmvMode = T %then %do;
		promote casdata="&lmvTabNmOutTrain." incaslib="casuser" outcaslib="&lmvLibrefOutTrain.";
		save incaslib="&lmvLibrefOutTrain." outcaslib="&lmvLibrefOutTrain." casdata="&lmvTabNmOutTrain." casout="&lmvTabNmOutTrain..sashdat" replace;
	%end;
	%if &lmvMode. = A or &lmvMode = S %then %do;
		promote casdata="&lmvTabNmOutScore." incaslib="casuser" outcaslib="&lmvLibrefOutScore.";
		save incaslib="&lmvLibrefOutScore." outcaslib="&lmvLibrefOutScore." casdata="&lmvTabNmOutScore." casout="&lmvTabNmOutScore..sashdat" replace;
	%end;
		droptable casdata="media" incaslib="casuser" quiet;
		/*droptable casdata="promo" incaslib="casuser" quiet; */
		droptable casdata="promo_x_product" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp" incaslib="casuser" quiet;
		droptable casdata="promo_ml_trp_expand" incaslib="casuser" quiet;
		droptable casdata="sum_trp" incaslib="casuser" quiet;
		droptable casdata="pbo_abt6_5_ml" incaslib="casuser" quiet;
		droptable casdata="pbo_abt6_3_ml" incaslib="casuser" quiet;
		droptable casdata="pbo_abt6_4_ml" incaslib="casuser" quiet;
	run;

%mend rtp_3_load_data_pbo_sep;