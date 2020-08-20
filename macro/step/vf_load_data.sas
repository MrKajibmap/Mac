/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных, используемых в сквозном процессе
*		для прогнозирования временными рядами
*
*  ПАРАМЕТРЫ:
*     Нет
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
*     %vf_load_data(mpEvents=dm_abt.events,mpEventsMkup=dm_abt.events_mkup);
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
*  28-07-2020  Борзунов		Промежуточные таблицы вынесены в casuser, events|events_mkup в dm_abt(caslib).
*							Добавлены параметры mpEvents, mpEventsMkup
****************************************************************************/
%macro vf_load_data(mpEvents=dm_abt.events,mpEventsMkup=dm_abt.events_mkup);
	%local lmvInLib lmvOutLibrefEvents lmvOutTabNameEvents lmvOutLibrefEventsMkup lmvOutTabNameEventsMkup;
	%let lmvInLib=ETL_STG2;
	
	%member_names (mpTable=&mpEvents, mpLibrefNameKey=lmvOutLibrefEvents, mpMemberNameKey=lmvOutTabNameEvents);
	%member_names (mpTable=&mpEventsMkup, mpLibrefNameKey=lmvOutLibrefEventsMkup, mpMemberNameKey=lmvOutTabNameEventsMkup);
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	/*-=-=-=-Подготовка данных и их загрузка в CAS-=-=-=-=-*/
	/*1. словарь ПБО с иерархиями и атрибутами*/
	proc casutil;
	  droptable casdata="pbo_dictionary" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_pbo_location casout='ia_pbo_location' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_PBO_LOC_HIERARCHY casout='IA_PBO_LOC_HIERARCHY' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_PBO_LOC_ATTRIBUTES casout='IA_PBO_LOC_ATTRIBUTES' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_loc_attr{options replace=true} as
			select distinct *
			from casuser.ia_pbo_loc_attributes
			;
	quit;

	proc cas;
	transpose.transpose /
	   table={name="pbo_loc_attr", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_dictionary{options replace=true} as
	   select t2.pbo_location_id, 
		   coalesce(t2.lvl3_id,-999) as lvl3_id,
		   coalesce(t2.lvl2_id,-99) as lvl2_id,
		   cast(1 as double) as lvl1_id,
		   coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
		   coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
		   coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
		   cast(inputn(t3.A_OPEN_DATE,'ddmmyy10.') as date) as A_OPEN_DATE,
		   cast(inputn(t3.A_CLOSE_DATE,'ddmmyy10.') as date) as A_CLOSE_DATE,
		   t3.A_PRICE_LEVEL,
		   t3.A_DELIVERY
	   from casuser.pbo_hier_flat t2
	   left join casuser.attr_transposed t3
	   on t2.pbo_location_id=t3.pbo_location_id
	   left join casuser.IA_PBO_LOCATION t14
	   on t2.pbo_location_id=t14.pbo_location_id
	   left join casuser.IA_PBO_LOCATION t13
	   on t2.lvl3_id=t13.pbo_location_id
	   left join casuser.IA_PBO_LOCATION t12
	   on t2.lvl2_id=t12.pbo_location_id
	   ;
	quit;

	proc casutil;
	  promote casdata="pbo_dictionary" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='pbo_loc_attr' incaslib='casuser' quiet;
	  droptable casdata='ia_pbo_location' incaslib='casuser' quiet;
	  droptable casdata='IA_PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
	  droptable casdata='IA_PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;

	/*2. словарь продуктов с иерархиями и атрибутами*/
	proc casutil;
	  droptable casdata="product_dictionary" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_product casout='ia_product' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_product_HIERARCHY casout='IA_product_HIERARCHY' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_product_ATTRIBUTES casout='IA_product_ATTRIBUTES' outcaslib='casuser' replace;
	run;

	proc cas;
	transpose.transpose /
	   table={name="ia_product_attributes", caslib="casuser", groupby={"product_id"}} 
	   attributes={{name="product_id"}} 
	   transpose={"PRODUCT_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PRODUCT_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.ia_product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from casuser.ia_product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.ia_product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
			;
	quit;

	proc fedsql sessref=casauto noprint;
	   create table casuser.product_dictionary{options replace=true} as
	   select t1.product_id, 
		   coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
		   coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
		   coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
		   cast(1 as double) as prod_lvl1_id,
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
	   left join casuser.IA_product t15
	   on t1.product_id=t15.product_id
	   left join casuser.IA_product t14
	   on t1.lvl4_id=t14.product_id
	   left join casuser.IA_product t13
	   on t1.lvl3_id=t13.product_id
	   left join casuser.IA_product t12
	   on t1.lvl2_id=t12.product_id
	   ;
	quit;

	proc casutil;
	  promote casdata="product_dictionary" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_product' incaslib='casuser' quiet;
	  droptable casdata='IA_product_HIERARCHY' incaslib='casuser' quiet;
	  droptable casdata='IA_product_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='product_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;

	/*3. цены*/
	proc casutil;
	  droptable casdata="price" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_PRICE casout='ia_price' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_PRICE_HISTORY casout='IA_PRICE_HISTORY' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.ia_price{options replace=true} as
		select 
		PRODUCT_ID,PBO_LOCATION_ID,PRICE_TYPE,NET_PRICE_AMT,GROSS_PRICE_AMT,
		cast(datepart(cast(START_DT as timestamp)) as date) as START_DT,
		cast(datepart(cast(END_DT as timestamp)) as date) as END_DT
		from casuser.IA_PRICE
		;
	quit;
	proc fedsql sessref=casauto noprint;
		create table casuser.ia_price_history{options replace=true} as
		select 
		PRODUCT_ID,PBO_LOCATION_ID,PRICE_TYPE,NET_PRICE_AMT,GROSS_PRICE_AMT,
		cast(datepart(cast(START_DT as timestamp)) as date) as START_DT,
		cast(datepart(cast(END_DT as timestamp)) as date) as END_DT
		from casuser.IA_PRICE_history
		;
	quit;
	proc fedsql sessref=casauto noprint;
		create table casuser.price{options replace=true} as
		select 
		coalesce(t1.PRODUCT_ID,t2.PRODUCT_ID) as PRODUCT_ID,
		coalesce(t1.PBO_LOCATION_ID,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
		coalesce(t1.PRICE_TYPE,t2.PRICE_TYPE) as PRICE_TYPE,
		coalesce(t1.START_DT,t2.START_DT) as START_DT,
		coalesce(t1.END_DT,t2.END_DT) as END_DT,
		coalesce(t1.NET_PRICE_AMT,t2.NET_PRICE_AMT) as NET_PRICE_AMT,
		coalesce(t1.GROSS_PRICE_AMT,t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT
		from casuser.ia_price t1 full outer join casuser.ia_price_history t2
		on t1.PRODUCT_ID = t2.PRODUCT_ID and t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		and t1.PRICE_TYPE=t2.PRICE_TYPE and t1.START_DT=t2.START_DT
		;
	quit;

	proc casutil;
	  promote casdata="price" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_price' incaslib='casuser' quiet;
	  droptable casdata='IA_PRICE_HISTORY' incaslib='casuser' quiet;
	run;
	/*4. продажи*/
	proc casutil;
	  droptable casdata="pmix_sales" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_pmix_sales casout='ia_pmix_sales' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_pmix_sales_HISTORY casout='IA_pmix_sales_HISTORY' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint; /*7.18*/
		create table casuser.pmix_sales{options replace=true} as
		select 
		coalesce(t1.PBO_LOCATION_ID,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
		coalesce(t1.PRODUCT_ID,t2.PRODUCT_ID) as PRODUCT_ID,
		coalesce(t1.CHANNEL_CD,t2.CHANNEL_CD) as CHANNEL_CD,
		coalesce(t1.SALES_D,t2.SALES_D) as SALES_DT,
		coalesce(t1.SALES_QTY,t2.SALES_QTY) as SALES_QTY,
		coalesce(t1.SALES_QTY_PROMO,t2.SALES_QTY_PROMO) as SALES_QTY_PROMO,
		coalesce(t1.GROSS_SALES_AMT,t2.GROSS_SALES_AMT) as GROSS_SALES_AMT,
		coalesce(t1.NET_SALES_AMT,t2.NET_SALES_AMT) as NET_SALES_AMT
		from (select *, datepart(sales_dt) as sales_d from casuser.ia_pmix_sales ) t1 
		full outer join 
		(select *, datepart(sales_dt) as sales_d from casuser.ia_pmix_sales_history) t2
		on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.PRODUCT_ID=t2.PRODUCT_ID
		and t1.CHANNEL_CD=t2.CHANNEL_CD and 
		t1.SALES_D=t2.SALES_D
		;
	quit;

	proc casutil;
	  promote casdata="pmix_sales" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_pmix_sales' incaslib='casuser' quiet;
	  droptable casdata='IA_pmix_sales_HISTORY' incaslib='casuser' quiet;  
	run;

	/*5. GC*/
	proc casutil;
	  droptable casdata="pbo_sales" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_pbo_sales casout='ia_pbo_sales' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_pbo_sales_HISTORY casout='IA_pbo_sales_HISTORY' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint; 
	  create table casuser.pbo_sales{options replace=true} as
	  select 
	  coalesce(t1.PBO_LOCATION_ID,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID,
	  coalesce(t1.CHANNEL_CD,t2.CHANNEL_CD) as CHANNEL_CD,
	  coalesce(t1.SALES_D,t2.SALES_D) as SALES_DT,
	  coalesce(t1.RECEIPT_QTY,t2.RECEIPT_QTY) as RECEIPT_QTY,
	  coalesce(t1.GROSS_SALES_AMT,t2.GROSS_SALES_AMT) as GROSS_SALES_AMT,
	  coalesce(t1.NET_SALES_AMT,t2.NET_SALES_AMT) as NET_SALES_AMT
	  from (select *, datepart(sales_dt) as sales_d from casuser.ia_pbo_sales ) t1 
		 full outer join 
	  (select *, datepart(sales_dt) as sales_d from casuser.ia_pbo_sales_history) t2
	  on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID 
		and t1.CHANNEL_CD=t2.CHANNEL_CD and 
		t1.SALES_D=t2.SALES_D;
	quit;

	proc casutil;
	  promote casdata="pbo_sales" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_pbo_sales' incaslib='casuser' quiet;
	  droptable casdata='ia_pbo_sales_history' incaslib='casuser' quiet;  
	run;

	/*6. Ассорт матрица*/
	proc casutil;
	  droptable casdata="assort_matrix" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_assort_matrix casout='ia_assort_matrix' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.assort_matrix{options replace=true} as
		  select PBO_LOCATION_ID,PRODUCT_ID,
		  datepart(START_DT) as start_dt,
		  datepart(END_DT) as end_dt
		  from casuser.IA_ASSORT_MATRIX
		;
	quit;

	proc casutil;
	  promote casdata="assort_matrix" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_assort_matrix' incaslib='casuser' quiet;
	run;

	/*6.1. Таблица с жизненным циклом */
	proc casutil;
	  droptable casdata="product_chain" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_product_chain casout='ia_product_chain' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.product_chain{options replace=true} as
		  select 
		LIFECYCLE_CD,PREDECESSOR_DIM2_ID,PREDECESSOR_PRODUCT_ID,
		SCALE_FACTOR_PCT,SUCCESSOR_DIM2_ID,SUCCESSOR_PRODUCT_ID,
		  datepart(PREDECESSOR_END_DT) as PREDECESSOR_END_DT,
		  datepart(SUCCESSOR_START_DT) as SUCCESSOR_START_DT
		  from casuser.ia_product_chain
		;
	quit;

	proc casutil;
	  promote casdata="product_chain" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_product_chain' incaslib='casuser' quiet;
	run;
	
	/*6.2 Даты временного закрытия ПБО ETL_STG.IA_PBO_CLOSE_PERIOD */
	proc casutil;
	  droptable casdata="pbo_close_period" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_PBO_CLOSE_PERIOD casout='IA_PBO_CLOSE_PERIOD' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto;
	create table casuser.PBO_CLOSE_PERIOD{options replace=true} as
	  select 
	  CHANNEL_CD,CLOSE_PERIOD_DESC,PBO_LOCATION_ID,
	  datepart(END_DT) as END_DT,
	  datepart(START_DT) as START_DT
	  from casuser.IA_PBO_CLOSE_PERIOD
	;
	quit;

	proc casutil;
	  promote casdata="PBO_CLOSE_PERIOD" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='IA_PBO_CLOSE_PERIOD' incaslib='casuser' quiet;
	run;
	
	/*7. events */
	proc casutil;
	  droptable casdata="&lmvOutTabNameEvents." incaslib="&lmvOutLibrefEvents." quiet;
	  droptable casdata="&lmvOutTabNameEventsMkup" incaslib="&lmvOutLibrefEventsMkup." quiet;
	  load data=&lmvInLib..IA_events casout='ia_events' outcaslib='casuser' replace;
	run;

	proc sql noprint; /*начало и конец события - к ближайшим понедельникам*/
		create table events as select distinct
		upcase(prxchange('s/ |''/_/i',-1,strip(event_nm))) as _name_,
		intnx('week.2',datepart(start_dt),0) as _st_date_ format=date9.,
		intnx('week.2',datepart(end_dt),0) as _end_date_ format=date9.
		from &lmvInLib..IA_events
		order by 1,2
		;
	quit;

	data events1; /*Если ивент попадает на несколько недель - делаем несколько наблюдений*/
		set events;
		format _startdate_ date9.;
		if _end_date_=. then _end_date_=_st_date_;
		do _startdate_=_st_date_ to _end_date_ by 7;
		output;
		end;
	run;

	/*создание репозитория для VF*/
	proc hpfevents /*data=events*/;
	   id _startdate_ interval=week.2;
	   by _name_;
	   eventdata in=events1 out=events_prep;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNameEventsMkup. {options replace=true} as
		select datepart(start_dt) as start_dt, 
		cast(intnx('week.2',datepart(start_dt),0) as date) as week_dt,
		tranwrd(strip(event_nm),' ','_') as event_nm,
		pbo_location_id from casuser.ia_events
		;
	quit;

	proc casutil; 
	  load data=work.events_prep casout="&lmvOutTabNameEvents." outcaslib="&lmvOutLibrefEvents." promote;
	  promote casdata="&lmvOutTabNameEventsMkup." incaslib="casuser" outcaslib="&lmvOutLibrefEventsMkup.";
	  droptable casdata='ia_events' incaslib='casuser' quiet;
	run;

	/* 8. ia_media */
	proc casutil;
	  droptable casdata="media" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_media casout='ia_media' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.media {options replace=true} as 
		select datepart(cast(REPORT_DT as timestamp)) as period_dt,
		promo_group_id as promo_group_id,trp
		from casuser.ia_media
		;
	quit;

	proc casutil;
	  promote casdata="media" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_media' incaslib='casuser' quiet;
	run;
	/*9. ia_weather */
	proc casutil;
	  droptable casdata="weather" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_weather casout='ia_weather' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.weather {options replace=true} as 
		select PBO_LOCATION_ID,datepart(cast(REPORT_DT as timestamp)) as period_dt,
		PRECIPITATION,TEMPERATURE
		from casuser.ia_weather
		;
	quit;

	proc casutil;
	  promote casdata="weather" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_weather' incaslib='casuser' quiet;
	run;
	/*10.ia_macro */
	proc casutil;
	  droptable casdata="macro" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_macro_factor casout='ia_macro' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.macro {options replace=true} as 
		select factor_cd as NAME,datepart(cast(REPORT_DT as timestamp)) as period_dt,
		factor_chng_pct as FACTOR_PCT
		from casuser.ia_macro
		;
	quit;

	proc casutil;
	  promote casdata="macro" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_macro' incaslib='casuser' quiet;
	run;

	/*11. Promo markup*/
	proc casutil;
	  droptable casdata="promo" incaslib="casuser" quiet;
	  droptable casdata="promo_pbo" incaslib="casuser" quiet;
	  droptable casdata="promo_prod" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_promo casout='ia_promo' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_promo_x_pbo casout='ia_promo_x_pbo' outcaslib='casuser' replace;
	  load data=&lmvInLib..IA_promo_x_product casout='ia_promo_x_product' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo {options replace=true} as 
		select CHANNEL_CD,PROMO_ID,PROMO_GROUP_ID,PROMO_MECHANICS,
		PROMO_NM,SEGMENT,
		PROMO_PRICE_AMT,
		NP_GIFT_PRICE_AMT,
		datepart(cast(START_DT as timestamp)) as start_dt,
		datepart(cast(end_DT as timestamp)) as end_dt
		from casuser.ia_promo
		where start_dt is not null and end_dt is not null
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo {options replace=true} as 
		select PBO_LOCATION_ID,PROMO_ID
		from casuser.IA_PROMO_X_PBO
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod {options replace=true} as 
		select GIFT_FLG,OPTION_NUMBER,PRODUCT_ID,PRODUCT_QTY,PROMO_ID
		from casuser.IA_PROMO_X_PRODUCT
		;
	quit;

	proc casutil;
	  promote casdata="promo" incaslib="casuser" outcaslib="casuser";
	  promote casdata="promo_pbo" incaslib="casuser" outcaslib="casuser";
	  promote casdata="promo_prod" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_promo' incaslib='casuser' quiet;
	  droptable casdata='ia_promo_x_pbo' incaslib='casuser' quiet;
	  droptable casdata='ia_promo_x_product' incaslib='casuser' quiet;
	run;

	/*12. competitors*/
	proc casutil;
	  droptable casdata="comp_media" incaslib="casuser" quiet;
	  load data=&lmvInLib..IA_comp_media casout='ia_comp_media' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.comp_media {options replace=true} as 
		select COMPETITOR_CD,TRP,
		datepart(cast(report_dt as timestamp)) as report_dt
		from casuser.IA_COMP_MEDIA
		;
	quit;

	proc casutil;
	  promote casdata="comp_media" incaslib="casuser" outcaslib="casuser";
	  droptable casdata='ia_comp_media' incaslib='casuser' quiet;
	run;

%mend vf_load_data;