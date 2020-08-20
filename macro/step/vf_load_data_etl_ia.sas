/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных из ETL_IA, используемых в сквозном процессе
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
*     %vf_load_data;
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf_load_data;
	%local lmvInLib;
	%let lmvInLib=ETL_IA;
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
		cas casauto;
		caslib _all_ assign;
	%end;
	
	/*-=-=-=-Подготовка данных и их загрузка в CAS-=-=-=-=-*/
	/*1. словарь ПБО с иерархиями и атрибутами*/
	proc casutil;
	  droptable casdata="pbo_dictionary" incaslib="public" quiet;
	run;
	
	data CASUSER.PBO_LOCATION (replace=yes);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	

	proc fedsql sessref=casauto noprint;
	   create table casuser.pbo_loc_attr{options replace=true} as
			select distinct *
			from casuser.PBO_LOC_ATTRIBUTES
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
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
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
	   left join casuser.pbo_location t14
	   on t2.pbo_location_id=t14.pbo_location_id
	   left join casuser.pbo_location t13
	   on t2.lvl3_id=t13.pbo_location_id
	   left join casuser.pbo_location t12
	   on t2.lvl2_id=t12.pbo_location_id
	   ;
	quit;

	proc casutil;
	  promote casdata="pbo_dictionary" incaslib="casuser" outcaslib="public";
	  droptable casdata='pbo_loc_attr' incaslib='casuser' quiet;
	  droptable casdata='pbo_location' incaslib='casuser' quiet;
	  droptable casdata='PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
	  droptable casdata='PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;

	/*2. словарь продуктов с иерархиями и атрибутами*/
	proc casutil;
	  droptable casdata="product_dictionary" incaslib="public" quiet;
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
	   table={name="product_ATTRIBUTES", caslib="casuser", groupby={"product_id"}} 
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
			(select * from casuser.product_HIERARCHY where product_lvl=5) as t1
			left join 
			(select * from casuser.product_HIERARCHY where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from casuser.product_HIERARCHY where product_lvl=3) as t3
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
	   left join casuser.product t15
	   on t1.product_id=t15.product_id
	   left join casuser.product t14
	   on t1.lvl4_id=t14.product_id
	   left join casuser.product t13
	   on t1.lvl3_id=t13.product_id
	   left join casuser.product t12
	   on t1.lvl2_id=t12.product_id
	   ;
	quit;

	proc casutil;
	  promote casdata="product_dictionary" incaslib="casuser" outcaslib="public";
	  droptable casdata='product' incaslib='casuser' quiet;
	  droptable casdata='product_HIERARCHY' incaslib='casuser' quiet;
	  droptable casdata='product_ATTRIBUTES' incaslib='casuser' quiet;
	  droptable casdata='product_hier_flat' incaslib='casuser' quiet;
	  droptable casdata='attr_transposed' incaslib='casuser' quiet;
	run;

	/*3. цены*/
	proc casutil;
	  droptable casdata="price" incaslib="public" quiet;
	run;
	
	data CASUSER.PRICE (replace=yes);
		set &lmvInLib..PRICE(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.PRICE{options replace=true} as
		select 
		PRODUCT_ID
		,PBO_LOCATION_ID
		,PRICE_TYPE
		,NET_PRICE_AMT
		,GROSS_PRICE_AMT
		,START_DT
		,END_DT
		/*cast(datepart(cast(START_DT as timestamp)) as date) as START_DT,
		cast(datepart(cast(END_DT as timestamp)) as date) as END_DT*/
		from casuser.PRICE
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.price{options replace=true} as
		select 
		t1.PRODUCT_ID
		,t1.PBO_LOCATION_ID
		,t1.PRICE_TYPE
		,t1.START_DT
		,t1.END_DT
		,t1.NET_PRICE_AMT
		,t1.GROSS_PRICE_AMT
		from casuser.PRICE t1
		;
	quit;

	proc casutil;
	  promote casdata="price" incaslib="casuser" outcaslib="public";
	  droptable casdata='PRICE' incaslib='casuser' quiet;
	run;
	/*4. продажи*/
	proc casutil;
	  droptable casdata="pmix_sales" incaslib="public" quiet;
	run;
	
	data CASUSER.pmix_sales (replace=yes);
		set &lmvInLib..pmix_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm. 
			and sales_dt>=%sysfunc(intnx(year,&lmvReportDt.,0)) and sales_dt<=%sysfunc(intnx(year,&lmvReportDt.,0,e))));
	run;

	proc fedsql sessref=casauto noprint; /*7.18*/
		create table casuser.pmix_sales{options replace=true} as
		select 
		t1.PBO_LOCATION_ID
		,t1.PRODUCT_ID
		,t1.CHANNEL_CD
		,t1.SALES_D
		,t1.SALES_QTY
		,t1.SALES_QTY_PROMO
		,t1.GROSS_SALES_AMT
		,t1.NET_SALES_AMT
		from casuser.pmix_sales t1 
		;
	quit;

	proc casutil;
	  promote casdata="pmix_sales" incaslib="casuser" outcaslib="public";
	  droptable casdata='pmix_sales' incaslib='casuser' quiet;
	run;

	/*5. GC*/
	proc casutil;
	  droptable casdata="pbo_sales" incaslib="public" quiet;
	run;

	data CASUSER.pbo_sales (replace=yes);
		set &lmvInLib..pbo_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
		and sales_dt>=%sysfunc(intnx(year,&lmvReportDt.,0)) and sales_dt<=%sysfunc(intnx(year,&lmvReportDt.,0,e))));
	run;
	
	proc fedsql sessref=casauto noprint; 
	  create table casuser.pbo_sales{options replace=true} as
	  select 
	  t1.PBO_LOCATION_ID
	  ,t1.CHANNEL_CD
	  ,t1.SALES_D
	  ,t1.RECEIPT_QTY
	  ,t1.GROSS_SALES_AMT
	  ,t1.NET_SALES_AMT
	  from casuser.pbo_sales t1 
	quit;

	proc casutil;
	  promote casdata="pbo_sales" incaslib="casuser" outcaslib="public";
	  droptable casdata='pbo_sales' incaslib='casuser' quiet; 
	run;

	/*6. Ассорт матрица*/
	proc casutil;
	  droptable casdata="assort_matrix" incaslib="public" quiet;
	  load data=&lmvInLib..assort_matrix casout='assort_matrix' outcaslib='casuser' replace;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.assort_matrix{options replace=true} as
		  select PBO_LOCATION_ID
		  ,PRODUCT_ID
		  ,START_DT
		  ,END_DT
		  from casuser.assort_matrix
		;
	quit;

	proc casutil;
	  promote casdata="assort_matrix" incaslib="casuser" outcaslib="public";
	  droptable casdata='assort_matrix' incaslib='casuser' quiet;
	run;

	/*6.1. Таблица с жизненным циклом */
	proc casutil;
	  droptable casdata="product_chain" incaslib="public" quiet;
	run;

	data CASUSER.product_chain (replace=yes);
		set &lmvInLib..product_chain(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.product_chain{options replace=true} as
		  select 
			LIFECYCLE_CD
			,PREDECESSOR_DIM2_ID
			,PREDECESSOR_PRODUCT_ID
			,SCALE_FACTOR_PCT
			,SUCCESSOR_DIM2_ID
			,SUCCESSOR_PRODUCT_ID
			,PREDECESSOR_END_DT
			,SUCCESSOR_START_DT
		  from casuser.product_chain
		;
	quit;

	proc casutil;
	  promote casdata="product_chain" incaslib="casuser" outcaslib="public";
	  droptable casdata='product_chain' incaslib='casuser' quiet;
	run;
	
	/*6.2 Даты временного закрытия ПБО ETL_STG.PBO_CLOSE_PERIOD */
	proc casutil;
	  droptable casdata="pbo_close_period" incaslib="public" quiet;
	run;

	data CASUSER.PBO_CLOSE_PERIOD (replace=yes);
		set &lmvInLib..PBO_CLOSE_PERIOD(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto;
	create table casuser.PBO_CLOSE_PERIOD{options replace=true} as
	  select 
	  CHANNEL_CD
	  ,CLOSE_PERIOD_DESC
	  ,PBO_LOCATION_ID
	  ,END_DT
	  ,START_DT
	  from public.PBO_CLOSE_PERIOD
	;
	quit;

	proc casutil;
	  promote casdata="PBO_CLOSE_PERIOD" incaslib="casuser" outcaslib="public";
	  droptable casdata='PBO_CLOSE_PERIOD' incaslib='casuser' quiet;
	run;
	
	/*7. events */
	proc casutil;
	  droptable casdata="events" incaslib="public" quiet;
	  droptable casdata="events_mkup" incaslib="public" quiet;
	run;
	
	data CASUSER.events (replace=yes);
		set &lmvInLib..events(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc sql noprint; /*начало и конец события - к ближайшим понедельникам*/
		create table events as 
		select distinct
			upcase(prxchange('s/ |''/_/i',-1,strip(event_nm))) as _name_
			,intnx('week.2',start_dt,0) as _st_date_ format=date9.
			,intnx('week.2',end_dt,0) as _end_date_ format=date9.
		from &lmvInLib..events
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
		create table casuser.events_mkup {options replace=true} as
		select datepart(start_dt) as start_dt, 
		cast(intnx('week.2',datepart(start_dt),0) as date) as week_dt,
		tranwrd(strip(event_nm),' ','_') as event_nm,
		pbo_location_id from casuser.events
		;
	quit;

	proc casutil;
	  load data=work.events_prep casout="events" outcaslib="public" promote;
	  promote casdata="events_mkup" incaslib="casuser" outcaslib="public";
	  droptable casdata='events' incaslib='casuser' quiet;
	run;

	/* 8. media */
	proc casutil;
	  droptable casdata="media" incaslib="public" quiet;
	run;
	
	data CASUSER.media (replace=yes);
		set &lmvInLib..media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.media {options replace=true} as 
		select datepart(cast(REPORT_DT as timestamp)) as period_dt,
		promo_group_id as promo_group_id,trp
		from casuser.media
		;
	quit;

	proc casutil;
	  promote casdata="media" incaslib="casuser" outcaslib="public";
	  droptable casdata='media' incaslib='casuser' quiet;
	run;
	/*9. weather */
	proc casutil;
	  droptable casdata="weather" incaslib="public" quiet;
	run;

	data CASUSER.weather (replace=yes);
		set &lmvInLib..weather(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.weather {options replace=true} as 
		select PBO_LOCATION_ID
		,cast(REPORT_DT as timestamp) as period_dt
		,PRECIPITATION
		,TEMPERATURE
		from casuser.weather
		;
	quit;

	proc casutil;
	  promote casdata="weather" incaslib="casuser" outcaslib="public";
	  droptable casdata='weather' incaslib='casuser' quiet;
	run;
	/*10.ia_macro */
	proc casutil;
	  droptable casdata="macro" incaslib="public" quiet;
	run;
	
	data CASUSER.macro_factor (replace=yes);
		set &lmvInLib..macro_factor(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.macro {options replace=true} as 
		select factor_cd as NAME,datepart(cast(REPORT_DT as timestamp)) as period_dt,
		factor_chng_pct as FACTOR_PCT
		from casuser.ia_macro
		;
	quit;

	proc casutil;
	  promote casdata="macro" incaslib="casuser" outcaslib="public";
	  droptable casdata='ia_macro' incaslib='casuser' quiet;
	run;

	/*11. Promo markup*/
	proc casutil;
	  droptable casdata="promo" incaslib="public" quiet;
	  droptable casdata="promo_pbo" incaslib="public" quiet;
	  droptable casdata="promo_prod" incaslib="public" quiet;
	  load data=&lmvInLib..promo casout='promo' outcaslib='casuser' replace;
	  load data=&lmvInLib..promo_x_pbo casout='promo_x_pbo' outcaslib='casuser' replace;
	  load data=&lmvInLib..promo_x_product casout='promo_x_product' outcaslib='casuser' replace;
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

	proc fedsql sessref=casauto noprint;
		create table casuser.promo {options replace=true} as 
		select CHANNEL_CD
		,PROMO_ID
		,PROMO_GROUP_ID
		,PROMO_MECHANICS
		,PROMO_NM
		,SEGMENT
		,PROMO_PRICE_AMT
		,NP_GIFT_PRICE_AMT
		,start_dt
		,end_dt
		from casuser.promo
		where start_dt is not null and end_dt is not null
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo {options replace=true} as 
		select PBO_LOCATION_ID,PROMO_ID
		from casuser.promo_X_PBO
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod {options replace=true} as 
		select GIFT_FLG,OPTION_NUMBER,PRODUCT_ID,PRODUCT_QTY,PROMO_ID
		from casuser.promo_X_PRODUCT
		;
	quit;

	proc casutil;
	  promote casdata="promo" incaslib="casuser" outcaslib="public";
	  promote casdata="promo_pbo" incaslib="casuser" outcaslib="public";
	  promote casdata="promo_prod" incaslib="casuser" outcaslib="public";
	  droptable casdata='promo' incaslib='casuser' quiet;
	  droptable casdata='promo_x_pbo' incaslib='casuser' quiet;
	  droptable casdata='promo_x_product' incaslib='casuser' quiet;
	run;

	/*12. competitors*/
	proc casutil;
	  droptable casdata="comp_media" incaslib="public" quiet;
	run;

	data CASUSER.comp_media (replace=yes);
		set &lmvInLib..comp_media(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	proc fedsql sessref=casauto noprint;
		create table casuser.comp_media {options replace=true} as 
		select COMPETITOR_CD
			,TRP
			,report_dt
		from casuser.comp_media
		;
	quit; 

	proc casutil;
	  promote casdata="comp_media" incaslib="casuser" outcaslib="public";
	  droptable casdata='comp_media' incaslib='casuser' quiet;
	run;

%mend vf_load_data;