	/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для создания витрины отчета VA, обогащенной данными прогноза,
*		и ее загрузки в DM_ABT (PG LIB)
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
*     %pc_load_pg_va_dm(mpPromoCalculationRk=8);
*
****************************************************************************
*  18-06-2020  Борзунов     Начальное кодирование
*  02-07-2020  Михайлова    bug fix, добавление параметров mpFcstTable, mpOut
****************************************************************************/

%macro pc_load_pg_va_dm(mpFcstTable=DM_ABT.PMIX_SCORED, mpPromoCalculationRk=, mpOut=DM_REP.VA_DATAMART); 
	/* init macrovars */
 	%local lmvInLibref lmvCASSESS lmvPromoCalculationRk lmvReportDt lmvReportDttm; 
	
	%let lmvInLibref = ETL_IA;	
	%let lmvPromoCalculationRk = &mpPromoCalculationRk.;
	%let lmvCASSESS = casauto;
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	
	cas &lmvCASSESS.;
	caslib _all_ assign;
	
	data CASUSER.PRODUCT (replace=yes drop=PRODUCT_NM_OLD);
		length PRODUCT_NM $100;
		format PRODUCT_NM $100.;
		set &lmvInLibref..PRODUCT(rename=(PRODUCT_NM=PRODUCT_NM_OLD) where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		PRODUCT_NM = substr(PRODUCT_NM_OLD,1,100);
	run;

	data CASUSER.PRODUCT_ATTRIBUTES (replace=yes drop=product_attr_value_old);
		length product_attr_value $50;
		format product_attr_value $50.;
		set &lmvInLibref..PRODUCT_ATTRIBUTES(rename=(product_attr_value=product_attr_value_old) where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		product_attr_value = substr(product_attr_value_old,1,50);
	run;

	data CASUSER.PRODUCT_HIERARCHY (replace=yes);
		set &lmvInLibref..PRODUCT_HIERARCHY (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PBO_LOCATION (replace=yes drop=pbo_location_nm_old);
		length pbo_location_nm $100;
		format pbo_location_nm $100.;
		set &lmvInLibref..PBO_LOCATION(rename=(pbo_location_nm=pbo_location_nm_old) where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		pbo_location_nm =  substr(pbo_location_nm_old,1,100);
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=pbo_loc_attr_value_old);
		length pbo_loc_attr_value $50;
		format pbo_loc_attr_value $50.;
		set &lmvInLibref..PBO_LOC_ATTRIBUTES(rename=(pbo_loc_attr_value=pbo_loc_attr_value_old) where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		pbo_loc_attr_value = substr(pbo_loc_attr_value_old,1,50);
	run;

	data CASUSER.PBO_LOC_HIERARCHY (replace=yes);
		set &lmvInLibref..PBO_LOC_HIERARCHY (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PRICE (replace=yes) ;
		set &lmvInLibref..PRICE(where=(valid_from_dttm<=&lmvReportDttm and valid_to_dttm>=&lmvReportDttm 
		and end_dt>=%sysfunc(intnx(year,&lmvReportDt.,0)) and start_dt<=%sysfunc(intnx(year,&lmvReportDt.,0,e))));
	run;

	data CASUSER.COST_PRICE (replace=yes);
		set &lmvInLibref..COST_PRICE(where=(valid_from_dttm<=&lmvReportDttm and valid_to_dttm>=&lmvReportDttm
		and end_dt>=%sysfunc(intnx(year,&lmvReportDt.,0)) and start_dt<=%sysfunc(intnx(year,&lmvReportDt.,0,e))));
	run;

	/* Разворачиваем иерархии в справочниках */
	proc fedsql SESSREF=&lmvCASSESS. noprint;
		create table CASUSER.PRODUCT_ALL_LVL {options replace=true} as
		select 
			ph1.product_id as PARENT_PRODUCT_ID_1
			,p1.product_nm as PARENT_PRODUCT_NM_1
			,ph2.product_id as PARENT_PRODUCT_ID_2
			,p2.product_nm as PARENT_PRODUCT_NM_2 
			,ph3.product_id as PARENT_PRODUCT_ID_3
			,p3.product_nm as PARENT_PRODUCT_NM_3 
			,ph4.product_id as PARENT_PRODUCT_ID_4
			,p4.product_nm as PARENT_PRODUCT_NM_4 
			,ph5.product_id as PRODUCT_ID
			,p5.product_nm as PRODUCT_NM
			,pa1.product_attr_value as OFFER_TYPE_OLD 
			,pa2.product_attr_value as ITEM_SIZE_OLD 
			,pa3.product_attr_value as PRODUCT_SUBGROUP_1_OLD
			,pa4.product_attr_value as PRODUCT_SUBGROUP_2_OLD 
		from CASUSER.PRODUCT_HIERARCHY ph1
		inner join CASUSER.PRODUCT_HIERARCHY ph2
			on ph2.product_lvl=2
			and ph2.parent_product_id=ph1.product_id
		inner join CASUSER.PRODUCT_HIERARCHY ph3
			on ph3.product_lvl=3
			and ph3.parent_product_id=ph2.product_id
		inner join CASUSER.PRODUCT_HIERARCHY ph4
			on ph4.product_lvl=4
			and ph4.parent_product_id=ph3.product_id
		inner join CASUSER.PRODUCT_HIERARCHY ph5
			on ph5.product_lvl=5
			and ph5.parent_product_id=ph4.product_id
		left join CASUSER.PRODUCT p1
			on p1.product_id=ph1.product_id
		left join CASUSER.PRODUCT p2
			on p2.product_id=ph2.product_id
		left join CASUSER.PRODUCT p3
			on p3.product_id=ph3.product_id
		left join CASUSER.PRODUCT p4
			on p4.product_id=ph4.product_id
		left join CASUSER.PRODUCT p5
			on p5.product_id=ph5.product_id
		left join CASUSER.PRODUCT_ATTRIBUTES pa1
			on pa1.product_id=ph5.product_id
			and pa1.product_attr_nm='OFFER_TYPE'
		left join CASUSER.PRODUCT_ATTRIBUTES pa2
			on pa2.product_id=ph5.product_id
			and pa2.product_attr_nm='ITEM_SIZE'
		left join CASUSER.PRODUCT_ATTRIBUTES pa3
			on pa3.product_id=ph5.product_id
			and pa3.product_attr_nm='PRODUCT_SUBGROUP_1'
		left join CASUSER.PRODUCT_ATTRIBUTES pa4
			on pa4.product_id=ph5.product_id
			and pa4.product_attr_nm='PRODUCT_SUBGROUP_2'
		where ph1.product_lvl=1
		;

		create table CASUSER.PBO_LOC_ALL_LVL {options replace=true} as
		select 
			ph1.pbo_location_id as PARENT_PBO_LOCATION_ID_1
			,p1.pbo_location_nm as PARENT_PBO_LOCATION_NM_1
			,ph2.pbo_location_id as PARENT_PBO_LOCATION_ID_2
			,p2.pbo_location_nm as PARENT_PBO_LOCATION_NM_2 
			,ph3.pbo_location_id as PARENT_PBO_LOCATION_ID_3
			,p3.pbo_location_nm as PARENT_PBO_LOCATION_NM_3 
			,ph4.pbo_location_id as PBO_LOCATION_ID
			,p4.pbo_location_nm as PBO_LOCATION_NM
			,pa1.pbo_loc_attr_value as BUILDING_TYPE_OLD 
			,pa2.pbo_loc_attr_value as COMPANY_OLD
			,pa3.pbo_loc_attr_value as PRICE_LEVEL_OLD
			,coalesce(pa4.pbo_loc_attr_value, ' ') as OPEN_DATE_char
			,pa5.pbo_loc_attr_value as BREAKFAST_OLD
			,pa6.pbo_loc_attr_value as WINDOW_TYPE_OLD 
			,pa7.pbo_loc_attr_value as DELIVERY_OLD
			,coalesce(pa8.pbo_loc_attr_value, ' ') as DELIVERY_OPEN_DATE_char
			,pa9.pbo_loc_attr_value as MCCAFE_TYPE_OLD
		from CASUSER.PBO_LOC_HIERARCHY ph1
		inner join CASUSER.PBO_LOC_HIERARCHY ph2
			on ph2.pbo_location_lvl=2
			and ph2.parent_pbo_location_id=ph1.pbo_location_id
		inner join CASUSER.PBO_LOC_HIERARCHY ph3
			on ph3.pbo_location_lvl=3
			and ph3.parent_pbo_location_id=ph2.pbo_location_id
		inner join CASUSER.PBO_LOC_HIERARCHY ph4
			on ph4.pbo_location_lvl=4
			and ph4.parent_pbo_location_id=ph3.pbo_location_id
		left join CASUSER.PBO_LOCATION p1
			on p1.pbo_location_id=ph1.pbo_location_id
		left join CASUSER.PBO_LOCATION p2
			on p2.pbo_location_id=ph2.pbo_location_id
		left join CASUSER.PBO_LOCATION p3
			on p3.pbo_location_id=ph3.pbo_location_id
		left join CASUSER.PBO_LOCATION p4
			on p4.pbo_location_id=ph4.pbo_location_id
		left join CASUSER.PBO_LOC_ATTRIBUTES pa1
			on pa1.pbo_location_id=ph4.pbo_location_id
			and pa1.pbo_loc_attr_nm='BUILDING_TYPE'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa2
			on pa2.pbo_location_id=ph4.pbo_location_id
			and pa2.pbo_loc_attr_nm='COMPANY'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa3
			on pa3.pbo_location_id=ph4.pbo_location_id
			and pa3.pbo_loc_attr_nm='PRICE_LEVEL'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa4
			on pa4.pbo_location_id=ph4.pbo_location_id
			and pa4.pbo_loc_attr_nm='OPEN_DATE'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa5
			on pa5.pbo_location_id=ph4.pbo_location_id
			and pa5.pbo_loc_attr_nm='BREAKFAST'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa6
			on pa6.pbo_location_id=ph4.pbo_location_id
			and pa6.pbo_loc_attr_nm='WINDOW_TYPE'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa7
			on pa7.pbo_location_id=ph4.pbo_location_id
			and pa7.pbo_loc_attr_nm='DELIVERY'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa8
			on pa8.pbo_location_id=ph4.pbo_location_id
			and pa8.pbo_loc_attr_nm='DELIVERY_OPEN_DATE'
		left join CASUSER.PBO_LOC_ATTRIBUTES pa9
			on pa9.pbo_location_id=ph4.pbo_location_id
			and pa9.pbo_loc_attr_nm='MCCAFE_TYPE'
		where ph1.pbo_location_lvl=1
		;
	quit;

	data CASUSER.PBO_LOC_ALL_LVL (replace=yes drop=OPEN_DATE_char
									 DELIVERY_OPEN_DATE_char
									 DELIVERY_OLD
									 WINDOW_TYPE_OLD
									 BREAKFAST_OLD
									 PRICE_LEVEL_OLD
									 COMPANY_OLD
									 BUILDING_TYPE_OLD
									 MCCAFE_TYPE_OLD
									 ) 
									 / SESSREF=&lmvCASSESS.;

		length OPEN_DATE DELIVERY_OPEN_DATE 8 
		DELIVERY $40
		WINDOW_TYPE $15
		BREAKFAST $3
		PRICE_LEVEL $30
		COMPANY $30
		BUILDING_TYPE $20
		MCCAFE_TYPE_OLD $3
		;

		format OPEN_DATE DELIVERY_OPEN_DATE date9. 
		DELIVERY $40. 
		WINDOW_TYPE $15.
		BREAKFAST $3.
		PRICE_LEVEL $30.
		COMPANY $30.
		BUILDING_TYPE $20.
		MCCAFE_TYPE_OLD $3.
		;

		set CASUSER.PBO_LOC_ALL_LVL;
		if OPEN_DATE_char='Undefined' then do;
			OPEN_DATE_char='00.00.0000';
		end;
		if DELIVERY_OPEN_DATE_char='Undefined' then do;
			DELIVERY_OPEN_DATE_char='00.00.0000';
		end;
		OPEN_DATE = input(OPEN_DATE_char,ddmmyy10.);
		DELIVERY_OPEN_DATE=input(DELIVERY_OPEN_DATE_char,ddmmyy10.);

		DELIVERY = substr(DELIVERY_OLD,1,40);
		WINDOW_TYPE = substr(WINDOW_TYPE_OLD,1,15);
		BREAKFAST = substr(BREAKFAST_OLD,1,3);
		PRICE_LEVEL = substr(PRICE_LEVEL_OLD,1,20);
		COMPANY = substr(COMPANY_OLD,1,15);
		BUILDING_TYPE = substr(BUILDING_TYPE_OLD,1,20);
		MCCAFE_TYPE = substr(MCCAFE_TYPE_OLD,1,3);
	run;

	data CASUSER.PRODUCT_ALL_LVL (replace=yes 
									drop=OFFER_TYPE_OLD
										ITEM_SIZE_OLD
										PRODUCT_SUBGROUP_1_OLD
										PRODUCT_SUBGROUP_2_OLD
									 ) 
									 / SESSREF=&lmvCASSESS.;

		length
			OFFER_TYPE $3 
			ITEM_SIZE $20
			PRODUCT_SUBGROUP_1 PRODUCT_SUBGROUP_2 $35
		;

		format 
			OFFER_TYPE $3. 
			ITEM_SIZE $20.
			PRODUCT_SUBGROUP_1 PRODUCT_SUBGROUP_2 $35. 
		;

		set CASUSER.PRODUCT_ALL_LVL;

		OFFER_TYPE = substr(OFFER_TYPE_OLD,1,3);
		ITEM_SIZE = substr(ITEM_SIZE_OLD,1,20);
		PRODUCT_SUBGROUP_1 = substr(PRODUCT_SUBGROUP_1_OLD,1,35);
		PRODUCT_SUBGROUP_2 = substr(PRODUCT_SUBGROUP_2_OLD,1,35);
	run;

	data CASUSER.product_format (replace=yes keep=START LABEL Fmtname Type) / SESSREF=&lmvCASSESS.;
		set CASUSER.PRODUCT;
		START = PRODUCT_ID;
		LABEL = PRODUCT_NM;
		Fmtname= 'product_name_fmt';
		Type = 'n';
	run;

	data CASUSER.pbo_format (replace=yes keep=START LABEL Fmtname Type) / SESSREF=&lmvCASSESS.;
		set CASUSER.PBO_LOCATION;
		START = PBO_LOCATION_ID;
		LABEL = PBO_LOCATION_NM;
		Fmtname= 'pbo_name_fmt';
		Type = 'n';
	run;

	proc format SESSREF=&lmvCASSESS. casfmtlib="FORMATS" cntlin=CASUSER.product_format ;
	run;

	proc format SESSREF=&lmvCASSESS. casfmtlib="FORMATS" cntlin=CASUSER.pbo_format ;
	run;
	/* выгрузка форматов в cas */
	cas casauto  savefmtlib fmtlibname=FORMATS table="dict_fmts.sashdat" caslib=formats replace;
	/* promote либы с форматами */
	cas casauto promotefmtlib fmtlibname=FORMATS replace;

	/* Подготовка данных о ценах*/
	data CASUSER.cost_price_full (replace=yes drop=start_dt end_dt) / SESSREF=&lmvCASSESS.;
		set CASUSER.COST_PRICE;
		do business_date=max(%SYSFUNC(intnx(year,&lmvReportDt.,0)),start_dt) to min(%sysfunc(intnx(year,&lmvReportDt.,0,e)),end_dt);
			output;
		end;
	run;

	data CASUSER.price_f (replace=yes drop=start_dt end_dt) / SESSREF=&lmvCASSESS.;
		set CASUSER.PRICE(where=(price_type='F'));
		do business_date=max(%SYSFUNC(intnx(year,&lmvReportDt.,0)),start_dt) to min(%sysfunc(intnx(year,&lmvReportDt.,0,e)),end_dt);
			output;
		end;
	run;
	
	data CASUSER.price_R (replace=yes drop=start_dt end_dt) / SESSREF=&lmvCASSESS.;
		set CASUSER.PRICE(where=(price_type='R'));
	do business_date=max(%SYSFUNC(intnx(year,&lmvReportDt.,0)),start_dt) to min(%sysfunc(intnx(year,&lmvReportDt.,0,e)),end_dt);
			output;
		end;
	run;
	
	data CASUSER.price_P (replace=yes drop=start_dt end_dt) / SESSREF=&lmvCASSESS.;
		set CASUSER.PRICE(where=(price_type='P'));
		do business_date=max(intnx('year',&lmvReportDt.,0),start_dt) to min(intnx('year',&lmvReportDt.,0,'e'),end_dt);
			output;
		end;
	run;
	/* exclude duplicates */
	data CASUSER.price_f (replace=yes) / SESSREF=&lmvCASSESS.;
		set CASUSER.price_f;
		by product_id pbo_location_id business_date;
		if first.product_id or first.pbo_location_id or first.business_date then output;
	run;
	
	data CASUSER.price_R (replace=yes) / SESSREF=&lmvCASSESS.;
		set CASUSER.price_R;
		by product_id pbo_location_id business_date;
		if first.product_id or first.pbo_location_id or first.business_date then output;
	run;
	
	data CASUSER.price_P (replace=yes) / SESSREF=&lmvCASSESS.;
		set CASUSER.price_P;
		by product_id pbo_location_id business_date;
		if first.product_id or first.pbo_location_id or first.business_date then output;
	run;

	proc casutil;
	  load data=DM_ABT.PLAN_GC_MONTH casout='PLAN_GC_MONTH' outcaslib='CASUSER' replace;
	  load data=DM_ABT.PLAN_UNITS_MONTH casout='PLAN_UNITS_MONTH' outcaslib='CASUSER' replace;
	run;
	
	data CASUSER.PLAN_GC_HIST (replace=yes) / SESSREF=&lmvCASSESS.;
	    set CASUSER.PLAN_GC_MONTH (where=(mon_dt>=%sysfunc(intnx(month,&lmvReportDt.,0)) and mon_dt<=%sysfunc(intnx(year,&lmvReportDt.,0,e))));
	    format business_date date9.;
	    mon_days_cnt=intnx('month',mon_dt,0,'e')-mon_dt;
	    do business_date=max(mon_dt,&lmvReportDt.) to intnx('month',mon_dt,0,'e');
	        plan_gc=divide(ff,mon_days_cnt);
	        output;
	    end;
	    drop mon_dt ff mon_days_cnt;
	run;
	
	data CASUSER.PLAN_UNIT_HIST (replace=yes) / SESSREF=&lmvCASSESS.;
	    set CASUSER.PLAN_UNITS_MONTH (where=(mon_dt>=%sysfunc(intnx(month,&lmvReportDt.,0)) and mon_dt<=%sysfunc(intnx(year,&lmvReportDt.,0,e))));
	    format business_date date9.;
	    mon_days_cnt=intnx('month',mon_dt,0,'e')-mon_dt;
	    do business_date=mon_dt to intnx('month',mon_dt,0,'e');
	        plan_units=divide(ff,mon_days_cnt);
	        if business_date>=&lmvReportDt. then output;
	    end;
	    drop mon_dt ff mon_days_cnt;
	run;

	data CASUSER.FCST (replace=yes keep=PBO_LOCATION_ID PRODUCT_ID CHANNEL_CD P_SUM_QTY SALES_DT); 
		set &mpFcstTable(where=(sales_dt>=&lmvReportDt. and promo_calculation_rk=&lmvPromoCalculationRk.));
	run;
	
	proc casutil;
		load data=DM_ABT.ENCODING_CHANNEL_CD casout='ENCODING_CHANNEL_CD' outcaslib='CASUSER' replace;
	run;
	
	/* 1.get fcst_data with additional periods */
	proc fedsql SESSREF=&lmvCASSESS. noprint;
		create table CASUSER.FCST_DATA {options replace=true} as
				select coalesce(fcst.PBO_LOCATION_ID, pu.PBO_LOCATION_ID) as PBO_LOCATION_ID,
						coalesce(fcst.PRODUCT_ID,pu.PRODUCT_ID) as PRODUCT_ID, 
						coalesce(cd.CHANNEL_CD, pu.CHANNEL_CD) as CHANNEL_CD,
						coalesce(fcst.SALES_DT, pu.business_date) as business_date,
						pu.plan_units,
						fcst.P_SUM_QTY
 			from CASUSER.FCST fcst
			left join CASUSER.ENCODING_CHANNEL_CD cd
				on cd.channel_cd_id = fcst.CHANNEL_CD
			full join CASUSER.PLAN_UNIT_HIST pu
				on fcst.PBO_LOCATION_ID= pu.PBO_LOCATION_ID
				and fcst.PRODUCT_ID=pu.PRODUCT_ID
				and cd.CHANNEL_CD= pu.CHANNEL_CD
				and fcst.SALES_DT = pu.business_date
		;
	quit;

	proc fedsql SESSREF=&lmvCASSESS. noprint;
		create table CASUSER.FCST_ADD_DATA {options replace=true} as
		select 
			fcst.PRODUCT_ID
			,fcst.PBO_LOCATION_ID
			,fcst.CHANNEL_CD as CHANNEL_CD_OLD
			,fcst.BUSINESS_DATE
			,cpr.FOOD_COST_AMT + cpr.PAPER_COST_AMT + cpr.NON_PRODUCT_COST_AMT as COST
			,coalesce(pprm.NET_PRICE_AMT,pfct.NET_PRICE_AMT,preg.NET_PRICE_AMT) as NET_PRICE
			,coalesce(pprm.GROSS_PRICE_AMT,pfct.GROSS_PRICE_AMT,preg.GROSS_PRICE_AMT) as GROSS_PRICE
			,. as GC
			,pgh.PLAN_GC
			,. as FORECAST_GC
			,. as UNITS
			,fcst.PLAN_UNITS
			,fcst.P_SUM_QTY as FORECAST_UNITS
		from CASUSER.FCST_DATA fcst
		left  join CASUSER.cost_price_full cpr
			on cpr.product_id=fcst.product_id
			and cpr.pbo_location_id=fcst.pbo_location_id
			and cpr.business_date=fcst.BUSINESS_DATE
		left join CASUSER.price_r  preg
			on preg.product_id=fcst.product_id
			and preg.pbo_location_id=fcst.pbo_location_id
			and preg.business_date=fcst.BUSINESS_DATE
		left join CASUSER.price_f  pfct
			on pfct.product_id=fcst.product_id
			and pfct.pbo_location_id=fcst.pbo_location_id
			and pfct.business_date=fcst.BUSINESS_DATE
		left join CASUSER.PRICE_p pprm
			on pprm.product_id=fcst.product_id
			and pprm.pbo_location_id=fcst.pbo_location_id
			and pprm.business_date=fcst.BUSINESS_DATE
		left join CASUSER.PLAN_GC_HIST pgh
			on pgh.channel_cd=fcst.channel_cd
			and pgh.pbo_location_id=fcst.pbo_location_id
			and pgh.business_date=fcst.BUSINESS_DATE
		;
	quit;

	data CASUSER.FCST_DATA (replace=yes drop=rc CHANNEL_CD_OLD) / SESSREF=&lmvCASSESS.;
		set CASUSER.FCST_ADD_DATA;
		format PARENT_PRODUCT_ID_1
				PARENT_PRODUCT_ID_2 
				PARENT_PRODUCT_ID_3
				PARENT_PRODUCT_ID_4 
				product_id product_name_fmt.
				PARENT_PBO_LOCATION_ID_1
				PARENT_PBO_LOCATION_ID_2
				PARENT_PBO_LOCATION_ID_3 
				pbo_location_id pbo_name_fmt.
				;
		length MONTH promo_calculation_rk 8
				CHANNEL_CD $3
				;
		label product_id = 'Product'
				PARENT_PRODUCT_ID_1 = 'Product 1'
				PARENT_PRODUCT_ID_2 = 'Product 2'
				PARENT_PRODUCT_ID_3 = 'Product 3'
				PARENT_PRODUCT_ID_4 = 'Product 4'
				OFFER_TYPE = 'Offer type'
				ITEM_SIZE = 'Item size'
				PRODUCT_SUBGROUP_1 = 'Product subgroup 1'
				PRODUCT_SUBGROUP_2 = 'Product subgroup 2'
				PBO_LOCATION_ID = 'PBO'
				PARENT_PBO_LOCATION_ID_1 = 'Location 1'
				PARENT_PBO_LOCATION_ID_2 = 'Location 2'
				PARENT_PBO_LOCATION_ID_3 = 'Location 3'
				BUILDING_TYPE = 'Building type'
				COMPANY = 'Company'
				PRICE_LEVEL = 'Price level'
				OPEN_DATE = 'Open date'
				BREAKFAST = 'Breakfast'
				DELIVERY = 'Delivery'
				CHANNEL_CD = 'Channel'
				BUSINESS_DATE = 'Date'
				MONTH = 'Month'
				NET_PRICE = 'Net Price'
				GROSS_PRICE = 'Gross Price'
				GC = 'GC'
				PLAN_GC = 'Plan GC'
				FORECAST_GC = 'Forecast GC'
				UNITS = 'Units'
				PLAN_UNITS = 'Plan Units'
				FORECAST_UNITS = 'Forecast Units'
				;

		if 0 then do;
			set CASUSER.PRODUCT_ALL_LVL(drop=PARENT_PRODUCT_NM_1 PARENT_PRODUCT_NM_2 PARENT_PRODUCT_NM_3 PARENT_PRODUCT_NM_4 PRODUCT_NM);
			set CASUSER.PBO_LOC_ALL_LVL(drop=PARENT_PBO_LOCATION_NM_1 PARENT_PBO_LOCATION_NM_2 PARENT_PBO_LOCATION_NM_3 PBO_LOCATION_NM);
		end;
		if _n_=1 then do;
			declare hash hprd(dataset: "CASUSER.PRODUCT_ALL_LVL");
			rc = hprd.defineKey('product_id');
			rc = hprd.defineData('PARENT_PRODUCT_ID_1','PARENT_PRODUCT_ID_2','PARENT_PRODUCT_ID_3','PARENT_PRODUCT_ID_4','OFFER_TYPE','ITEM_SIZE','PRODUCT_SUBGROUP_1','PRODUCT_SUBGROUP_2');
			rc = hprd.defineDone();
	
			declare hash hloc(dataset: "CASUSER.PBO_LOC_ALL_LVL");
			rc = hloc.defineKey('pbo_location_id');
			rc = hloc.defineData('PARENT_PBO_LOCATION_ID_1','PARENT_PBO_LOCATION_ID_2','PARENT_PBO_LOCATION_ID_3','BUILDING_TYPE','COMPANY','PRICE_LEVEL','OPEN_DATE','BREAKFAST','WINDOW_TYPE','DELIVERY','DELIVERY_OPEN_DATE','MCCAFE_TYPE');
			rc = hloc.defineDone();
		end;
		rc = hprd.find();
		rc = hloc.find();

		/*additional calculations*/
		CHANNEL_CD  = substr(CHANNEL_CD_OLD,1,3);
		MONTH = month(BUSINESS_DATE);
		promo_calculation_rk = &lmvPromoCalculationRk;
	run;

	%partitions_processing(mpPromoCalculationRk=&lmvPromoCalculationRk., mpTableName=&mpOut);
	
	proc append BASE=&mpOut._&lmvPromoCalculationRk.(bulkload=yes) DATA=CASUSER.FCST_DATA (DATALIMIT=ALL)  FORCE;
	run;
	quit;

	cas &lmvCASSESS terminate;

%mend pc_load_pg_va_dm;