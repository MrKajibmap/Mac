/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для создания витрины со сравнением двух промо-расчетов
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
*     %promo_calc_comp_va_dm(mpPromoCalculationRk1=1,mpPromoCalculationRk2=2);
*
****************************************************************************
*  30-06-2020  Борзунов     Начальное кодирование
*  06-07-2020  Михайлова    Перенос таблиц в CAS
*  21-07-2020  Борзунов		Изменение целевой библиотеки на DM_REP (для VA_DATAMART_CMP_)
							изменение библиотеки входных наборов на DM_REP (для VA_DATAMART_)
****************************************************************************/

%macro promo_calc_comp_va_dm_join(mpPromoCalculationRk1=,mpPromoCalculationRk2=);

	%local lmvPromoCalculationRk1 lmvPromoCalculationRk2 lmvCASSESS;
	%let lmvPromoCalculationRk1 = &mpPromoCalculationRk1.;
	%let lmvPromoCalculationRk2 = &mpPromoCalculationRk2.;
	%let lmvCASSESS = casauto;
	
	cas &lmvCASSESS.;
	caslib _all_ assign;
	
	data CASUSER.PRODUCT (replace=yes drop=PRODUCT_NM_OLD);
		length PRODUCT_NM $100;
		format PRODUCT_NM $100.;
		set ETL_IA.PRODUCT(rename=(PRODUCT_NM=PRODUCT_NM_OLD) where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
		PRODUCT_NM = substr(PRODUCT_NM_OLD,1,100);
	run;
	
	data CASUSER.PBO_LOCATION (replace=yes drop=pbo_location_nm_old);
		length pbo_location_nm $100;
		format pbo_location_nm $100.;
		set ETL_IA.PBO_LOCATION(rename=(pbo_location_nm=pbo_location_nm_old) where=(valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.));
		pbo_location_nm =  substr(pbo_location_nm_old,1,100);
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

	proc format SESSREF=&lmvCASSESS. casfmtlib="FMTDICT" cntlin=CASUSER.product_format ;
	run;

	proc format SESSREF=&lmvCASSESS. casfmtlib="FMTDICT" cntlin=CASUSER.pbo_format ;
	run;
	/* выгрузка форматов в cas */
	cas casauto  savefmtlib fmtlibname=FMTDICT table="dict_fmts.sashdat" caslib=formats replace;
	/* promote либы с форматами */
	cas casauto promotefmtlib fmtlibname=FMTDICT replace;
	
	proc casutil;
		droptable casdata="VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2." incaslib="DM_REP" quiet;
	run;
	
	proc fedsql SESSREF=&lmvCASSESS. noprint;
		create table CASUSER.VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2.{options replace=true} as
			select	coalesce(t1.breakfast, t2.breakfast) as breakfast
					,coalesce(t1.building_type, t2.building_type) as building_type
					,coalesce(t1.business_date, t2.business_date) as business_date
					,coalesce(t1.channel_cd, t2.channel_cd) as channel_cd
					,coalesce(t1.company, t2.company) as company
					,coalesce(t1.cost, t2.cost) as cost
					,coalesce(t1.delivery, t2.delivery) as delivery
					,t1.forecast_gc as forecast_gc
					,t2.forecast_gc as forecast_gc_cmp
					,t1.forecast_units as forecast_units
					,t2.forecast_units as forecast_units_cmp
					,coalesce(t1.gc, t2.gc) as gc
					,coalesce(t1.gross_price, t2.gross_price) as gross_price
					,coalesce(t1.item_size, t2.item_size) as item_size
					,coalesce(t1.month, t2.month) as month
					,coalesce(t1.net_price, t2.net_price) as net_price
					,coalesce(t1.offer_type, t2.offer_type) as offer_type
					,coalesce(t1.open_date, t2.open_date) as open_date
					,coalesce(t1.parent_pbo_location_id_1, t2.parent_pbo_location_id_1) as parent_pbo_location_id_1
					,coalesce(t1.parent_pbo_location_id_2, t2.parent_pbo_location_id_2) as parent_pbo_location_id_2
					,coalesce(t1.parent_pbo_location_id_3, t2.parent_pbo_location_id_3) as parent_pbo_location_id_3
					,coalesce(t1.parent_product_id_1, t2.parent_product_id_1) as parent_product_id_1
					,coalesce(t1.parent_product_id_2, t2.parent_product_id_2) as parent_product_id_2
					,coalesce(t1.parent_product_id_3, t2.parent_product_id_3) as parent_product_id_3
					,coalesce(t1.parent_product_id_4, t2.parent_product_id_4) as parent_product_id_4
					,coalesce(t1.pbo_location_id, t2.pbo_location_id) as pbo_location_id
					,coalesce(t1.plan_gc, t2.plan_gc) as plan_gc
					,coalesce(t1.plan_units, t2.plan_units) as plan_units
					,coalesce(t1.price_level, t2.price_level) as price_level
					,coalesce(t1.product_id, t2.product_id) as product_id
					,coalesce(t1.product_subgroup_1, t2.product_subgroup_1) as product_subgroup_1
					,coalesce(t1.product_subgroup_2, t2.product_subgroup_2) as product_subgroup_2
					,coalesce(t1.units, t2.units) as units
					,t1.PROMO_NET_PRICE as PROMO_NET_PRICE
					,t2.PROMO_NET_PRICE as PROMO_NET_PRICE_CMP
				from DM_REP.VA_DATAMART_&lmvPromoCalculationRk1 t1
					full join DM_REP.VA_DATAMART_&lmvPromoCalculationRk2 t2
						on t1.product_id = t2.product_id
						and t1.pbo_location_id = t2.pbo_location_id
						and t1.channel_cd = t2.channel_cd
						and t1.business_date = t2.business_date
				where coalesce(t1.business_date, t2.business_date)>=cast(&ETL_CURRENT_DT as date)
		;
	quit;
	
	data CASUSER.VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2./ SESSREF=&lmvCASSESS.;
		set CASUSER.VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2.;
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
				COST = 'Cost'
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
				FORECAST_GC = 'Forecast GC 1'
				FORECAST_GC_CMP = 'Forecast GC 2'
				UNITS = 'Units'
				PLAN_UNITS = 'Plan Units'
				FORECAST_UNITS = 'Forecast Units 1'
				FORECAST_UNITS_CMP = 'Forecast Units 2'
				PROMO_NET_PRICE = 'Promo Net Price'
				PROMO_NET_PRICE_CMP = 'Promo Net Price 2'
				;
		end;
	run;
	
	proc casutil;
		promote casdata="VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2." incaslib="CASUSER" outcaslib="DM_REP";
	run;
	quit;
	
	cas &lmvCASSESS. terminate;
	
%mend promo_calc_comp_va_dm_join;
