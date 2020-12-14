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
****************************************************************************/

%macro promo_calc_comp_va_dm(mpPromoCalculationRk1=,mpPromoCalculationRk2=);

	%local lmvPromoCalculationRk1 lmvPromoCalculationRk2 lmvCASSESS;
	%let lmvPromoCalculationRk1 = &mpPromoCalculationRk1.;
	%let lmvPromoCalculationRk2 = &mpPromoCalculationRk2.;
	%let lmvCASSESS = casauto;
	
	cas &lmvCASSESS.;
	caslib _all_ assign;
	
	proc casutil;
	  load data=dm_rep.va_datamart_&lmvPromoCalculationRk1 casout="DM_&lmvPromoCalculationRk1" outcaslib='CASUSER' replace;
	  load data=dm_rep.va_datamart_&lmvPromoCalculationRk2 casout="DM_&lmvPromoCalculationRk2" outcaslib='CASUSER' replace;
	run;
	
	proc fedsql SESSREF=&lmvCASSESS. noprint;
		create table CASUSER.DM_CMP{options replace=true} as
			select	coalesce(t1.breakfast, t2.breakfast) as breakfast
					,coalesce(t1.building_type, t2.building_type) as building_type
					,coalesce(t1.business_date, t2.business_date) as business_date
					,coalesce(t1.channel_cd, t2.channel_cd) as channel_cd
					,coalesce(t1.company, t2.company) as company 
					,coalesce(t1.cost, t2.cost) as cost 
					,coalesce(t1.delivery, t2.delivery) as delivery
					,coalesce(t1.delivery_open_date, t2.delivery_open_date) as delivery_open_date
					,t1.forecast_gc as forecast_gc
					,t2.forecast_gc as forecast_gc_cmp
					,t1.forecast_units as forecast_units
					,t2.forecast_units as forecast_units_cmp
					,coalesce(t1.gc, t2.gc) as gc 
					,coalesce(t1.gross_price, t2.gross_price) as gross_price 
					,coalesce(t1.item_size, t2.item_size) as item_size 
					,coalesce(t1.mccafe_type, t2.mccafe_type) as mccafe_type 
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
					,coalesce(t1.promo_calculation_rk, t2.promo_calculation_rk) as promo_calculation_rk
					,cast((&lmvPromoCalculationRk1.||'_'||&lmvPromoCalculationRk2) as character(8)) as promo_calculation_cmp_rk 		
					,coalesce(t1.units, t2.units) as units 
					,coalesce(t1.window_type, t2.window_type) as window_type
				from CASUSER.DM_&lmvPromoCalculationRk1 t1
					full join CASUSER.DM_&lmvPromoCalculationRk2 t2
						on t1.product_id = t2.product_id
						and t1.pbo_location_id = t2.pbo_location_id
						and t1.channel_cd = t2.channel_cd
						and t1.business_date = t2.business_date
		;
	quit;
	
	%partitions_processing(mpPromoCalculationRk=&lmvPromoCalculationRk1._&lmvPromoCalculationRk2, mpTableName=dm_rep.va_datamart_cmp);
	
	proc append BASE=dm_rep.va_datamart_cmp_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2 (bulkload=yes bl_default_dir="/data/tmp/" bl_psql_path="/usr/pgsql-11/bin/psql" BL_FORMAT=CSV BL_ESCAPE=ON BL_DELETE_DATAFILE=YES) 
				DATA=CASUSER.DM_CMP 
				(DATALIMIT=ALL) FORCE;
	run;
	
	proc casutil;
		droptable incaslib="CASUSER" casdata="DM_&lmvPromoCalculationRk1" quiet;
		droptable incaslib="CASUSER" casdata="DM_&lmvPromoCalculationRk2" quiet;
		droptable incaslib="CASUSER" casdata="DM_CMP" quiet;
	run;
	
%mend promo_calc_comp_va_dm;
