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
****************************************************************************/

%macro promo_calc_comp_va_dm(mpPromoCalculationRk1=,mpPromoCalculationRk2=);

	%local lmvPromoCalculationRk1 lmvPromoCalculationRk2 lmvCASSESS;
	%let lmvPromoCalculationRk1 = &mpPromoCalculationRk1.;
	%let lmvPromoCalculationRk2 = &mpPromoCalculationRk2.;
	%let lmvCASSESS = casauto;
	
	cas &lmvCASSESS.;
	caslib _all_ assign;
	
	proc fedsql SESSREF=&lmvCASSESS. noprint;
		create table CASUSER.VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2.{options replace=true} as
			select	1 as promo_calendar_num
					,breakfast
					,building_type
					,business_date
					,channel_cd
					,company 
					,cost 
					,delivery
					,delivery_open_date
					,forecast_gc
					,forecast_units
					,gc 
					,gross_price 
					,item_size 
					,mccafe_type 
					,month
					,net_price 
					,offer_type 
					,open_date 
					,parent_pbo_location_id_1 
					,parent_pbo_location_id_2 
					,parent_pbo_location_id_3 
					,parent_product_id_1 
					,parent_product_id_2 
					,parent_product_id_3 
					,parent_product_id_4 
					,pbo_location_id 
					,plan_gc 
					,plan_units
					,price_level 
					,product_id 
					,product_subgroup_1 
					,product_subgroup_2 		
					,units 
					,window_type
				from PUBLIC.VA_DATAMART_&lmvPromoCalculationRk1 t1
				where business_date>=cast(&ETL_CURRENT_DT as date)
				union
				select	2 as promo_calendar_num
					,breakfast
					,building_type
					,business_date
					,channel_cd
					,company 
					,cost 
					,delivery
					,delivery_open_date
					,forecast_gc
					,forecast_units
					,gc 
					,gross_price 
					,item_size 
					,mccafe_type 
					,month
					,net_price 
					,offer_type 
					,open_date 
					,parent_pbo_location_id_1 
					,parent_pbo_location_id_2 
					,parent_pbo_location_id_3 
					,parent_product_id_1 
					,parent_product_id_2 
					,parent_product_id_3 
					,parent_product_id_4 
					,pbo_location_id 
					,plan_gc 
					,plan_units
					,price_level 
					,product_id 
					,product_subgroup_1 
					,product_subgroup_2 		
					,units 
					,window_type 
				from PUBLIC.VA_DATAMART_&lmvPromoCalculationRk2
				where business_date>=cast(&ETL_CURRENT_DT as date)
		;
	quit;
	
	proc casutil;
		promote casdata="VA_DATAMART_CMP_&lmvPromoCalculationRk1._&lmvPromoCalculationRk2." incaslib="CASUSER" outcaslib="CASUSER";
	run;
	
	cas &lmvCASSESS. terminate;
	
%mend promo_calc_comp_va_dm;
