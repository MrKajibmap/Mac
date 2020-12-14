%include "/opt/sas/mcd_config/config/initialize_global.sas";

%let PromoCalculationRk1=&PromoCalculationRk1;
%let PromoCalculationRk2=&PromoCalculationRk2;

%promo_calc_comp_va_dm_join(mpPromoCalculationRk1=&PromoCalculationRk1,mpPromoCalculationRk2=&PromoCalculationRk2);

cas casauto;
caslib _all_ assign;

/*Создание таблицы с информацией по промо-календаре и отчетной дате для VA*/
proc sql;
	create table WORK.PROMO_CALENDAR1 as
	select 
			1 as promo_calendar_num
			,clndr.p_cal_id as promo_calendar_id
			,clndr.p_cal_nm as promo_calendar_nm
			,&ETL_CURRENT_DT. as report_dt format=date9.
		from pt.promo_calculation calc
		left join pt.promo_calendar clndr
			on clndr.p_cal_rk=calc.p_cal_rk
	where calc.promo_calculation_rk=&PromoCalculationRk1
	;
quit;

proc sql;
	create table WORK.PROMO_CALENDAR2 as
	select 
			2 as promo_calendar_num
			,clndr.p_cal_id as promo_calendar_id
			,clndr.p_cal_nm as promo_calendar_nm
			,&ETL_CURRENT_DT. as report_dt format=date9.
		from pt.promo_calculation calc
		left join pt.promo_calendar clndr
			on clndr.p_cal_rk=calc.p_cal_rk
	where calc.promo_calculation_rk=&PromoCalculationRk2
	;
quit;

data WORK.PROMO_CALENDAR_CMP;
	set WORK.PROMO_CALENDAR1 WORK.PROMO_CALENDAR2;
run;

proc casutil;
	droptable casdata="PROMO_CALENDAR_CMP" incaslib="CASUSER" quiet;
	load data=work.PROMO_CALENDAR_CMP casout='PROMO_CALENDAR_CMP' outcaslib='CASUSER' replace;
	promote casdata="PROMO_CALENDAR_CMP" incaslib="CASUSER" outcaslib="CASUSER";
run;

proc casutil;
	droptable casdata="VA_DATAMART_CMP" incaslib="CASUSER" quiet;
run;

proc cas;
  table.view name='VA_DATAMART_CMP' 
  tables = {
            {name="VA_DATAMART_CMP_&PromoCalculationRk1._&PromoCalculationRk2",
            caslib='DM_REP',
            vars = {{name='product_id', label='Product', format='product_name_fmt.'},
					{name='PARENT_PRODUCT_ID_1', label='Product 1', format='product_name_fmt.'},
					{name='PARENT_PRODUCT_ID_2', label='Product 2', format='product_name_fmt.'},
					{name='PARENT_PRODUCT_ID_3', label='Product 3', format='product_name_fmt.'},
					{name='PARENT_PRODUCT_ID_4', label='Product 4', format='product_name_fmt.'},
					{name='OFFER_TYPE', label='Offer type'},
					{name='ITEM_SIZE', label='Item size'},
					{name='PRODUCT_SUBGROUP_1', label='Product subgroup 1'},
					{name='PRODUCT_SUBGROUP_2', label='Product subgroup 2'},
					{name='PBO_LOCATION_ID', label='PBO', format='pbo_name_fmt.'},
					{name='PARENT_PBO_LOCATION_ID_1', label='Location 1', format='pbo_name_fmt.'},
					{name='PARENT_PBO_LOCATION_ID_2', label='Location 2', format='pbo_name_fmt.'},
					{name='PARENT_PBO_LOCATION_ID_3', label='Location 3', format='pbo_name_fmt.'},
					{name='BUILDING_TYPE', label='Building type'},
					{name='COMPANY', label='Company'},
					{name='PRICE_LEVEL', label='Price level'},
					{name='COST', label='Cost'},
					{name='OPEN_DATE', label='Open date'},
					{name='BREAKFAST', label='Breakfast'},
					{name='DELIVERY', label='Delivery'},
					{name='CHANNEL_CD', label='Channel'},
					{name='BUSINESS_DATE', label='Date'},
					{name='MONTH', label='Month'},
					{name='NET_PRICE', label='Net Price'},
					{name='GROSS_PRICE', label='Gross Price'},
					{name='GC', label='GC'},
					{name='PLAN_GC', label='Plan GC'},
					{name='FORECAST_GC', label='Forecast GC base'},
					{name='FORECAST_GC_CMP', label='Forecast GC compare'},
					{name='UNITS', label='Units'},
					{name='PLAN_UNITS', label='Plan Units'},
					{name='FORECAST_UNITS', label='Forecast Units base'},
					{name='FORECAST_UNITS_CMP', label='Forecast Units compare'}}
         }
};
quit;

proc casutil;
    promote casdata="VA_DATAMART_CMP" incaslib="CASUSER" outcaslib="CASUSER";
run;
quit;

cas casauto terminate;