/*****************************************************************
*  ВЕРСИЯ:
*     $Id:  $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Очистка таблиц ETL_STG
*     
*
*  ПАРАМЕТРЫ:
*
******************************************************************
*  Использует:
*     %postgres_connect
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     %truncate_etl_stg;
*
******************************************************************
*  16-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro truncate_etl_stg;

	proc sql;
		%postgres_connect (mpLoginSet=ETL_STG);
			execute      
				(
				truncate table etl_stg.STG_ASSORT_MATRIX;
				truncate table etl_stg.STG_CHANNEL;
				truncate table etl_stg.STG_COMPETITOR;
				truncate table etl_stg.STG_COMP_MEDIA;
				truncate table etl_stg.STG_COST_PRICE;
				truncate table etl_stg.STG_EVENTS;
				truncate table etl_stg.STG_MACRO_FACTOR;
				truncate table etl_stg.STG_MEDIA;
				truncate table etl_stg.STG_PBO_CLOSE_PERIOD;
				truncate table etl_stg.STG_PBO_LOCATION;
				truncate table etl_stg.STG_PBO_LOC_ATTRIBUTES;
				truncate table etl_stg.STG_PBO_LOC_HIERARCHY;
				truncate table etl_stg.STG_PBO_SALES;
				truncate table etl_stg.STG_PBO_SALES_HISTORY;
				truncate table etl_stg.STG_PMIX_SALES;
				truncate table etl_stg.STG_PMIX_SALES_HISTORY;
				truncate table etl_stg.STG_PRICE;
				truncate table etl_stg.STG_PRICE_HISTORY;
				truncate table etl_stg.STG_PRODUCT;
				truncate table etl_stg.STG_PRODUCT_ATTRIBUTES;
				truncate table etl_stg.STG_PRODUCT_CHAIN;
				truncate table etl_stg.STG_PRODUCT_HIERARCHY;
				truncate table etl_stg.STG_PROMO;
				truncate table etl_stg.STG_PROMO_HISTORY;
				truncate table etl_stg.STG_PROMO_X_PBO;
				truncate table etl_stg.STG_PROMO_X_PBO_HISTORY;
				truncate table etl_stg.STG_PROMO_X_PRODUCT;
				truncate table etl_stg.STG_PROMO_X_PRODUCT_HISTORY;
				truncate table etl_stg.STG_RECEIPT;
				truncate table etl_stg.STG_RECEIPT_HISTORY;
				truncate table etl_stg.STG_SEGMENT;
				truncate table etl_stg.STG_WEATHER;
				) 
				by postgres;  
		disconnect from postgres;
	quit;

%mend truncate_etl_stg;