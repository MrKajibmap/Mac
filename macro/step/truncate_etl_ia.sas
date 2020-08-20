/*****************************************************************
*  ВЕРСИЯ:
*     $Id:  $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Очистка таблиц ETL_IA
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
*     %truncate_etl_ia;
*
******************************************************************
*  16-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro truncate_etl_ia;

	proc sql;
		%postgres_connect (mpLoginSet=ETL_IA);
			execute      
				(
				truncate table etl_ia.ASSORT_MATRIX;
				truncate table etl_ia.ASSORT_MATRIX_DELTA;
				truncate table etl_ia.ASSORT_MATRIX_SNAP;
				truncate table etl_ia.ASSORT_MATRIX_SNUP;
				truncate table etl_ia.CHANNEL;
				truncate table etl_ia.CHANNEL_DELTA;
				truncate table etl_ia.CHANNEL_SNAP;
				truncate table etl_ia.CHANNEL_SNUP;
				truncate table etl_ia.COMPETITOR;
				truncate table etl_ia.COMPETITOR_DELTA;
				truncate table etl_ia.COMPETITOR_SNAP;
				truncate table etl_ia.COMPETITOR_SNUP;
				truncate table etl_ia.COMP_MEDIA;
				truncate table etl_ia.COMP_MEDIA_DELTA;
				truncate table etl_ia.COMP_MEDIA_SNAP;
				truncate table etl_ia.COMP_MEDIA_SNUP;
				truncate table etl_ia.COST_PRICE;
				truncate table etl_ia.COST_PRICE_DELTA;
				truncate table etl_ia.COST_PRICE_SNAP;
				truncate table etl_ia.COST_PRICE_SNUP;
				truncate table etl_ia.EVENTS;
				truncate table etl_ia.EVENTS_DELTA;
				truncate table etl_ia.EVENTS_SNAP;
				truncate table etl_ia.EVENTS_SNUP;
				truncate table etl_ia.IA_PROMO_X_PBO;
				truncate table etl_ia.IA_PROMO_X_PRODUCT;
				truncate table etl_ia.MACRO_FACTOR;
				truncate table etl_ia.MACRO_FACTOR_DELTA;
				truncate table etl_ia.MACRO_FACTOR_SNAP;
				truncate table etl_ia.MACRO_FACTOR_SNUP;
				truncate table etl_ia.MEDIA;
				truncate table etl_ia.MEDIA_DELTA;
				truncate table etl_ia.MEDIA_SNAP;
				truncate table etl_ia.MEDIA_SNUP;
				truncate table etl_ia.PBO_CLOSE_PERIOD;
				truncate table etl_ia.PBO_CLOSE_PERIOD_DELTA;
				truncate table etl_ia.PBO_CLOSE_PERIOD_SNAP;
				truncate table etl_ia.PBO_CLOSE_PERIOD_SNUP;
				truncate table etl_ia.PBO_LOCATION;
				truncate table etl_ia.PBO_LOCATION_DELTA;
				truncate table etl_ia.PBO_LOCATION_SNAP;
				truncate table etl_ia.PBO_LOCATION_SNUP;
				truncate table etl_ia.PBO_LOC_ATTRIBUTES;
				truncate table etl_ia.PBO_LOC_ATTRIBUTES_DELTA;
				truncate table etl_ia.PBO_LOC_ATTRIBUTES_SNAP;
				truncate table etl_ia.PBO_LOC_ATTRIBUTES_SNUP;
				truncate table etl_ia.PBO_LOC_HIERARCHY;
				truncate table etl_ia.PBO_LOC_HIERARCHY_DELTA;
				truncate table etl_ia.PBO_LOC_HIERARCHY_SNAP;
				truncate table etl_ia.PBO_LOC_HIERARCHY_SNUP;
				truncate table etl_ia.PBO_SALES;
				truncate table etl_ia.PBO_SALES_DELTA;
				truncate table etl_ia.PBO_SALES_SNAP;
				truncate table etl_ia.PBO_SALES_SNUP;
				truncate table etl_ia.PMIX_SALES;
				truncate table etl_ia.PMIX_SALES_DELTA;
				truncate table etl_ia.PMIX_SALES_SNAP;
				truncate table etl_ia.PMIX_SALES_SNUP;
				truncate table etl_ia.PRICE;
				truncate table etl_ia.PRICE_DELTA;
				truncate table etl_ia.PRICE_SNAP;
				truncate table etl_ia.PRICE_SNUP;
				truncate table etl_ia.PRODUCT;
				truncate table etl_ia.PRODUCT_ATTRIBUTES;
				truncate table etl_ia.PRODUCT_ATTRIBUTES_DELTA;
				truncate table etl_ia.PRODUCT_ATTRIBUTES_SNAP;
				truncate table etl_ia.PRODUCT_ATTRIBUTES_SNUP;
				truncate table etl_ia.PRODUCT_CHAIN;
				truncate table etl_ia.PRODUCT_CHAIN_DELTA;
				truncate table etl_ia.PRODUCT_CHAIN_SNAP;
				truncate table etl_ia.PRODUCT_CHAIN_SNUP;
				truncate table etl_ia.PRODUCT_DELTA;
				truncate table etl_ia.PRODUCT_HIERARCHY;
				truncate table etl_ia.PRODUCT_HIERARCHY_DELTA;
				truncate table etl_ia.PRODUCT_HIERARCHY_SNAP;
				truncate table etl_ia.PRODUCT_HIERARCHY_SNUP;
				truncate table etl_ia.PRODUCT_SNAP;
				truncate table etl_ia.PRODUCT_SNUP;
				truncate table etl_ia.PROMO;
				truncate table etl_ia.PROMO_DELTA;
				truncate table etl_ia.PROMO_SNAP;
				truncate table etl_ia.PROMO_SNUP;
				truncate table etl_ia.PROMO_X_PBO;
				truncate table etl_ia.PROMO_X_PBO_DELTA;
				truncate table etl_ia.PROMO_X_PBO_SNAP;
				truncate table etl_ia.PROMO_X_PBO_SNUP;
				truncate table etl_ia.PROMO_X_PRODUCT;
				truncate table etl_ia.PROMO_X_PRODUCT_DELTA;
				truncate table etl_ia.PROMO_X_PRODUCT_SNAP;
				truncate table etl_ia.PROMO_X_PRODUCT_SNUP;
				truncate table etl_ia.SEGMENT;
				truncate table etl_ia.SEGMENT_DELTA;
				truncate table etl_ia.SEGMENT_SNAP;
				truncate table etl_ia.SEGMENT_SNUP;
				truncate table etl_ia.TEST_IA;
				truncate table etl_ia.WEATHER;
				truncate table etl_ia.WEATHER_DELTA;
				truncate table etl_ia.WEATHER_SNAP;
				truncate table etl_ia.WEATHER_SNUP;  
				) 
				by postgres;  
		disconnect from postgres;
	quit;

%mend truncate_etl_ia;