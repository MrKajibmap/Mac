/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки таблицы pbo_sal_abt в рамках
*	  сквозного процесса прогнозирования временными рядами
*	
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
*     %vf000_003_prepare_ts_abt_pbo;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_003_prepare_ts_abt_pbo;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	%let etls_jobName=vf000_003_prepare_ts_abt_pbo;
	%etl_job_start;
	%m_etl_update_resource_status(P, vf_restore_sales_gc);
	
	/*2. Загрузка таблицы pbo_sal_abt */
	%vf_prepare_ts_abt_pbo_sep(mpPboSalAbt=mn_long.pbo_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d, 
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w );
	
	%m_etl_update_resource_status(L, vf_restore_sales_gc);
	%m_etl_open_resource(vf_prepare_ts_abt_pbo);
	%etl_job_finish;
	
%mend vf000_003_prepare_ts_abt_pbo;