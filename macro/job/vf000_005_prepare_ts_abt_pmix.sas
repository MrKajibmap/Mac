/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки таблицы pmix_sal_abt в рамках 
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
*     %vf000_005_prepare_ts_abt_pmix;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_005_prepare_ts_abt_pmix;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	%let etls_jobName=vf000_005_prepare_ts_abt_pmix;
	%etl_job_start;
	%m_etl_update_resource_status(P, vf_run_project_pbo);
	
	/* 4. Загрузка таблицы pmix_sal_abt*/
	%vf_prepare_ts_abt_pmix_sep(mpVfPboProjName=pbo_sales_v1,
							mpPmixSalAbt=mn_long.pmix_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d,
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w);
	
	%m_etl_update_resource_status(L, vf_run_project_pbo);
	%m_etl_open_resource(vf_prepare_ts_abt_pmix);
	%etl_job_finish;
	
%mend vf000_005_prepare_ts_abt_pmix;