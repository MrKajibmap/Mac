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
*     %vf000_004_prepare_ts_abt_pmix;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_004_prepare_ts_abt_pmix;

	%let etls_jobName=vf000_004_prepare_ts_abt_pmix;
	%etl_job_start;
	
	/* 4. Загрузка таблицы pmix_sal_abt*/
	%vf_prepare_ts_abt_pmix(mpPmixSalAbt=dm_abt.pmix_sal_abt,
							mpPromoW1=casuser.promo_w1,
							mpPromoD=casuser.promo_d,
							mpPboSales=casuser.TS_pbo_sales,
							mpWeatherW=casuser.weather_w);
	
	%etl_job_finish;
	
%mend vf000_004_prepare_ts_abt_pmix;