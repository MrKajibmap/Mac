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
*     %vf000_002_prepare_ts_abt_pbo;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_002_prepare_ts_abt_pbo;

	%let etls_jobName=vf000_002_prepare_ts_abt_pbo;
	%etl_job_start;
	
	/*2. Загрузка таблицы pbo_sal_abt */
	%vf_prepare_ts_abt_pbo(mpPboSalAbt=dm_abt.pbo_sal_abt,
							mpPromoW1=casuser.promo_w1,
							mpPromoD=casuser.promo_d, 
							mpPboSales=casuser.TS_pbo_sales,
							mpWeatherW=casuser.weather_w );
	
	%etl_job_finish;
	
%mend vf000_002_prepare_ts_abt_pbo;