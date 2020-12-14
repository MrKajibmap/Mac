/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для применения недельного профиля - переразбивка прогноза 
*	  pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц
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
*     %vf000_007_month_aggregation;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_007_month_aggregation;

	%let etls_jobName=vf000_007_month_aggregation;
	%etl_job_start;
	%m_etl_update_resource_status(P, vf_train_week_profile);
	
	/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
	%vf_month_aggregation(mpInEventsMkup=dm_abt.events_mkup,
							mpOutPmix=dm_abt.plan_pmix_month,
							mpOutGc=dm_abt.plan_gc_month, 
							mpOutOutforgc=casuser.TS_OUTFORGC,
							mpOutOutfor=casuser.TS_OUTFOR, 
							mpOutNnetWp=casuser.nnet_wp1,
							mpInWpGc=casuser.wp_gc,
							mpPrmt=Y) ;
							
	%m_etl_update_resource_status(L, vf_train_week_profile);
	%m_etl_open_resource(vf_month_aggregation);
	
	%etl_job_finish;
	
%mend vf000_007_month_aggregation;