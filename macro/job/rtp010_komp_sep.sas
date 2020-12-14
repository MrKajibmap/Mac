/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения таргет таблиц с прогнозами
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
*     %rtp010_komp_sep;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp010_komp_sep;

	%let etls_jobName=rtp010_komp_sep;
	%etl_job_start;
	
	%rtp_komp_sep(mpVfPmixProjName=pmix_sales_v1,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=dm_abt.pmix_reconciled_full,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=casuser.plan_pmix_month,
							mpOutGcLt=casuser.plan_gc_month, 
							mpOutUptLt=casuser.plan_upt_month, 
							mpOutPmixSt=casuser.plan_pmix_day,
							mpOutGcSt=casuser.plan_gc_day, 
							mpOutUptSt=casuser.plan_upt_day, 
							mpOutOutforgc=casuser.TS_OUTFORGC,
							mpOutOutfor=casuser.TS_OUTFOR, 
							mpOutNnetWp=public.nnet_wp1,
							mpPrmt=N) ;

	%etl_job_finish;
	
%mend rtp010_komp_sep;