/* ********************************************************************* */
/* ********************************************************************* */
/* Джоб для запуска сквозного процесса прогнозирования временными рядами */
/* ********************************************************************* */
/* ********************************************************************* */
%macro vf_full_process;

	%M_ETL_REDIRECT_LOG(START, vf_process, Main);
	/* 1. загрузка данных в CAS */
	/* Значения параметров mpEvents mpEventsMkup выставлены по умолчанию,
	*  если необходимо, значения можно изменить, но нужно учитывать, что таблица 
	*  параметра mpEventsMkup используется в:
	*  		vf_train_week_profile_gc(параметр mpInEventsMkup=)
	*  		vf_month_aggregation(параметр mpInEventsMkup=) */
	/*
	%M_LOG_EVENT(START, vf_load_data);
	%vf_load_data(mpEvents=dm_abt.events,mpEventsMkup=dm_abt.events_mkup);
	%M_LOG_EVENT(END, vf_load_data);
	*/
	
	
	%M_ETL_REDIRECT_LOG(START, vf_load_data_sep, Main);
	%M_LOG_EVENT(START, vf_load_data_sep);
		%vf_load_data_sep(mpEvents=mn_long.events, mpEventsMkup=mn_long.events_mkup);
	%M_LOG_EVENT(END, vf_load_data_sep);
	%M_ETL_REDIRECT_LOG(END, vf_load_data_sep, Main);
	
	/*2.  */
	/* %M_LOG_EVENT(START, vf_restore_sales_gc);
	%vf_restore_sales_gc;
	%M_LOG_EVENT(END, vf_restore_sales_gc);
	*/
	
	%M_ETL_REDIRECT_LOG(START, vf_restore_sales_gc_sep, Main);
	%M_LOG_EVENT(START, vf_restore_sales_gc_sep);
		%vf_restore_sales_gc_sep;
	%M_LOG_EVENT(END, vf_restore_sales_gc_sep);
	%M_ETL_REDIRECT_LOG(END, vf_restore_sales_gc_sep, Main);
	
	
	/*3. Загрузка таблицы pbo_sal_abt */
	/*	Значения параметров выставлены по умолчанию - если необходимо, их можно изменить, учитывая, 
	*	что они используются в vf_prepare_ts_abt_pmix(соответствующие параметры) */
/*	%M_LOG_EVENT(START, vf_prepare_ts_abt_pbo);
	%vf_prepare_ts_abt_pbo(mpPboSalAbt=dm_abt.pbo_sal_abt,
								mpPromoW1=casuser.promo_w1,
								mpPromoD=casuser.promo_d, 
								mpPboSales=casuser.TS_pbo_sales,
								mpWeatherW=casuser.weather_w );
	%M_LOG_EVENT(END, vf_prepare_ts_abt_pbo);
*/

	%M_ETL_REDIRECT_LOG(START, vf_prepare_ts_abt_pbo_sep, Main);
	%M_LOG_EVENT(START, vf_prepare_ts_abt_pbo_sep);
		%vf_prepare_ts_abt_pbo_sep(mpPboSalAbt=mn_long.pbo_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d, 
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w);
	%M_LOG_EVENT(END, vf_prepare_ts_abt_pbo_sep);
	%M_ETL_REDIRECT_LOG(END, vf_prepare_ts_abt_pbo_sep, Main);
	
	/*4. Запуск VF-проекта на основе pbo_sal_abt*/
	/* Необходимо указать ИМЯ VF-проекта. Например, pbo_sales_v2*/
/*
	%M_LOG_EVENT(START, vf_run_project_pbo);
	%vf_run_project(mpProjectName=pbo_sales_v1);
	%M_LOG_EVENT(END, vf_run_project_pbo);
	*/
	%M_ETL_REDIRECT_LOG(START, vf_run_project_pbo, Main);
	%M_LOG_EVENT(START, vf_run_project_pbo);
		%vf_run_project(mpProjectName=&VF_PBO_PROJ_NM.); 
	%M_LOG_EVENT(END, vf_run_project_pbo);
	%M_ETL_REDIRECT_LOG(END, vf_run_project_pbo, Main);
	
	/* Загрузка таблицы pmix_sal_abt*/
	/* Необходимо указать ИМЯ VF-проекта в параметре mpProjectName, построенного на mpPboSalAbt=casuser.pbo_sal_abt */
/*	%M_LOG_EVENT(START, vf_prepare_ts_abt_pmix);
	%vf_prepare_ts_abt_pmix(mpVfPboProjName=pbo_sales_v1,
								mpPmixSalAbt=dm_abt.pmix_sal_abt,
								mpPromoW1=casuser.promo_w1,
								mpPromoD=casuser.promo_d,
								mpPboSales=casuser.TS_pbo_sales,
								mpWeatherW=casuser.weather_w);
	%M_LOG_EVENT(END, vf_prepare_ts_abt_pmix);
	*/
	%M_ETL_REDIRECT_LOG(START, vf_prepare_ts_abt_pmix_sep, Main);
	%M_LOG_EVENT(START, vf_prepare_ts_abt_pmix_sep);
		%vf_prepare_ts_abt_pmix_sep(mpVfPboProjName=&VF_PBO_PROJ_NM.,
							mpPmixSalAbt=mn_long.pmix_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d,
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w);
	%M_LOG_EVENT(END, vf_prepare_ts_abt_pmix_sep);
	%M_ETL_REDIRECT_LOG(END, vf_prepare_ts_abt_pmix_sep, Main);
	
	/*5. Запуск VF-проекта на основе pmix_sal_abt*/
	/* Необходимо указать ИМЯ VF-проекта в параметре mpProjectName. Например, pmix_sales_v1*/
	%M_ETL_REDIRECT_LOG(START, vf_run_project_pmix, Main);
	%M_LOG_EVENT(START, vf_run_project_pmix);
 		%vf_run_project(mpProjectName=&VF_PMIX_PROJ_NM.); 
 	%M_LOG_EVENT(END, vf_run_project_pmix);
	%M_ETL_REDIRECT_LOG(END, vf_run_project_pmix, Main);
	/*6. Создание модели недельного профиля для разбивки по дням и переагрегации недель до месяцев*/
	%M_ETL_REDIRECT_LOG(START, vf_train_week_profile_sep, Main);
	%M_LOG_EVENT(START, vf_train_week_profile_sep);
		%vf_train_week_profile_sep(mpOutWpGc=mn_long.wp_gc);
	%M_LOG_EVENT(END, vf_train_week_profile_sep);
	%M_ETL_REDIRECT_LOG(END, vf_train_week_profile_sep, Main);
	
	/*7. Создание модели недельного профиля для разбивки GC по дням и переагрегации недель до месяцев*/
	/*%vf_train_week_profile_gc(mpInEventsMkup=dm_abt.events_mkup,
									 mpNnetWp=casuser.nnet_wp1,
									 mpPromo_W=casuser.promo_w 
									 ); */
	/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
	/* Параметры mpPrmt=Y/N (Будут ли указанные таблицы запромоучены) */
	/* Параметр mpInWpGc = таблица, формируемая в vf_train_week_profile(параметр mpOutWpGc); */
	/*8. Необходимо указать ИМЕНА VF-проектов в параметрах mpVfPmixProjName, mpVfPboProjName*/
/*
	%M_LOG_EVENT(START, vf_month_aggregation);
	%vf_month_aggregation(mpVfPmixProjName=pmix_sales_v2,
								mpVfPboProjName=pbo_sales_v1,
								mpInEventsMkup=dm_abt.events_mkup,
								mpOutPmix=dm_abt.plan_pmix_month,
								mpOutGc=dm_abt.plan_gc_month, 
								mpOutOutforgc=casuser.TS_OUTFORGC,
								mpOutOutfor=casuser.TS_OUTFOR, 
								mpOutNnetWp=casuser.nnet_wp1,
								mpInWpGc=dm_abt.wp_gc,
								mpPrmt=Y);	
	%M_LOG_EVENT(END, vf_month_aggregation);			
*/
	%M_ETL_REDIRECT_LOG(START, vf_month_aggregation_sep, Main);
	%M_LOG_EVENT(START, vf_month_aggregation_sep);
		%vf_month_aggregation_sep(mpVfPmixProjName=&VF_PMIX_PROJ_NM.,
								mpVfPboProjName=&VF_PBO_PROJ_NM.,
								mpInEventsMkup=mn_long.events_mkup,
								mpOutPmix=mn_long.plan_pmix_month,
								mpOutGc=mn_long.plan_gc_month, 
								mpOutOutforgc=mn_long.TS_OUTFORGC,
								mpOutOutfor=mn_long.TS_OUTFOR, 
								mpOutNnetWp=public.nnet_wp1,
								mpInWpGc=mn_long.wp_gc,
								mpPrmt=Y) ;
	%M_LOG_EVENT(END, vf_month_aggregation_sep);
	%M_ETL_REDIRECT_LOG(END, vf_month_aggregation_sep, Main);
	
	/* 9. Выгрузка данных в CSV + в DP */
	*%vf_6_out_integration(mpVfPmixProjName=pmix_sales_v1,
								mpVfPboProjName=pbo_sales_v1,
								mpMLPmixTabName=DM_ABT.PLAN_PMIX_MONTH,
								mpInEventsMkup=dm_abt.events_mkup,
								mpInWpGc=dm_abt.wp_gc,
								mpOutPmixLt=casuser.plan_pmix_month,
								mpOutGcLt=casuser.plan_gc_month, 
								mpOutUptLt=casuser.plan_upt_month, 
								mpOutOutforgc=casuser.TS_OUTFORGC,
								mpOutOutfor=casuser.TS_OUTFOR, 
								mpOutNnetWp=public.nnet_wp1,
								mpPrmt=N) ;

	/*9. Экспорт в CSV */					
	*%dp_export_csv(mpInput=dm_abt.plan_pmix_month
							, mpTHREAD_CNT=30
							, mpPath=/data/dm_rep/);						
	*%dp_export_csv(mpInput=dm_abt.plan_gc_month
							, mpTHREAD_CNT=30
							, mpPath=/data/dm_rep/);	
							
	/*10. Запуск загрузки данных в DP */
	*%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM);
	*%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM);

	%M_ETL_REDIRECT_LOG(END, vf_process, Main);
%mend vf_full_process;