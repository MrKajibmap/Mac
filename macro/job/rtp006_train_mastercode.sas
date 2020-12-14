/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для обучения моделей PMIX
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
*     %rtp006_train_mastercode;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp006_train_mastercode;

	%let etls_jobName=rtp006_train_mastercode;
	%etl_job_start;
	
	

	%rtp_4_modeling(mode=TRAIN,
				external=1,
				ids = prod_lvl4_id pbo_location_id sales_dt,
				target=sum_qty,
				categories=lvl2_id prod_lvl2_id, 
				external_modeltable=/data/files/input/MASTER_MODEL_TABLE.csv, 
				modeltable=MASTER_MODEL_TABLE,				
				traintable=dm_abt.master_code_train,
				scoretable=dm_abt.master_code_score,
				resulttable=dm_abt.pmix_days_result, 
				default_params=seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget,
				default_interval=GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX NUNIQUE_PRODUCT,
				default_nominal=OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY,
			model_prefix=MASTER_FOREST);	
			
	%etl_job_finish;
	
%mend rtp006_train_mastercode;