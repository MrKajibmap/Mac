/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки данных в рамках сквозного процесса для оперпрогноза (продукты)
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
*     %rtp001_load_data_product;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp001_load_data_product;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 

	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	
	%let etls_jobName=rtp001_load_data_product;
	%etl_job_start;
	
	%m_etl_update_resource_status(P, SHORTTERM);
	/* 1. загрузка данных в CAS */
	%rtp_1_load_data_product_sep(mpMode=A,
					 mpOutTrain=mn_short.all_ml_train,
					 mpOutScore=mn_short.all_ml_scoring);
	
	%m_etl_update_resource_status(L, SHORTTERM);
	%m_etl_open_resource(load_data_product);

	%etl_job_finish;
%mend rtp001_load_data_product;