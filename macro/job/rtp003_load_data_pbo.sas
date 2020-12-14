/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки данных в рамках сквозного процесса для оперпрогноза (pbo)
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
*     %rtp003_load_data_pbo;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp003_load_data_pbo;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 

	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	
	%let etls_jobName=rtp003_load_data_pbo;
	%etl_job_start;
	%m_etl_update_resource_status(P, load_data_mcode);
	/* 1. загрузка данных в CAS */
	%rtp_3_load_data_pbo(mpMode=A, 
							mpOutTableTrain=dm_abt.pbo_train,
							mpOutTableScore=dm_abt.pbo_score); 
	%m_etl_update_resource_status(L, load_data_mcode);
	%m_etl_open_resource(load_data_pbo);
	%etl_job_finish;
	
%mend rtp003_load_data_pbo;