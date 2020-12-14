/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в рамках сквозного процесса для оперпрогноза (мастеркоды)
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
*     %rtp002_load_data_mcode;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp002_load_data_mcode;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 

	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	
	%let etls_jobName=rtp002_load_data_mcode;
	%etl_job_start;
	
	%m_etl_update_resource_status(P, load_data_product);
	
	%rtp_2_load_data_mastercode_sep( mpMode=A,
							mpInputTableScore=mn_short.all_ml_scoring, 
							mpInputTableTrain=mn_short.all_ml_train,
							mpOutputTableScore = mn_short.master_code_score,
							mpOutputTableTrain = mn_short.master_code_train
							);
	
	%m_etl_update_resource_status(L, load_data_product);
	%m_etl_open_resource(load_data_mcode);
	
	%etl_job_finish;
	
%mend rtp002_load_data_mcode;