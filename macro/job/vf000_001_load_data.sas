/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках 
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
*     %vf000_001_load_data;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_001_load_data;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 

	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	
	%let etls_jobName=vf000_001_load_data;
	%etl_job_start;
	%m_etl_update_resource_status(P, LONGTERM);
	/* 1. загрузка данных в CAS */
	%vf_load_data_sep(mpEvents=mn_long.events, mpEventsMkup=mn_long.events_mkup);

	%m_etl_update_resource_status(L, LONGTERM);
	%m_etl_open_resource(vf_load_data);
	%etl_job_finish;
	
%mend vf000_001_load_data;
%vf000_001_load_data;