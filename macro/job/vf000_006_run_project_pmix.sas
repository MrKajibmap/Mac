/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для запуска VF-проекта на основе pmix_sal_abt
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
*     %vf000_006_run_project_pmix;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_006_run_project_pmix;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	%let etls_jobName=vf000_006_run_project_pmix;
	%etl_job_start;
	%m_etl_update_resource_status(P, vf_prepare_ts_abt_pmix);
	
	/* Запуск VF-проекта на основе pmix_sal_abt*/
	%vf_run_project(mpProjectId=&VF_PMIX_PROJ_NM.);  
	
	%m_etl_update_resource_status(L, vf_prepare_ts_abt_pmix);
	%m_etl_open_resource(vf_run_project_pmix);
	%etl_job_finish;
	
%mend vf000_006_run_project_pmix;