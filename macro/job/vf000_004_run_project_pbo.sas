/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для запуска VF-проекта на основе pbo_sal_abt
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
*     %vf000_004_run_project_pbo;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_004_run_project_pbo;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	%let etls_jobName=vf000_004_run_project_pbo;
	%etl_job_start;
	%m_etl_update_resource_status(P, vf_prepare_ts_abt_pbo);
	
	/*3. Запуск VF-проекта на основе pbo_sal_abt*/
	%vf_run_project(mpProjectName=&VF_PBO_PROJ_NM.);  
	
	%m_etl_update_resource_status(L, vf_prepare_ts_abt_pbo);
	%m_etl_open_resource(vf_run_project_pbo);
	%etl_job_finish;
	
%mend vf000_004_run_project_pbo;