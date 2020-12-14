/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для восстановления GC
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
*     %vf000_002_restore_sales_gc;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf000_002_restore_sales_gc;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
	cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;
	%let etls_jobName=vf000_002_restore_sales_gc;
	%etl_job_start;
	%m_etl_update_resource_status(P, vf_load_data);
	
	%vf_restore_sales_gc_sep;
	
	%m_etl_update_resource_status(L, vf_load_data);
	%m_etl_open_resource(vf_restore_sales_gc);
	%etl_job_finish;
	
%mend vf000_002_restore_sales_gc;