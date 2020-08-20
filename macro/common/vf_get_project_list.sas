/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения списка vf-проектов в указанную таблицу
*	
*
*  ПАРАМЕТРЫ:
*     mpOut	- Выходная таблица со списком доступных vf-проектов
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
*     %vf_get_project_list();
*	  %vf_get_project_list(mpOut=work.vf_project_list);
*
****************************************************************************
*  11-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf_get_project_list(mpOut=__vf_project_list);

	filename resp TEMP;
	
	proc http
	  method="GET"
	  url="&CUR_API_URL/analyticsGateway/projects?limit=99999"
		oauth_bearer = sas_services
	  out=resp;
	  headers
	    "Accept"="application/vnd.sas.collection+json";    
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;
	
	libname respjson JSON fileref=resp;
	data &mpOut;
	  set respjson.items;
	run;
	
%mend vf_get_project_list;	