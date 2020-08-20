/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения ID vf-проекта из списка доступных VF-проектов, полученных макросом %vf_get_project_list
*		*необходимо использовать в формате %let vf_pr_name = %vf_get_project_id_by_name(mpName=pbo_sales_v2, mpProjList=__vf_project_list);
*	
*
*  ПАРАМЕТРЫ:
*	  mpProjList - Входная таблица со списком доступных vf-проектов (генерируется макросом %vf_get_project_list)
*     mpName	- Наименование vf-проекта (pbo_sales_v2)
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
*	  %let vf_proj_name = %vf_get_project_id_by_name(mpName=pbo_sales_v2, mpProjList=work.vf_project_list);
*	  %vf_get_project_id_by_name(mpName=pbo_sales_v2); 
*
****************************************************************************
*  11-08-2020  Борзунов     Начальное кодирование
*  11-08-2020  Борзунов     Добавлено сравнение по upcase
****************************************************************************/
%macro vf_get_project_id_by_name(mpName=, mpProjList=__vf_project_list);
	%local lmvDsid lmvRc lmvProfName;
	%let lmvProfName = %sysfunc(upcase(&mpName.));
	%let lmvDsid = %sysfunc(open(&mpProjList(where=(upcase(name)="&lmvProfName")), I));
    %let lmvRc=%sysfunc(fetch(&lmvDsid));  
    %do;%sysfunc(getvarc(&lmvDsid, %sysfunc(varnum(&lmvDsid, id))))%end;
    %let lmvRc=%sysfunc(close(&lmvDsid)); 
%mend vf_get_project_id_by_name;