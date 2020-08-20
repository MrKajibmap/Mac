/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для создания модели недельного профиля для разбивки
*	  по дням и переагрегации недель до месяцев
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
*     %vf100_001_train_week_profile;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf100_001_train_week_profile;

	%let etls_jobName=vf100_001_train_week_profile;
	%etl_job_start;
	
	/* Создание модели недельного профиля для разбивки по дням и переагрегации недель до месяцев*/
	%vf_train_week_profile; 
	
	%etl_job_finish;
	
%mend vf100_001_train_week_profile;